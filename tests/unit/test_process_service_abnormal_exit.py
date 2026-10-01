"""爬虫进程「异常退出」检测与告警的测试。

背景（真实事故）：一次定时运行在第 10/30 个商品处因 Playwright 的 Node driver
崩溃（EPIPE）而猝死，但 `_watch_process_exit` 完全忽略 `process.returncode`，
于是**既不记失败也不发通知**——无人值守时，一个已经崩掉的任务会一直看起来
像正常运行。这里锁定「崩溃必须告警」，同时保证「用户主动停止不能误报」。
"""

from __future__ import annotations

import asyncio

import pytest

from src.services import process_service as module
from src.services.process_service import ProcessService, _describe_abnormal_exit


class FakeProcess:
    """与 test_process_service.py 保持一致的假进程。"""

    def __init__(self, pid: int):
        self.pid = pid
        self.returncode = None
        self._done = asyncio.Event()

    async def wait(self):
        await self._done.wait()
        return self.returncode

    def finish(self, returncode: int = 0):
        self.returncode = returncode
        self._done.set()

    def terminate(self):
        self.finish(-15)

    def kill(self):
        self.finish(-9)


@pytest.fixture()
def sent(monkeypatch):
    """捕获 send_ntfy_notification 的调用。"""
    calls = []

    async def fake_send(product_data, reason):
        calls.append((product_data, reason))

    monkeypatch.setattr(module, "send_ntfy_notification", fake_send)
    return calls


def _service_with_process(returncode, *, task_id=0, task_name="macbook pro监控", log_path=None):
    service = ProcessService()
    process = FakeProcess(pid=1234)
    process.finish(returncode)
    service.processes[task_id] = process
    service.task_names[task_id] = task_name
    service.log_paths[task_id] = log_path
    return service, process


@pytest.mark.parametrize(
    "returncode,expected",
    [
        (0, None),
        (None, None),
        (1, "退出码 1"),
        (-9, "被信号 9 终止"),
        (-15, "被信号 15 终止"),
    ],
)
def test_describe_abnormal_exit(returncode, expected):
    assert _describe_abnormal_exit(returncode) == expected


def test_abnormal_exit_is_reported(sent):
    """核心诉求：进程猝死必须告警，不能静默。"""
    service, process = _service_with_process(1)

    asyncio.run(service._watch_process_exit(process))

    assert len(sent) == 1, "异常退出必须发送通知"
    product_data, reason = sent[0]
    assert "任务异常退出" in product_data["商品标题"]
    assert "macbook pro监控" in product_data["商品标题"]
    assert "退出码 1" in reason
    assert "macbook pro监控" in reason


def test_normal_exit_is_silent(sent):
    """正常结束不该产生告警，否则通知会被淹没。"""
    service, process = _service_with_process(0)

    asyncio.run(service._watch_process_exit(process))

    assert sent == []


def test_intentional_stop_is_not_reported(sent):
    """用户主动停止（含后端关闭时的 stop_all）也会给出非零退出码，不能误报。"""
    service, process = _service_with_process(-15)
    service._intentional_stops.add(0)

    asyncio.run(service._watch_process_exit(process))

    assert sent == []
    assert 0 not in service._intentional_stops, "标记应被消费掉"


def test_intentional_stop_flag_is_consumed_once(sent):
    """标记只对那一次退出有效，不能永久压制后续告警。"""
    service, first = _service_with_process(-15, task_id=0)
    service._intentional_stops.add(0)
    asyncio.run(service._watch_process_exit(first))

    second = FakeProcess(pid=1235)
    second.finish(-9)
    service.processes[0] = second
    service.task_names[0] = "macbook pro监控"
    asyncio.run(service._watch_process_exit(second))

    assert len(sent) == 1, "第二次非预期退出仍应告警"
    assert "被信号 9 终止" in sent[0][1]


def test_notification_failure_does_not_break_exit_watching(monkeypatch):
    """通知渠道坏掉时不能抛异常，否则退出监听会留下未处理异常。"""
    service, process = _service_with_process(1)

    async def broken_send(product_data, reason):
        raise RuntimeError("通知服务不可用")

    monkeypatch.setattr(module, "send_ntfy_notification", broken_send)

    # 不应抛出
    asyncio.run(service._watch_process_exit(process))


def test_abnormal_exit_appends_marker_to_task_log(sent, tmp_path):
    """日志里留痕，便于在界面上区分“跑完”和“崩了”。"""
    log_path = tmp_path / "task-0.log"
    log_path.write_text("已有内容\n", encoding="utf-8")
    service, process = _service_with_process(-9, log_path=str(log_path))

    asyncio.run(service._watch_process_exit(process))

    content = log_path.read_text(encoding="utf-8")
    assert "已有内容" in content, "不应覆盖原有日志"
    assert "异常退出" in content
    assert "被信号 9 终止" in content


def test_stop_task_marks_intentional_before_terminating(sent, monkeypatch, tmp_path):
    """排序很关键：必须在终止进程之前打标记。

    否则退出监听可能在标记写入之前就完成判断，把主动停止误报成崩溃。
    """
    service, process = _service_with_process(0, log_path=str(tmp_path / "t.log"))
    process.returncode = None
    process._done = asyncio.Event()
    observed = {}

    async def fake_terminate(proc, task_id):
        # 此刻标记必须已经存在
        observed["marked_during_terminate"] = task_id in service._intentional_stops
        proc.finish(-15)

    monkeypatch.setattr(service, "_terminate_process", fake_terminate)

    async def run():
        service.exit_watchers[0] = asyncio.create_task(
            service._watch_process_exit(process)
        )
        return await service.stop_task(0)

    assert asyncio.run(run()) is True
    assert observed["marked_during_terminate"] is True
    assert sent == [], "主动停止不应告警"
