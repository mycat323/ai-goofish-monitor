"""启动任务失败时必须说明真实原因。

背景（真实事故）：定时任务被熔断暂停后，界面点“启动”只弹出一句
「启动任务失败」（HTTP 500），而服务端日志里真正的原因其实是
「连续失败 1/3，暂停到 2026-10-02 20:29:18，原因 FAIL_SYS_USER_VALIDATE」。
API 把可预期的业务状态（被暂停）当成了服务端错误，用户完全无从判断。

`start_task` 现在返回 StartTaskResult(started, reason)，接口层据此返回 409 +
可读原因，前端 http 封装会把 detail 直接显示到 toast。
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from src.api.routes import tasks as tasks_route
from src.services.process_service import ProcessService, StartTaskResult


class _FakeProcess:
    def __init__(self, pid: int = 999):
        self.pid = pid
        self.returncode = None

    async def wait(self):
        return self.returncode


def _decision(*, skip: bool, reason: str = "", paused_until=None, failures: int = 0):
    return SimpleNamespace(
        skip=skip,
        should_notify=False,
        reason=reason,
        paused_until=paused_until,
        consecutive_failures=failures,
    )


def test_already_running_reports_that_reason(monkeypatch):
    service = ProcessService()
    monkeypatch.setattr(service, "is_running", lambda task_id: True)

    result = asyncio.run(service.start_task(0, "task-a"))

    assert result.started is False
    assert result.reason == "任务已在运行中"


def test_paused_task_reports_pause_deadline_and_reason(monkeypatch):
    """核心诉求：用户必须能看到“暂停到什么时候、因为什么”。"""
    service = ProcessService()
    monkeypatch.setattr(service, "is_running", lambda task_id: False)

    paused_until = datetime(2026, 10, 2, 20, 29, 18)
    monkeypatch.setattr(
        service.failure_guard,
        "should_skip_start",
        lambda *a, **k: _decision(
            skip=True,
            reason="FAIL_SYS_USER_VALIDATE",
            paused_until=paused_until,
            failures=1,
        ),
    )

    async def no_notify(task_name, decision):
        return None

    monkeypatch.setattr(service, "_notify_skip", no_notify)

    result = asyncio.run(service.start_task(0, "macbook pro监控"))

    assert result.started is False
    assert "2026-10-02 20:29:18" in result.reason
    assert "FAIL_SYS_USER_VALIDATE" in result.reason
    assert "1/3" in result.reason
    assert "cookies" in result.reason, "应告诉用户怎么恢复"


def test_spawn_failure_reports_the_underlying_error(monkeypatch, tmp_path):
    service = ProcessService()
    monkeypatch.setattr(service, "is_running", lambda task_id: False)
    monkeypatch.setattr(
        service.failure_guard,
        "should_skip_start",
        lambda *a, **k: _decision(skip=False),
    )
    monkeypatch.setattr(
        "src.services.process_service.build_task_log_path",
        lambda task_id, _name: str(tmp_path / f"task-{task_id}.log"),
    )

    async def boom(*args, **kwargs):
        raise OSError("cannot allocate memory")

    monkeypatch.setattr(asyncio, "create_subprocess_exec", boom)

    result = asyncio.run(service.start_task(0, "task-a"))

    assert result.started is False
    assert "cannot allocate memory" in result.reason


def test_successful_start_reports_no_reason(monkeypatch, tmp_path):
    service = ProcessService()
    monkeypatch.setattr(service, "is_running", lambda task_id: False)
    monkeypatch.setattr(
        service.failure_guard,
        "should_skip_start",
        lambda *a, **k: _decision(skip=False),
    )
    monkeypatch.setattr(
        "src.services.process_service.build_task_log_path",
        lambda task_id, _name: str(tmp_path / f"task-{task_id}.log"),
    )

    async def fake_spawn(*args, **kwargs):
        return _FakeProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)

    result = asyncio.run(service.start_task(0, "task-a"))

    assert result.started is True
    assert result.reason is None


def test_route_returns_409_with_reason_instead_of_generic_500(monkeypatch):
    """可预期的业务状态不该报 500，且 detail 必须带上真实原因。"""

    class _TaskService:
        async def get_task(self, task_id):
            return SimpleNamespace(
                id=task_id, task_name="macbook pro监控", enabled=True, is_running=False
            )

    class _ProcessService:
        async def start_task(self, task_id, task_name):
            return StartTaskResult(False, "任务处于暂停状态，暂停到 2026-10-02 20:29:18。")

    from fastapi import HTTPException

    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(
            tasks_route.start_task(
                1, task_service=_TaskService(), process_service=_ProcessService()
            )
        )

    assert excinfo.value.status_code == 409
    assert "2026-10-02 20:29:18" in excinfo.value.detail


def test_route_reports_success(monkeypatch):
    class _TaskService:
        async def get_task(self, task_id):
            return SimpleNamespace(
                id=task_id, task_name="task-a", enabled=True, is_running=False
            )

    class _ProcessService:
        async def start_task(self, task_id, task_name):
            return StartTaskResult(True)

    payload = asyncio.run(
        tasks_route.start_task(
            1, task_service=_TaskService(), process_service=_ProcessService()
        )
    )

    assert "已启动" in payload["message"]


def test_pause_deadline_formatting_falls_back_when_missing(monkeypatch):
    """没有暂停时间时不该崩，也不能显示成 "None"。"""
    service = ProcessService()
    monkeypatch.setattr(service, "is_running", lambda task_id: False)
    monkeypatch.setattr(
        service.failure_guard,
        "should_skip_start",
        lambda *a, **k: _decision(skip=True, reason="未知原因", paused_until=None),
    )

    async def no_notify(task_name, decision):
        return None

    monkeypatch.setattr(service, "_notify_skip", no_notify)

    result = asyncio.run(service.start_task(0, "task-a"))

    assert "暂停到 N/A" in result.reason
    assert "None" not in result.reason


def test_expired_pause_does_not_skip(monkeypatch):
    """暂停到点后必须重新尝试启动，否则任务会被永久卡死。"""
    service = ProcessService()
    monkeypatch.setattr(service, "is_running", lambda task_id: False)

    past = datetime.now() - timedelta(minutes=1)
    monkeypatch.setattr(
        service.failure_guard,
        "should_skip_start",
        lambda *a, **k: _decision(skip=False, paused_until=past),
    )

    spawned = []

    async def fake_spawn(*args, **kwargs):
        spawned.append(True)
        return _FakeProcess()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_spawn)
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        monkeypatch.setattr(
            "src.services.process_service.build_task_log_path",
            lambda task_id, _name: f"{tmp}/task-{task_id}.log",
        )
        result = asyncio.run(service.start_task(0, "task-a"))

    assert result.started is True
    assert spawned == [True]
