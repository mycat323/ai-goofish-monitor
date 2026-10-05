"""结果更新的 WebSocket 广播测试。

背景：前端 `useResults.ts` 一直在监听 `results_updated` 事件，但后端**从来没
广播过它**（全项目只有 app.py 一处广播 `task_status_changed`）。所以任务运行时
结果页看不到任何新商品，用户只能手动刷新。

爬虫跑在独立子进程里，访问不到后端的 WebSocket 连接，因此“结果已更新”只能由
后端轮询任务状态后代为广播。
"""

from __future__ import annotations

import asyncio
from contextlib import suppress

import src.app as app_module


class _FakeProcessService:
    def __init__(self, processes=None):
        self.processes = dict(processes or {})


def _collect_broadcasts(monkeypatch, *, processes, seconds=0.06, fail_times=0):
    monkeypatch.setattr(app_module, "RESULTS_BROADCAST_INTERVAL_SECONDS", 0.01)
    broadcasts: list[str] = []
    calls = {"n": 0}

    async def fake_broadcast(message_type, data):
        calls["n"] += 1
        if calls["n"] <= fail_times:
            raise RuntimeError("连接已断开")
        broadcasts.append(message_type)

    monkeypatch.setattr(app_module.websocket, "broadcast_message", fake_broadcast)
    monkeypatch.setattr(app_module, "process_service", _FakeProcessService(processes))

    async def _run():
        task = asyncio.create_task(app_module._broadcast_results_while_running())
        await asyncio.sleep(seconds)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    asyncio.run(_run())
    return broadcasts


def test_no_broadcast_when_no_task_is_running(monkeypatch):
    """没任务在跑就别无谓地让前端反复拉取。"""
    assert _collect_broadcasts(monkeypatch, processes={}) == []


def test_broadcasts_results_updated_while_a_task_runs(monkeypatch):
    """核心诉求：任务运行期间前端能收到刷新通知，不必等任务跑完。"""
    broadcasts = _collect_broadcasts(monkeypatch, processes={1: object()})

    assert broadcasts, "任务运行期间必须广播结果更新"
    assert set(broadcasts) == {"results_updated"}


def test_broadcast_failure_does_not_kill_the_loop(monkeypatch):
    """一次广播失败（客户端断开）不能让整个循环停掉。"""
    broadcasts = _collect_broadcasts(
        monkeypatch, processes={1: object()}, fail_times=1
    )

    assert broadcasts, "首次广播失败后仍应继续广播"


def test_loop_stops_when_the_task_finishes(monkeypatch):
    """任务结束后不再广播。"""
    monkeypatch.setattr(app_module, "RESULTS_BROADCAST_INTERVAL_SECONDS", 0.01)
    broadcasts = []
    state = {"processes": {1: object()}}

    async def fake_broadcast(message_type, data):
        broadcasts.append(message_type)

    monkeypatch.setattr(app_module.websocket, "broadcast_message", fake_broadcast)
    monkeypatch.setattr(
        app_module, "process_service", _FakeProcessService(state["processes"])
    )

    async def _run():
        task = asyncio.create_task(app_module._broadcast_results_while_running())
        await asyncio.sleep(0.05)
        running = len(broadcasts)

        # 模拟任务结束
        app_module.process_service.processes.clear()
        await asyncio.sleep(0.05)
        stopped = len(broadcasts)

        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        return running, stopped

    running, stopped = asyncio.run(_run())

    assert running > 0
    assert stopped == running, "任务结束后不应再产生新的广播"
