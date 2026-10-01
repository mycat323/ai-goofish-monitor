"""风控「人工在环」等待门的测试。

目标：把「必须有人守着整个任务」降低为「收到通知后点一下」，同时保证
无头模式（没人能看到窗口）下行为与之前完全一致。

注意这里**不测试任何自动过验证**——本模块刻意不做滑块求解，只等真人。
"""

from __future__ import annotations

import asyncio

import pytest

from src import scraper
from src.services.risk_control_gate import wait_for_manual_verification


class _Clock:
    """假时钟：sleep 推进时间，使等待逻辑可确定性测试。"""

    def __init__(self) -> None:
        self.t = 0.0
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self.t

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


def _run(coro):
    return asyncio.run(coro)


def test_zero_wait_disables_the_gate_entirely():
    """wait_seconds=0（无头模式的等价物）必须完全不等待、也不发通知。"""
    clock = _Clock()
    notified = []

    async def notify():
        notified.append(True)

    async def probe():
        return True

    result = _run(
        wait_for_manual_verification(
            probe=probe,
            wait_seconds=0,
            notify=notify,
            sleep=clock.sleep,
            now=clock.now,
        )
    )

    assert result is False
    assert notified == [], "不等待时不应发“请去验证”的通知"
    assert clock.sleeps == []


def test_notification_is_sent_before_waiting_and_gate_returns_true_on_recovery():
    """先发通知再轮询；验证通过后立刻返回 True 继续任务。"""
    clock = _Clock()
    events: list[str] = []
    probes = {"count": 0}

    async def notify():
        events.append("notify")

    async def probe():
        probes["count"] += 1
        events.append(f"probe{probes['count']}")
        return probes["count"] >= 2  # 第二次轮询时用户已过验证

    result = _run(
        wait_for_manual_verification(
            probe=probe,
            wait_seconds=600,
            notify=notify,
            poll_interval_seconds=30,
            sleep=clock.sleep,
            now=clock.now,
        )
    )

    assert result is True
    assert probes["count"] == 2
    assert events[0] == "notify", "通知必须先于第一次等待发出"
    assert clock.sleeps == [30, 30], "轮询间隔应被遵守"


def test_timeout_returns_false():
    clock = _Clock()

    async def probe():
        return False

    result = _run(
        wait_for_manual_verification(
            probe=probe,
            wait_seconds=60,
            poll_interval_seconds=30,
            sleep=clock.sleep,
            now=clock.now,
        )
    )

    assert result is False
    # 60 秒 / 30 秒 轮询 => 两次探测后到点
    assert clock.sleeps == [30, 30]


def test_notification_failure_does_not_prevent_waiting():
    """通知渠道坏了也必须继续等待用户，而不是直接放弃。"""
    clock = _Clock()
    probes = {"count": 0}

    async def notify():
        raise RuntimeError("通知服务不可用")

    async def probe():
        probes["count"] += 1
        return True

    result = _run(
        wait_for_manual_verification(
            probe=probe,
            wait_seconds=600,
            notify=notify,
            poll_interval_seconds=30,
            sleep=clock.sleep,
            now=clock.now,
        )
    )

    assert result is True
    assert probes["count"] == 1


def test_probe_errors_do_not_abort_the_wait():
    """探测本身出错（页面被关掉等）应继续等待，交给超时兜底。"""
    clock = _Clock()
    probes = {"count": 0}

    async def probe():
        probes["count"] += 1
        if probes["count"] == 1:
            raise RuntimeError("页面已关闭")
        return True

    result = _run(
        wait_for_manual_verification(
            probe=probe,
            wait_seconds=600,
            poll_interval_seconds=30,
            sleep=clock.sleep,
            now=clock.now,
        )
    )

    assert result is True
    assert probes["count"] == 2


def test_poll_interval_is_clamped_to_at_least_one_second():
    """轮询间隔小于 1 秒没有任何意义，且会放大请求量。"""
    clock = _Clock()

    async def probe():
        return False

    _run(
        wait_for_manual_verification(
            probe=probe,
            wait_seconds=2,
            poll_interval_seconds=0,
            sleep=clock.sleep,
            now=clock.now,
        )
    )

    assert clock.sleeps == [1, 1]


def test_scraper_disables_wait_in_headless_mode(monkeypatch):
    """无头模式没人能看到窗口，等待只会白拖时间。"""
    monkeypatch.setattr(scraper, "RUN_HEADLESS", True)
    monkeypatch.setattr(scraper, "RISK_CONTROL_WAIT_SECONDS", "600")
    assert scraper._risk_control_wait_seconds() == 0


def test_scraper_honours_wait_seconds_when_headed(monkeypatch):
    monkeypatch.setattr(scraper, "RUN_HEADLESS", False)
    monkeypatch.setattr(scraper, "RISK_CONTROL_WAIT_SECONDS", "900")
    assert scraper._risk_control_wait_seconds() == 900

    monkeypatch.setattr(scraper, "RISK_CONTROL_WAIT_SECONDS", "0")
    assert scraper._risk_control_wait_seconds() == 0, "0 表示显式关闭"

    monkeypatch.setattr(scraper, "RISK_CONTROL_WAIT_SECONDS", "不是数字")
    assert scraper._risk_control_wait_seconds() == 600, "非法值应回退到默认 600"


class _FakeLocator:
    def __init__(self, visible: bool):
        self._visible = visible
        self.first = self

    async def is_visible(self) -> bool:
        return self._visible


class _FakePage:
    def __init__(self, visible_selectors: set[str]):
        self._visible = visible_selectors
        self.queried: list[str] = []

    def locator(self, selector: str):
        self.queried.append(selector)
        return _FakeLocator(selector in self._visible)


@pytest.mark.parametrize(
    "visible,expected",
    [
        (set(), False),
        ({"div.baxia-dialog-mask"}, True),
        ({"div.J_MIDDLEWARE_FRAME_WIDGET"}, True),
        ({"iframe[src*='nocaptcha']"}, True),
    ],
)
def test_verification_ui_detection(visible, expected):
    page = _FakePage(visible)
    assert _run(scraper._verification_ui_visible(page)) is expected


def test_overlay_cleared_is_the_inverse_of_ui_visible():
    """搜索页的探测就是“遮罩有没有消失”，不发起任何额外请求。"""
    assert _run(scraper._overlay_cleared(_FakePage(set()))) is True
    assert _run(scraper._overlay_cleared(_FakePage({"div.baxia-dialog-mask"}))) is False


def test_locator_errors_are_treated_as_not_visible():
    """定位器报错（页面已关闭）不应让探测崩溃。"""

    class _BrokenPage:
        def locator(self, selector):
            raise RuntimeError("页面已关闭")

    assert _run(scraper._verification_ui_visible(_BrokenPage())) is False
    assert _run(scraper._overlay_cleared(_BrokenPage())) is True
