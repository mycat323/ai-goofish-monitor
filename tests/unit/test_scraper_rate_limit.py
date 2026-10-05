"""区分「限流」与「可交互的人机验证」。

背景（真人实测）：窗口里出现「亲，请拖动下方滑块完成验证」，用户拖了，
但**界面一直转圈、过不去**。对照闲鱼的返回串：

    ['FAIL_SYS_USER_VALIDATE', 'RGV587_ERROR::SM::哎哟喂,被挤爆啦,请稍后重试']

`RGV587_ERROR` 是**限流**语义（“被挤爆啦,请稍后重试”），真人拖对了滑块也
解不开——限流要的是冷却时间。而人工在环流程原先会对所有 FAIL_SYS_USER_VALIDATE
一律等待 10 分钟，并且通知里写着“请在浏览器窗口中完成验证”，属于**错误指令**，
让人白费力气，还白等 10 分钟、白占浏览器资源。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src import scraper
from src.scraper import _is_rate_limited_ret


REAL_RATE_LIMIT_RET = (
    "['FAIL_SYS_USER_VALIDATE', 'RGV587_ERROR::SM::哎哟喂,被挤爆啦,请稍后重试']"
)


@pytest.mark.parametrize(
    "ret_string,expected,label",
    [
        (REAL_RATE_LIMIT_RET, True, "真实实测返回（限流）"),
        ("['FAIL_SYS_RATE_LIMIT']", True, "限流标记"),
        ("['FAIL_SYS_USER_VALIDATE']", False, "纯人机验证，值得等真人"),
        ("['SUCCESS::调用成功']", False, "正常返回"),
        ("", False, "空串"),
    ],
)
def test_rate_limit_detection(ret_string, expected, label):
    assert _is_rate_limited_ret(ret_string) is expected, label


def test_rate_limited_notification_does_not_ask_user_to_drag_a_slider(monkeypatch):
    """限流时不能叫用户去拖滑块——那是徒劳的。"""
    sent = []

    async def fake_send(product_data, reason):
        sent.append((product_data, reason))

    monkeypatch.setattr(scraper, "send_ntfy_notification", fake_send)

    import asyncio

    asyncio.run(scraper._notify_rate_limited("macbook pro监控", REAL_RATE_LIMIT_RET))

    assert len(sent) == 1
    product_data, reason = sent[0]
    assert "限流" in product_data["商品标题"]
    assert "macbook pro监控" in reason
    assert "不是" in reason and "滑块" in reason, "必须明确说明拖滑块没用"
    assert "冷却" in reason, "要告诉用户真正的解法"
    assert "cookies" in reason, "要给出恢复路径"


def test_rate_limit_branch_runs_before_the_manual_wait():
    """结构守卫：命中限流时必须跳过人工等待。

    否则又会出现“明明拖了也过不去，却白等 10 分钟”的情况。
    """
    source = Path(scraper.__file__).read_text(encoding="utf-8")

    limit_index = source.index("rate_limited = _is_rate_limited_ret(ret_string)")
    wait_index = source.index("if await _wait_for_manual_verification(")

    assert limit_index < wait_index, "限流判定必须发生在人工等待之前"


def test_block_site_uses_the_rate_limit_classifier():
    """确保探测调用点确实用了分类器，而不是又回到“一律等待”。"""
    source = Path(scraper.__file__).read_text(encoding="utf-8")
    assert "_is_rate_limited_ret(ret_string)" in source
    assert "_notify_rate_limited(" in source
