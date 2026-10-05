"""详情页访问节奏倍率（DETAIL_DELAY_MULTIPLIER）的测试。

详情页是被风控得最严的接口：实测限额只打在 `idle.pc.detail` 上，同一会话下
卖家资料、图片 CDN 等其它接口仍可正常访问。因此它的访问节奏单独抽出来，
便于按倍率统一调整。

倍率的基准是代码中原有的三段等待（访问详情前 2~4s + 提交后 5~10s +
关闭页面后 2~4s，合计约 9~18s/商品）——项目里并没有实测出来的“安全间隔”。
"""

from __future__ import annotations

import asyncio

import pytest

from src import scraper


@pytest.fixture()
def recorded(monkeypatch):
    """捕获 _detail_sleep 实际传给 random_sleep 的范围。"""
    calls: list[tuple[float, float]] = []

    async def fake_random_sleep(low, high):
        calls.append((low, high))

    monkeypatch.setattr(scraper, "random_sleep", fake_random_sleep)
    return calls


@pytest.mark.parametrize(
    "multiplier,expected",
    [
        (1.0, [(2.0, 4.0), (5.0, 10.0), (2.0, 4.0)]),
        (2.0, [(4.0, 8.0), (10.0, 20.0), (4.0, 8.0)]),
        (3.0, [(6.0, 12.0), (15.0, 30.0), (6.0, 12.0)]),
    ],
)
def test_multiplier_scales_all_three_detail_waits(monkeypatch, recorded, multiplier, expected):
    monkeypatch.setattr(scraper, "DETAIL_DELAY_MULTIPLIER", multiplier)

    async def run():
        await scraper._detail_sleep(2, 4)
        await scraper._detail_sleep(5, 10)
        await scraper._detail_sleep(2, 4)

    asyncio.run(run())

    assert recorded == expected


def test_default_multiplier_keeps_historical_behaviour(monkeypatch, recorded):
    """倍率 1 必须与改动前的硬编码值逐字一致（零回归）。"""
    monkeypatch.setattr(scraper, "DETAIL_DELAY_MULTIPLIER", 1.0)

    asyncio.run(scraper._detail_sleep(5, 10))

    assert recorded == [(5.0, 10.0)]


@pytest.mark.parametrize("bad", [0.0, -1.0])
def test_non_positive_multiplier_falls_back_to_one(monkeypatch, recorded, bad):
    """0 或负数会让等待退化成忙循环，应回退为 1。"""
    monkeypatch.setattr(scraper, "DETAIL_DELAY_MULTIPLIER", bad)

    asyncio.run(scraper._detail_sleep(2, 4))

    assert recorded == [(2.0, 4.0)]


def test_non_detail_waits_are_not_scaled():
    """结构守卫：只有详情页节奏走 _detail_sleep，其它等待（筛选/搜索/翻页）不受影响。"""
    from pathlib import Path

    source = Path(scraper.__file__).read_text(encoding="utf-8")

    assert source.count("await _detail_sleep(") == 3, (
        "应恰好有 3 处详情页节奏走倍率：访问详情前 / 提交后 / 关闭页面后"
    )
    # 翻页间的长休息属于另一种节奏，不应被详情倍率放大
    assert "await random_sleep(10, 15)" in source
