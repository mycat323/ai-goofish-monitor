"""卖家资料缓存的磁盘持久化测试。

背景：卖家资料是整条链路最贵的一环（实测 46 次卖家爬取 → 220 次分页 API），
而缓存原先只有内存版、TTL 30 分钟，导致每次运行、每个任务都要把所有卖家
重爬一遍。这里锁定「跨运行复用、过期失效、跨任务共享」这几个关键性质。
"""

from __future__ import annotations

import asyncio
import json

from src.services.seller_profile_cache import (
    DEFAULT_SELLER_PROFILE_CACHE_TTL,
    SellerProfileCache,
)


class _Clock:
    def __init__(self, value: float = 1_000_000.0):
        self.value = value

    def __call__(self) -> float:
        return self.value


def _loader_counter(payload=None):
    calls = []

    async def loader(user_id: str):
        calls.append(user_id)
        return payload if payload is not None else {"user_id": user_id}

    return loader, calls


def test_default_ttl_is_seven_days():
    assert DEFAULT_SELLER_PROFILE_CACHE_TTL == 7 * 24 * 60 * 60


def test_value_survives_across_cache_instances(tmp_path):
    """核心诉求：换一个实例（等价于下一次运行）不再重新抓取。"""
    db = str(tmp_path / "app.sqlite3")
    clock = _Clock()
    loader, calls = _loader_counter({"user_id": "s1", "items": [1, 2]})

    first = SellerProfileCache(ttl_seconds=3600, time_source=clock, db_path=db)
    value = asyncio.run(first.get_or_load("s1", loader))
    assert value == {"user_id": "s1", "items": [1, 2]}
    assert calls == ["s1"]

    # 全新实例 = 下一次运行；内存是空的，必须靠磁盘命中
    second = SellerProfileCache(ttl_seconds=3600, time_source=clock, db_path=db)
    again = asyncio.run(second.get_or_load("s1", loader))

    assert again == {"user_id": "s1", "items": [1, 2]}
    assert calls == ["s1"], "磁盘命中时不应再调用 loader"


def test_expired_entry_is_refetched(tmp_path):
    db = str(tmp_path / "app.sqlite3")
    clock = _Clock()
    loader, calls = _loader_counter()

    cache = SellerProfileCache(ttl_seconds=60, time_source=clock, db_path=db)
    asyncio.run(cache.get_or_load("s1", loader))
    assert calls == ["s1"]

    clock.value += 61  # 越过 TTL
    fresh = SellerProfileCache(ttl_seconds=60, time_source=clock, db_path=db)
    asyncio.run(fresh.get_or_load("s1", loader))

    assert calls == ["s1", "s1"], "过期后应重新抓取"


def test_memory_entry_does_not_outlive_disk_expiry(tmp_path):
    """内存条目的过期时间必须跟随磁盘，不能各自延展。"""
    db = str(tmp_path / "app.sqlite3")
    clock = _Clock()
    loader, calls = _loader_counter()

    writer = SellerProfileCache(ttl_seconds=60, time_source=clock, db_path=db)
    asyncio.run(writer.get_or_load("s1", loader))

    # 读到磁盘条目后，内存里的 TTL 也只应剩原条目的剩余时间
    reader = SellerProfileCache(ttl_seconds=999_999, time_source=clock, db_path=db)
    asyncio.run(reader.get_or_load("s1", loader))
    assert calls == ["s1"]

    clock.value += 61
    asyncio.run(reader.get_or_load("s1", loader))
    assert calls == ["s1", "s1"], "磁盘已过期，内存也不应继续命中"


def test_cache_is_shared_across_tasks(tmp_path):
    """不同实例（不同任务）读写同一张表，卖家重叠时只抓一次。"""
    db = str(tmp_path / "app.sqlite3")
    clock = _Clock()
    loader, calls = _loader_counter()

    task_a = SellerProfileCache(ttl_seconds=3600, time_source=clock, db_path=db)
    task_b = SellerProfileCache(ttl_seconds=3600, time_source=clock, db_path=db)

    asyncio.run(task_a.get_or_load("shared-seller", loader))
    asyncio.run(task_b.get_or_load("shared-seller", loader))

    assert calls == ["shared-seller"]


def test_corrupt_payload_falls_back_to_loader(tmp_path):
    db = str(tmp_path / "app.sqlite3")
    clock = _Clock()
    cache = SellerProfileCache(ttl_seconds=3600, time_source=clock, db_path=db)

    from src.infrastructure.persistence.sqlite_bootstrap import bootstrap_sqlite_storage
    from src.infrastructure.persistence.sqlite_connection import sqlite_connection

    bootstrap_sqlite_storage(db)
    with sqlite_connection(db) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO seller_profile_cache"
            " (user_id, payload, expires_at, updated_at) VALUES (?, ?, ?, ?)",
            ("broken", "{not json", clock.value + 3600, clock.value),
        )
        conn.commit()

    loader, calls = _loader_counter({"user_id": "broken"})
    value = asyncio.run(cache.get_or_load("broken", loader))

    assert value == {"user_id": "broken"}
    assert calls == ["broken"], "损坏的 payload 应降级为重新抓取"


def test_write_failure_does_not_break_the_run(tmp_path):
    """磁盘写入失败只能降级为“下次重抓”，不能影响本次任务。"""
    loader, calls = _loader_counter({"user_id": "s1"})
    # 指向一个不可能创建的路径，强制写盘失败
    cache = SellerProfileCache(
        ttl_seconds=60, db_path=str(tmp_path / "missing" / "\0bad" / "app.sqlite3")
    )

    value = asyncio.run(cache.get_or_load("s1", loader))

    assert value == {"user_id": "s1"}
    assert calls == ["s1"]


def test_persist_disabled_never_touches_disk(tmp_path):
    db = str(tmp_path / "app.sqlite3")
    clock = _Clock()
    loader, calls = _loader_counter()

    cache = SellerProfileCache(
        ttl_seconds=3600, time_source=clock, persist=False, db_path=db
    )
    asyncio.run(cache.get_or_load("s1", loader))

    assert not (tmp_path / "app.sqlite3").exists(), "persist=False 不应创建数据库文件"

    # 新实例（内存为空）必须重新抓取
    again = SellerProfileCache(
        ttl_seconds=3600, time_source=clock, persist=False, db_path=db
    )
    asyncio.run(again.get_or_load("s1", loader))
    assert calls == ["s1", "s1"]
