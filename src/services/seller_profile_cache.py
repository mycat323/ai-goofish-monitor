"""卖家资料缓存服务。

两级缓存：
- 进程内字典（命中快、无 IO）
- SQLite（跨运行、跨任务共享）

为什么需要磁盘层：卖家资料是整条抓取链路里最贵的一环——单个卖家要拉完整
商品列表和完整评价历史。实测一次运行里 46 次卖家爬取产生了 131 + 89 = 220
次分页 API 调用，占该次运行闲鱼 API 流量的一半以上。

而原先缓存只有内存版、TTL 30 分钟，于是**每次运行、每个任务都要把所有卖家
重爬一遍**。卖家信誉不会在几小时内变化，且卖家在不同任务之间高度重叠
（卖 MacBook 的人会同时出现在多个 Mac 相关任务里），所以「磁盘持久化 + 长
TTL」是降低总请求量最有效的一环：任务越多、重叠越多，收益越大。
"""

from __future__ import annotations

import asyncio
import copy
import json
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from src.infrastructure.persistence.sqlite_bootstrap import bootstrap_sqlite_storage
from src.infrastructure.persistence.sqlite_connection import sqlite_connection


SellerProfileLoader = Callable[[str], Awaitable[dict]]

# 7 天。卖家信誉/评价不会在几小时内变化，而爬取成本极高。
DEFAULT_SELLER_PROFILE_CACHE_TTL = 7 * 24 * 60 * 60


@dataclass(frozen=True)
class _CacheEntry:
    value: dict
    expires_at: float


class SellerProfileCache:
    """带 TTL、跨运行持久化和并发合并的卖家资料缓存。

    过期时间使用**墙上时钟**（time.time）而非单调时钟，否则写入磁盘的
    expires_at 在下次进程启动时不可比较。
    """

    def __init__(
        self,
        ttl_seconds: int = DEFAULT_SELLER_PROFILE_CACHE_TTL,
        time_source: Optional[Callable[[], float]] = None,
        *,
        persist: bool = True,
        db_path: Optional[str] = None,
    ) -> None:
        self._ttl_seconds = max(0, int(ttl_seconds))
        self._time_source = time_source or time.time
        self._persist = persist
        self._db_path = db_path
        self._entries: dict[str, _CacheEntry] = {}
        self._inflight: dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._bootstrapped = False

    def _now(self) -> float:
        return float(self._time_source())

    def _clone(self, value: dict) -> dict:
        return copy.deepcopy(value)

    def _get_memory_value(self, user_id: str) -> Optional[dict]:
        entry = self._entries.get(user_id)
        if entry is None:
            return None
        if entry.expires_at < self._now():
            self._entries.pop(user_id, None)
            return None
        return self._clone(entry.value)

    def _ensure_storage(self) -> None:
        if self._bootstrapped:
            return
        bootstrap_sqlite_storage(self._db_path)
        self._bootstrapped = True

    def _load_from_disk(self, user_id: str) -> Optional[tuple[dict, float]]:
        """返回 (value, expires_at)；未命中或不可用时返回 None。"""
        if not self._persist:
            return None
        try:
            self._ensure_storage()
            with sqlite_connection(self._db_path) as conn:
                row = conn.execute(
                    "SELECT payload, expires_at FROM seller_profile_cache"
                    " WHERE user_id = ?",
                    (user_id,),
                ).fetchone()
        except Exception as e:
            print(f"警告：读取卖家资料缓存失败，将回退到实际抓取: {e}")
            return None

        if row is None:
            return None
        expires_at = float(row["expires_at"])
        if expires_at < self._now():
            return None
        try:
            value = json.loads(row["payload"])
        except Exception:
            return None
        if not isinstance(value, dict):
            return None
        return value, expires_at

    def _save_to_disk(self, user_id: str, value: dict, expires_at: float) -> None:
        if not self._persist:
            return
        try:
            self._ensure_storage()
            with sqlite_connection(self._db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO seller_profile_cache
                        (user_id, payload, expires_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        user_id,
                        json.dumps(value, ensure_ascii=False),
                        expires_at,
                        self._now(),
                    ),
                )
                # 顺手清理过期条目，避免磁盘缓存无限增长。
                conn.execute(
                    "DELETE FROM seller_profile_cache WHERE expires_at < ?",
                    (self._now(),),
                )
                conn.commit()
        except Exception as e:
            # 缓存写入失败只应降级为“下次重新抓取”，不能影响本次任务。
            print(f"警告：写入卖家资料缓存失败（不影响本次运行）: {e}")

    async def get_or_load(self, user_id: str, loader: SellerProfileLoader) -> dict:
        async with self._lock:
            cached_value = self._get_memory_value(user_id)
            if cached_value is not None:
                return cached_value
            task = self._inflight.get(user_id)
            if task is None:
                task = asyncio.create_task(self._load_and_store(user_id, loader))
                self._inflight[user_id] = task
        return self._clone(await task)

    async def _load_and_store(self, user_id: str, loader: SellerProfileLoader) -> dict:
        try:
            hit = await asyncio.to_thread(self._load_from_disk, user_id)
            if hit is not None:
                value, expires_at = hit
            else:
                value = self._clone(await loader(user_id))
                expires_at = self._now() + self._ttl_seconds
                await asyncio.to_thread(self._save_to_disk, user_id, value, expires_at)

            async with self._lock:
                # 内存条目的过期时间跟随磁盘，避免磁盘上只剩 1 分钟到期
                # 却在内存里被当成 7 天有效。
                self._entries[user_id] = _CacheEntry(
                    value=self._clone(value), expires_at=expires_at
                )
            return value
        finally:
            async with self._lock:
                self._inflight.pop(user_id, None)
