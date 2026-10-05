"""「发现即落盘」的增量保存测试。

背景（真实事故）：一次运行日志声明「累计处理 15 个新商品」，但数据库只多了
6 条——**9 条（60%）丢了**。

原因：分析流水线（爬卖家完整资料 + AI，单个 30s~2min、并发只有 2）比「发现」
（约 14s/个）慢得多，队列必然积压。原先只有在分析完成后才写库，因此任务被
停止 / 进程猝死 / 风控中止时，积压的那批会整批丢失。

改法：发现时就先落盘一条待分析记录，分析完成后再原地更新同一条。
"""

from __future__ import annotations

import asyncio
import json

import pytest

from src.infrastructure.persistence.sqlite_connection import sqlite_connection
from src.services.result_storage_service import (
    save_result_record,
    upsert_result_record,
)


@pytest.fixture(autouse=True)
def _isolated_db(monkeypatch, tmp_path):
    """把结果库指向临时文件，避免污染真实 data/app.sqlite3。"""
    monkeypatch.setenv("APP_DATABASE_FILE", str(tmp_path / "results.sqlite3"))
    yield


KEYWORD = "test-keyword"


def _record(**overrides):
    record = {
        "爬取时间": "2026-10-05T22:10:00",
        "搜索关键字": KEYWORD,
        "任务名称": "测试任务",
        "商品信息": {
            "商品ID": "T1",
            "商品标题": "一台 MacBook Pro",
            "当前售价": "¥10000",
            "商品链接": "https://www.goofish.com/item?id=T1",
        },
        "卖家信息": {},
    }
    record.update(overrides)
    return record


def _fetch(item_id="T1"):
    with sqlite_connection() as conn:
        return conn.execute(
            "SELECT title, is_recommended, analysis_source, status, raw_json,"
            " COUNT(1) OVER () AS total FROM result_items WHERE item_id = ?",
            (item_id,),
        ).fetchone()


def test_discovered_item_is_persisted_immediately():
    """发现即落盘：此时还没有 AI 分析结果，但商品已经在结果库里了。"""
    asyncio.run(save_result_record(_record(), KEYWORD))

    row = _fetch()
    assert row is not None, "商品必须在发现时就被保存"
    assert row["title"] == "一台 MacBook Pro"
    assert row["is_recommended"] == 0
    assert row["analysis_source"] is None
    assert "ai_analysis" not in (row["raw_json"] or "")


def test_analysis_updates_the_same_row_without_duplicating():
    """核心诉求：分析完成后原地更新，不能新增一行、也不能丢分析结果。"""
    asyncio.run(save_result_record(_record(), KEYWORD))

    analyzed = _record(
        卖家信息={"卖家昵称": "某卖家"},
        ai_analysis={
            "is_recommended": True,
            "analysis_source": "ai",
            "reason": "符合要求",
            "keyword_hit_count": 0,
        },
    )
    asyncio.run(upsert_result_record(analyzed, KEYWORD))

    row = _fetch()
    assert row["total"] == 1, "必须是原地更新，不能产生重复记录"
    assert row["is_recommended"] == 1
    assert row["analysis_source"] == "ai"
    assert json.loads(row["raw_json"])["ai_analysis"]["reason"] == "符合要求"


def test_plain_save_does_not_overwrite_an_existing_row():
    """钉住为什么必须用 UPSERT。

    save_result_record 走的是 INSERT OR IGNORE：行已存在时会被**静默忽略**。
    如果分析完成后仍然用它写回，AI 结果永远存不进去（而且毫无报错）。
    """
    asyncio.run(save_result_record(_record(), KEYWORD))
    analyzed = _record(ai_analysis={"is_recommended": True, "analysis_source": "ai"})

    asyncio.run(save_result_record(analyzed, KEYWORD))

    row = _fetch()
    assert row["is_recommended"] == 0, "INSERT OR IGNORE 不会覆盖既有行"
    assert row["analysis_source"] is None


def test_upsert_preserves_user_set_status():
    """分析回写不能把用户手动设置的「隐藏」状态覆盖掉。"""
    asyncio.run(save_result_record(_record(), KEYWORD))
    with sqlite_connection() as conn:
        conn.execute(
            "UPDATE result_items SET status = 'hidden' WHERE item_id = 'T1'"
        )
        conn.commit()

    asyncio.run(
        upsert_result_record(
            _record(ai_analysis={"is_recommended": True, "analysis_source": "ai"}),
            KEYWORD,
        )
    )

    assert _fetch()["status"] == "hidden"


def test_upsert_inserts_when_not_previously_saved():
    """兜底：没有经过“发现即落盘”时（例如旧数据回填），UPSERT 应能新增。"""
    asyncio.run(
        upsert_result_record(
            _record(ai_analysis={"is_recommended": True, "analysis_source": "ai"}),
            KEYWORD,
        )
    )

    row = _fetch()
    assert row is not None
    assert row["total"] == 1
    assert row["is_recommended"] == 1


def test_different_items_stay_separate_rows():
    asyncio.run(save_result_record(_record(), KEYWORD))
    other = _record()
    other["商品信息"] = dict(other["商品信息"], 商品ID="T2",
                          商品链接="https://www.goofish.com/item?id=T2",
                          商品标题="另一台")
    asyncio.run(save_result_record(other, KEYWORD))

    with sqlite_connection() as conn:
        total = conn.execute("SELECT COUNT(1) FROM result_items").fetchone()[0]
    assert total == 2
