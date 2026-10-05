"""历史结果重跑工具（recheck_results.py）的关键逻辑测试。

这个工具会被用来批量重跑上百条结果，跑一次要一小时以上，所以两个性质必须可靠：
- **可续跑**：已处理过的条目要能识别出来（靠 ai_analysis.criteria_revision 标记），
  中途中断后重跑不能从头再来。
- **只挑该处理的**：默认只处理被淘汰（is_recommended=0）的结果。
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

from src.infrastructure.persistence.sqlite_connection import sqlite_connection
from src.infrastructure.persistence.storage_names import build_result_filename

REPO_ROOT = Path(__file__).resolve().parents[2]

KEYWORD = "recheck-test-keyword"
REVISION = "recheck-v1"


def _load_module():
    """加载仓库根目录下的 recheck_results.py（它不是包内模块）。"""
    if "recheck_results" in sys.modules:
        return sys.modules["recheck_results"]
    path = REPO_ROOT / "recheck_results.py"
    spec = importlib.util.spec_from_file_location("recheck_results", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["recheck_results"] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def _isolated_db(monkeypatch, tmp_path):
    monkeypatch.setenv("APP_DATABASE_FILE", str(tmp_path / "results.sqlite3"))
    # 真实代码路径里由 save_result_record 内部调用 bootstrap，
    # 本测试直接写库，因此需要自己建表。
    from src.infrastructure.persistence.sqlite_bootstrap import bootstrap_sqlite_storage

    bootstrap_sqlite_storage()
    yield


def _insert(item_id: str, *, is_recommended: int, revision: str | None = None):
    record = {
        "爬取时间": "2026-10-05T22:40:00",
        "搜索关键字": KEYWORD,
        "任务名称": "recheck",
        "商品信息": {
            "商品ID": item_id,
            "商品标题": f"item {item_id}",
            "当前售价": "¥10000",
            "商品链接": f"https://www.goofish.com/item?id={item_id}",
        },
        "卖家信息": {},
    }
    if revision is not None:
        record["ai_analysis"] = {"is_recommended": False, "criteria_revision": revision}
    with sqlite_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO result_items (
                result_filename, keyword, task_name, crawl_time, publish_time, price,
                price_display, item_id, title, link, link_unique_key, seller_nickname,
                is_recommended, analysis_source, keyword_hit_count, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                build_result_filename(KEYWORD),
                KEYWORD,
                "recheck",
                "2026-10-05T22:40:00",
                None,
                10000.0,
                "¥10000",
                item_id,
                f"item {item_id}",
                f"https://www.goofish.com/item?id={item_id}",
                f"item:{item_id}",
                None,
                is_recommended,
                "ai",
                0,
                json.dumps(record, ensure_ascii=False),
            ),
        )
        conn.commit()


def test_only_rejected_items_are_selected_by_default():
    module = _load_module()
    _insert("R1", is_recommended=0)
    _insert("K1", is_recommended=1)  # 已推荐，默认不动

    targets, skipped = module._load_targets(
        KEYWORD, only_rejected=True, revision=REVISION
    )

    assert [t["raw"]["商品信息"]["商品ID"] for t in targets] == ["R1"]
    assert skipped == 0


def test_already_rechecked_items_are_skipped():
    """核心：可续跑——带标记的条目必须被跳过。"""
    module = _load_module()
    _insert("R1", is_recommended=0)                      # 待处理
    _insert("R2", is_recommended=0, revision=REVISION)   # 已处理过

    targets, skipped = module._load_targets(
        KEYWORD, only_rejected=True, revision=REVISION
    )

    assert len(targets) == 1
    assert targets[0]["raw"]["商品信息"]["商品ID"] == "R1"
    assert skipped == 1


def test_different_revision_marks_everything_for_reprocessing():
    """换一个 revision 标记即可让所有条目重跑（用于下次再改标准）。"""
    module = _load_module()
    _insert("R1", is_recommended=0, revision=REVISION)

    targets, skipped = module._load_targets(
        KEYWORD, only_rejected=True, revision="recheck-v2"
    )

    assert len(targets) == 1
    assert skipped == 0


def test_all_flag_includes_recommended_items():
    module = _load_module()
    _insert("R1", is_recommended=0)
    _insert("K1", is_recommended=1)

    targets, _ = module._load_targets(KEYWORD, only_rejected=False, revision=REVISION)

    assert len(targets) == 2


def test_items_without_raw_json_are_ignored():
    module = _load_module()
    with sqlite_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO result_items (
                result_filename, keyword, task_name, crawl_time, publish_time, price,
                price_display, item_id, title, link, link_unique_key, seller_nickname,
                is_recommended, analysis_source, keyword_hit_count, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                build_result_filename(KEYWORD), KEYWORD, "recheck", "t", None, None,
                None, "BAD1", "bad", None, "item:BAD1", None, 0, "ai", 0, "不是JSON{{{",
            ),
        )
        conn.commit()

    targets, skipped = module._load_targets(
        KEYWORD, only_rejected=True, revision=REVISION
    )

    assert targets == []
    assert skipped == 0
