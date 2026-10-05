#!/usr/bin/env python3
"""用当前的分析标准（criteria）重跑历史结果里的 AI 判定。

用途：当你修改了任务的 `ai_prompt_criteria_file`（例如放宽某项要求）后，
把之前按旧标准淘汰掉的结果重新判定一遍，让它们按新标准浮出来。

设计要点：
- **不访问闲鱼的风控接口**。只从图片 CDN 重新取图（该通道实测不受详情接口
  风控影响），然后调用 AI。因此不会给账号带来额外风控风险。
- **可续跑**：每条结果写入 `ai_analysis.criteria_revision` 标记，重复执行会
  自动跳过已处理过的条目。中途中断（或 Ctrl+C）后直接再跑即可。
- **幂等**：通过 upsert 原地更新同一条记录，不会产生重复行。

用法：
    # 先小批量验证
    python recheck_results.py --task-name "macbook pro监控" --limit 3

    # 确认无误后跑全部被淘汰的结果
    python recheck_results.py --task-name "macbook pro监控"

    # 只看统计、不实际调用 AI
    python recheck_results.py --task-name "macbook pro监控" --dry-run

常用参数：
    --limit N        最多处理 N 条
    --max-images N   每条最多重新下载几张图（默认 6，0 表示不下载图片）
    --revision TAG   标记名，换标准重跑时改一下即可让所有条目重跑
    --only-rejected  只处理 is_recommended=0 的（默认）
    --all            处理该结果集下的所有条目
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time

from src.ai_handler import download_all_images, get_ai_analysis
from src.infrastructure.persistence.sqlite_connection import sqlite_connection
from src.infrastructure.persistence.storage_names import build_result_filename
from src.services.result_storage_service import upsert_result_record


DEFAULT_REVISION = "recheck-v1"
DEFAULT_MAX_IMAGES = 6


def _load_task_row(args: argparse.Namespace) -> dict:
    """按 id 或名称解析任务；优先使用 --task-id。

    任务名常常包含中文，经 shell / PowerShell 传参时容易被编码破坏
    （实测 'macbook pro监控' 变成 'pro鐩戞帶'），因此提供纯数字的 --task-id
    作为脚本调用时的首选方式。
    """
    with sqlite_connection() as conn:
        if args.task_id is not None:
            row = conn.execute(
                "SELECT id, task_name, keyword, ai_prompt_base_file,"
                " ai_prompt_criteria_file FROM tasks WHERE id = ? LIMIT 1",
                (args.task_id,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT id, task_name, keyword, ai_prompt_base_file,"
                " ai_prompt_criteria_file FROM tasks WHERE task_name = ? LIMIT 1",
                (args.task_name,),
            ).fetchone()
    if row is None:
        target = args.task_id if args.task_id is not None else repr(args.task_name)
        raise SystemExit(f"未找到任务: {target}")
    return dict(row)


def _load_active_criteria(task: dict) -> tuple[str, str]:
    """返回 (任务使用的 criteria 文件路径, 完整 prompt 文本)。"""
    base_file = task.get("ai_prompt_base_file") or "prompts/base_prompt.txt"
    criteria_file = task.get("ai_prompt_criteria_file") or ""
    if not criteria_file or not os.path.exists(criteria_file):
        raise SystemExit(f"任务的 criteria 文件不存在: {criteria_file!r}")

    with open(base_file, "r", encoding="utf-8") as fh:
        base_text = fh.read()
    with open(criteria_file, "r", encoding="utf-8") as fh:
        criteria_text = fh.read()

    if "{{CRITERIA_SECTION}}" not in base_text:
        raise SystemExit(f"基础 prompt 缺少 {{{{CRITERIA_SECTION}}}} 占位符: {base_file}")

    return criteria_file, base_text.replace("{{CRITERIA_SECTION}}", criteria_text)


def _load_targets(
    keyword: str, *, only_rejected: bool, revision: str
) -> tuple[list[dict], int]:
    filename = build_result_filename(keyword)
    where = "result_filename = ?"
    params: list = [filename]
    if only_rejected:
        where += " AND is_recommended = 0"

    with sqlite_connection() as conn:
        rows = list(
            conn.execute(
                f"SELECT id, item_id, title, raw_json FROM result_items"
                f" WHERE {where} ORDER BY id",
                params,
            )
        )

    targets, skipped = [], 0
    for row in rows:
        try:
            raw = json.loads(row["raw_json"] or "{}")
        except json.JSONDecodeError:
            continue
        if (raw.get("ai_analysis") or {}).get("criteria_revision") == revision:
            skipped += 1
            continue
        targets.append({"id": row["id"], "raw": raw, "title": row["title"]})
    return targets, skipped


def _cleanup(paths: list[str]) -> None:
    for path in paths:
        try:
            if os.path.exists(path):
                os.remove(path)
        except OSError:
            pass


async def _process(
    target: dict,
    *,
    prompt_text: str,
    keyword: str,
    task_name: str,
    revision: str,
    max_images: int,
    index: int,
    total: int,
) -> str:
    """返回 'recommended' / 'rejected' / 'failed'。"""
    raw = target["raw"]
    item = raw.get("商品信息") or {}
    item_id = str(item.get("商品ID") or target["id"])
    label = f"[{index}/{total}] {item_id} {(target['title'] or '')[:28]}"

    image_paths: list[str] = []
    try:
        if max_images > 0:
            urls = [u for u in (item.get("商品图片列表") or []) if str(u).startswith("http")]
            image_paths = await download_all_images(
                item_id, urls[:max_images], task_name
            )

        result = await get_ai_analysis(raw, image_paths, prompt_text)
        if not result:
            print(f"{label} -> 失败（AI 未返回结果）", flush=True)
            return "failed"

        result["analysis_source"] = "ai"
        result["keyword_hit_count"] = 0
        result["criteria_revision"] = revision
        raw["ai_analysis"] = result

        await upsert_result_record(raw, keyword)

        verdict = "recommended" if result.get("is_recommended") else "rejected"
        tags = ",".join(result.get("risk_tags") or [])[:60]
        print(f"{label} -> {verdict}  图片{len(image_paths)}张  tags={tags}", flush=True)
        return verdict
    except Exception as exc:  # 单条失败不应中断整批
        print(f"{label} -> 异常: {type(exc).__name__}: {exc}", flush=True)
        return "failed"
    finally:
        _cleanup(image_paths)


async def _main_async(args: argparse.Namespace) -> int:
    task = _load_task_row(args)
    args.task_name = task["task_name"]
    if not args.keyword:
        args.keyword = task["keyword"]

    criteria_file, prompt_text = _load_active_criteria(task)
    print(f"任务: {task['task_name']} (id={task['id']})")
    print(f"结果集关键字: {args.keyword}")
    print(f"使用 criteria: {criteria_file}")
    print(f"prompt 长度: {len(prompt_text)} 字符")
    print(f"revision 标记: {args.revision}")

    targets, skipped = _load_targets(
        args.keyword, only_rejected=not args.all, revision=args.revision
    )
    if args.limit:
        targets = targets[: args.limit]

    print(f"\n待处理: {len(targets)} 条   已处理过（跳过）: {skipped} 条")
    if not targets:
        print("没有需要处理的结果。")
        return 0

    if args.dry_run:
        print("\n--dry-run：仅列出前 5 条，不调用 AI。")
        for t in targets[:5]:
            print(f"  id={t['id']} {(t['title'] or '')[:50]}")
        return 0

    started = time.time()
    counts = {"recommended": 0, "rejected": 0, "failed": 0}
    done = {"n": 0}
    counter_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(max(1, args.concurrency))

    async def worker(index: int, target: dict) -> None:
        async with semaphore:
            verdict = await _process(
                target,
                prompt_text=prompt_text,
                keyword=args.keyword,
                task_name=args.task_name,
                revision=args.revision,
                max_images=args.max_images,
                index=index,
                total=len(targets),
            )
            async with counter_lock:
                counts[verdict] += 1
                done["n"] += 1
                print(
                    f"--- 进度 {done['n']}/{len(targets)}"
                    f"  推荐 {counts['recommended']}"
                    f"  仍不推荐 {counts['rejected']}"
                    f"  失败 {counts['failed']}",
                    flush=True,
                )
            if args.delay > 0:
                await asyncio.sleep(args.delay)

    await asyncio.gather(*(worker(i, t) for i, t in enumerate(targets, start=1)))

    elapsed = time.time() - started
    print("\n" + "=" * 56)
    print(f"完成：共 {len(targets)} 条，用时 {elapsed/60:.1f} 分钟")
    print(f"  改为推荐: {counts['recommended']}")
    print(f"  仍不推荐: {counts['rejected']}")
    print(f"  失败:     {counts['failed']}")
    print("=" * 56)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="用当前 criteria 重跑历史结果的 AI 判定（不访问闲鱼风控接口）"
    )
    parser.add_argument(
        "--task-id", type=int, default=None, help="任务 ID（推荐：避免中文参数被编码破坏）"
    )
    parser.add_argument("--task-name", default=None, help="任务名称（含中文时建议改用 --task-id）")
    parser.add_argument(
        "--keyword",
        default=None,
        help="结果集关键字（默认取任务的 keyword）",
    )
    parser.add_argument("--limit", type=int, default=0, help="最多处理多少条")
    parser.add_argument(
        "--max-images",
        type=int,
        default=DEFAULT_MAX_IMAGES,
        help=f"每条最多重新下载几张图（默认 {DEFAULT_MAX_IMAGES}，0 = 不下载）",
    )
    parser.add_argument("--revision", default=DEFAULT_REVISION, help="标记名")
    parser.add_argument("--delay", type=float, default=0.5, help="每条之间的间隔秒数")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="并发处理的条数（默认 1）。调大可显著缩短总时长，但会加大 AI 端压力。",
    )
    parser.add_argument("--all", action="store_true", help="处理全部结果（默认只处理被淘汰的）")
    parser.add_argument("--dry-run", action="store_true", help="只统计，不调用 AI")
    args = parser.parse_args()

    if args.task_id is None and not args.task_name:
        parser.error("必须提供 --task-id 或 --task-name")

    try:
        return asyncio.run(_main_async(args))
    except KeyboardInterrupt:
        print("\n已中断。直接重新执行即可续跑（已处理的会被跳过）。")
        return 130


if __name__ == "__main__":
    sys.exit(main())
