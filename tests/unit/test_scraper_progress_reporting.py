"""爬虫运行报告的正确性测试（进度计数 + 终止条件措辞）。

背景（真实事故）：一次运行被风控中止后，日志报告：

    任务 'macbook pro监控' 正常结束，本次运行共处理了 0 个新商品。
    爬取过程中发生未知错误: FAIL_SYS_USER_VALIDATE

但实际已经写入了 33 条结果。原因是完成计数是 `_run_scrape_attempt` 的局部
变量，风控抛异常时外层的 `+=` 被跳过，计数归零；而 `FAIL_SYS_USER_VALIDATE`
明明是已经分类好的终止条件，却被报成“未知错误”，会把排查方向引向代码 bug。

说明：`_run_scrape_attempt` 是 `scrape_xianyu` 内部的嵌套函数，且需要完整的
Playwright 对象树才能驱动，因此这里用「签名契约 + 源码结构守卫」组合锁定行为。
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

from src import scraper


def _source() -> str:
    return Path(scraper.__file__).read_text(encoding="utf-8")


def _find_nested_function(tree: ast.AST, name: str) -> ast.AsyncFunctionDef:
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == name:
            return node
    raise AssertionError(f"未找到函数 {name}")


def test_run_scrape_attempt_receives_a_shared_progress_container():
    """计数必须由调用方传入，不能是函数内的局部变量。

    这条是本 bug 的核心：局部变量在异常路径下会被整体丢弃。
    """
    tree = ast.parse(_source())
    func = _find_nested_function(tree, "_run_scrape_attempt")
    arg_names = [a.arg for a in func.args.args]

    assert "progress" in arg_names, (
        "_run_scrape_attempt 必须接收调用方提供的 progress 容器，"
        "否则风控中止时已完成的计数会丢失"
    )


def test_completion_count_is_no_longer_a_local_variable():
    """旧实现用 `processed_item_count` 局部变量，异常时 `+=` 被跳过。"""
    source = _source()
    assert "processed_item_count" not in source, (
        "完成计数仍在用局部变量，异常路径下会丢失进度"
    )


def test_progress_container_is_created_in_the_calling_scope():
    """容器要在 scrape_xianyu 里创建（而非每次尝试内部），才能跨尝试保留。"""
    tree = ast.parse(_source())
    func = _find_nested_function(tree, "scrape_xianyu")
    assigned = {
        target.id
        for node in ast.walk(func)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert "progress" in assigned, "progress 容器应在 scrape_xianyu 作用域内创建"


def test_abort_conditions_are_not_reported_as_unknown_errors():
    """风控/登录失效是已分类的终止条件，措辞不能是“未知错误”。"""
    source = _source()

    marker = "爬取过程中发生未知错误"
    assert marker in source
    index = source.index(marker)

    guard_index = source.rfind("_is_abort_signal(e)", 0, index)
    assert guard_index != -1, (
        "打印“未知错误”之前应先判断 _is_abort_signal(e)，"
        "否则风控中止会被描述成代码 bug"
    )
    assert "任务中止" in source[guard_index:index], (
        "终止条件应有独立的措辞（如“任务中止”）"
    )
