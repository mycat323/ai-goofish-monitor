"""风控/登录失效必须中断任务，不能被当成“单个商品失败”吞掉。

背景（真实事故）：详情页检测到 ``FAIL_SYS_USER_VALIDATE`` 后会打印
“程序将终止”并休眠，但该 ``RiskControlError`` 随后被 item 循环里宽泛的
``except Exception`` 捕获，降级成“处理商品详情时发生未知错误”，循环继续
处理下一个商品。实测一次运行里触发了 146 次风控、200 次详情页抓取，
即账号已被标记后仍在持续冲撞——这是最容易导致封号的行为。

说明：``_run_scrape_attempt`` 是 ``scrape_xianyu`` 内部的嵌套函数，且需要
完整的 Playwright 对象树才能驱动，因此这里用「策略单测 + 源码结构守卫」
组合来锁定行为，而不是构造一个脆弱的浏览器替身。
"""

from __future__ import annotations

from pathlib import Path

from src import scraper
from src.scraper import (
    ABORT_SIGNAL_EXCEPTIONS,
    LoginRequiredError,
    RiskControlError,
    _is_abort_signal,
)


def test_abort_signals_are_recognised():
    assert _is_abort_signal(RiskControlError("FAIL_SYS_USER_VALIDATE")) is True
    assert _is_abort_signal(LoginRequiredError("passport")) is True


def test_abort_signal_policy_covers_exactly_the_documented_exceptions():
    assert set(ABORT_SIGNAL_EXCEPTIONS) == {RiskControlError, LoginRequiredError}


def test_ordinary_errors_are_not_abort_signals():
    """普通错误仍应被降级为“跳过该商品”，不能升级成整任务中止。"""
    assert _is_abort_signal(RuntimeError("普通错误")) is False
    assert _is_abort_signal(ValueError("解析失败")) is False
    assert _is_abort_signal(TimeoutError()) is False


def test_detail_loop_filters_broad_handler_with_abort_signals():
    """详情页处理里的宽泛 except Exception 必须先放行终止信号。

    回归守卫：防止后续有人新增/改写宽泛异常处理时，再次把风控降级成
    “处理商品详情时发生未知错误”并继续硬爬下一个商品。
    """
    source = Path(scraper.__file__).read_text(encoding="utf-8")

    marker = "错误: 处理商品详情时发生未知错误"
    assert marker in source, "详情页宽泛异常处理的位置已变化，请更新本测试"
    index = source.index(marker)

    guard_index = source.rfind("_is_abort_signal(e)", 0, index)
    assert guard_index != -1, (
        "详情页的 except Exception 必须先用 _is_abort_signal(e) 过滤，"
        "否则风控/登录失效会被吞掉"
    )

    # 识别出终止信号后必须 raise，而不是打印一句就继续下一个商品
    assert "raise" in source[guard_index:index], (
        "识别出终止信号后必须重新抛出，否则会继续硬爬"
    )
