"""风控「人工在环」等待门。

检测到闲鱼要求人机验证时，与其直接中止整个任务，不如：

1. 立刻发通知（企业微信 / Bark / Telegram …）告诉用户需要处理
2. 保留浏览器窗口，轮询等待用户完成验证
3. 验证通过 → 继续后续商品；超时 → 按原逻辑安全退出并记录

**这不是自动过验证。** 这里不做任何滑块求解或轨迹伪造——只是把「必须有人
在场」从「你必须守着整个任务跑」降低成「收到通知后花 10 秒点一下」。真人完成
站点要求的验证是正常使用行为；自动化绕过人机验证既违反站点规则，也会因为被
判定为自动化而更快地把账号推向封禁。

仅在有可见窗口时才值得等待（`RUN_HEADLESS=false`）。无头模式下没人能看到
窗口，等待只会白白拖时间，因此由调用方把 `wait_seconds` 置 0 关闭。
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable, Optional


VerificationProbe = Callable[[], Awaitable[bool]]
NotifyCallback = Callable[[], Awaitable[None]]

DEFAULT_RISK_CONTROL_WAIT_SECONDS = 600
DEFAULT_RISK_CONTROL_POLL_SECONDS = 30


async def wait_for_manual_verification(
    *,
    probe: VerificationProbe,
    wait_seconds: int,
    notify: Optional[NotifyCallback] = None,
    poll_interval_seconds: int = DEFAULT_RISK_CONTROL_POLL_SECONDS,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    now: Callable[[], float] = time.monotonic,
) -> bool:
    """等待真人完成人机验证。

    Args:
        probe: 判断验证是否已解除；返回 True 表示可以继续。
        wait_seconds: 最长等待秒数；<= 0 表示关闭该能力（直接返回 False）。
        notify: 开始等待前调用的通知回调；失败只告警，不中断。
        poll_interval_seconds: 轮询间隔。
        sleep / now: 便于测试注入。

    Returns:
        True 表示验证已通过、可以继续；False 表示超时或未启用。
    """
    if wait_seconds <= 0:
        return False

    if notify is not None:
        try:
            await notify()
        except Exception as e:
            # 通知失败不能妨碍等待本身
            print(f"警告：发送人机验证通知失败: {e}")

    interval = max(1, int(poll_interval_seconds))
    print(
        f"检测到需要人机验证：已发送通知，请在浏览器窗口中完成验证。"
        f"最多等待 {wait_seconds} 秒（每 {interval} 秒检查一次）..."
    )

    deadline = now() + wait_seconds
    while now() < deadline:
        await sleep(interval)
        try:
            if await probe():
                print("人机验证已通过，继续执行后续商品。")
                return True
        except Exception as e:
            # 探测本身出错（例如页面已关闭）时继续等待，交给超时兜底
            print(f"检查验证状态失败，继续等待: {e}")

    print(f"等待人工验证超时（{wait_seconds} 秒），按安全退出处理。")
    return False
