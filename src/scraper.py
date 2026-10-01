import asyncio
import json
import os
import random
from datetime import datetime
from typing import Optional
from urllib.parse import urlencode

from playwright.async_api import (
    Response,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

from src.ai_handler import (
    download_all_images,
    get_ai_analysis,
    send_ntfy_notification,
    cleanup_task_images,
)
from src.config import (
    AI_DEBUG_MODE,
    BROWSER_USER_DATA_DIR,
    DETAIL_API_URL_PATTERN,
    LOGIN_IS_EDGE,
    RUN_HEADLESS,
    RUNNING_IN_DOCKER,
    SKIP_AI_ANALYSIS,
    STATE_FILE,
)
from src.parsers import (
    _parse_search_results_json,
    _parse_user_items_data,
    calculate_reputation_from_ratings,
    parse_ratings_data,
    parse_user_head_data,
)
from src.utils import (
    format_registration_days,
    get_link_unique_key,
    log_time,
    random_sleep,
    safe_get,
    save_to_jsonl,
)
from src.rotation import RotationPool, load_state_files, parse_proxy_pool, RotationItem
from src.failure_guard import FailureGuard
from src.services.account_strategy_service import resolve_account_runtime_plan
from src.infrastructure.persistence.storage_names import build_result_filename
from src.services.item_analysis_dispatcher import (
    ItemAnalysisDispatcher,
    ItemAnalysisJob,
)
from src.services.price_history_service import (
    build_market_reference,
    load_price_snapshots,
    record_market_snapshots,
)
from src.services.result_storage_service import load_processed_link_keys
from src.services.seller_profile_cache import SellerProfileCache
from src.services.search_pagination import (
    advance_search_page,
    is_search_results_response,
)


class RiskControlError(Exception):
    pass


class LoginRequiredError(Exception):
    """Raised when Goofish redirects to the passport/mini_login flow."""


# 必须中断整个任务的异常。
#
# item 循环里存在宽泛的 ``except Exception``（把单个商品的处理失败记下来、
# 继续下一个）。风控与登录失效属于控制流信号，一旦被它吞掉，就会出现
# “打印『程序将终止』、却继续对下一个商品硬爬”的情况：在账号已经被标记的
# 前提下反复冲撞，是最容易导致封号的行为（实测一次运行触发 146 次风控）。
#
# 新增任何宽泛异常处理时，请先用 _is_abort_signal() 过滤。
ABORT_SIGNAL_EXCEPTIONS = (RiskControlError, LoginRequiredError)


def _is_abort_signal(exc: BaseException) -> bool:
    """判断异常是否应该向上传播、中断整个任务。"""
    return isinstance(exc, ABORT_SIGNAL_EXCEPTIONS)


FAILURE_GUARD = FailureGuard()
EDGE_DOCKER_WARNING_PRINTED = False


def _is_login_url(url: str) -> bool:
    if not url:
        return False
    lowered = url.lower()
    return "passport.goofish.com" in lowered or "mini_login" in lowered


def _resolve_browser_channel() -> str:
    global EDGE_DOCKER_WARNING_PRINTED
    if RUNNING_IN_DOCKER:
        if LOGIN_IS_EDGE and not EDGE_DOCKER_WARNING_PRINTED:
            print(
                "检测到 LOGIN_IS_EDGE=true，但 Docker 镜像未内置 Edge，"
                "任务运行时将改用 Chromium。"
            )
            EDGE_DOCKER_WARNING_PRINTED = True
        return "chromium"
    return "msedge" if LOGIN_IS_EDGE else "chrome"


def _should_analyze_images(task_config: dict) -> bool:
    raw_value = task_config.get("analyze_images", True)
    if isinstance(raw_value, bool):
        return raw_value
    return str(raw_value).strip().lower() not in {"false", "0", "no", "off"}


def _format_failure_reason(reason: str, limit: int = 500) -> str:
    if not reason:
        return "未知错误"
    cleaned = " ".join(str(reason).split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3] + "..."


async def _notify_task_failure(
    task_config: dict, reason: str, *, cookie_path: Optional[str]
) -> None:
    task_name = task_config.get("task_name", "未命名任务")
    keyword = task_config.get("keyword", "")
    formatted_reason = _format_failure_reason(reason)

    # Some failures are deterministic misconfiguration and should pause/notify immediately.
    pause_immediately = any(
        marker in formatted_reason
        for marker in (
            "未找到可用的代理地址",
            "未找到可用的登录状态文件",
        )
    )

    try:
        guard_result = FAILURE_GUARD.record_failure(
            task_name,
            formatted_reason,
            cookie_path=cookie_path,
            min_failures_to_pause=1 if pause_immediately else None,
        )
    except Exception as guard_error:
        # 熔断状态只是记账，写不进去不应掩盖真实失败原因，也不能吞掉通知。
        print(f"[FailureGuard] 状态写入失败，仍将发送通知: {guard_error}")
        guard_result = {"should_notify": True, "paused_until": None}

    if not guard_result.get("should_notify"):
        print(
            f"[FailureGuard] 任务 '{task_name}' 失败计数 {guard_result.get('consecutive_failures')}/{FAILURE_GUARD.threshold}，暂不通知。"
        )
        return

    paused_until = guard_result.get("paused_until")
    paused_until_str = (
        paused_until.strftime("%Y-%m-%d %H:%M:%S") if paused_until else "N/A"
    )

    product_data = {
        "商品标题": f"[任务异常] {task_name}",
        "当前售价": "N/A",
        "商品链接": "#",
    }
    notify_reason = (
        f"任务运行失败(已连续 {guard_result.get('consecutive_failures')}/{FAILURE_GUARD.threshold} 次): {formatted_reason}"
        f"\n任务: {task_name}"
        f"\n关键词: {keyword or 'N/A'}"
        f"\n已自动暂停重试，暂停到: {paused_until_str}"
        f"\n修复后(更新登录态/cookies文件)将自动恢复。"
    )

    try:
        await send_ntfy_notification(product_data, notify_reason)
    except Exception as e:
        print(f"发送任务异常通知失败: {e}")


def _as_bool(value, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _as_int(value, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _get_rotation_settings(task_config: dict) -> dict:
    account_cfg = task_config.get("account_rotation") or {}
    proxy_cfg = task_config.get("proxy_rotation") or {}

    account_enabled = _as_bool(
        account_cfg.get("enabled"),
        _as_bool(os.getenv("ACCOUNT_ROTATION_ENABLED"), False),
    )
    account_mode = (
        account_cfg.get("mode") or os.getenv("ACCOUNT_ROTATION_MODE", "per_task")
    ).lower()
    account_state_dir = account_cfg.get("state_dir") or os.getenv(
        "ACCOUNT_STATE_DIR", "state"
    )
    account_retry_limit = _as_int(
        account_cfg.get("retry_limit"),
        _as_int(os.getenv("ACCOUNT_ROTATION_RETRY_LIMIT"), 2),
    )
    account_blacklist_ttl = _as_int(
        account_cfg.get("blacklist_ttl_sec"),
        _as_int(os.getenv("ACCOUNT_BLACKLIST_TTL"), 300),
    )

    proxy_enabled = _as_bool(
        proxy_cfg.get("enabled"), _as_bool(os.getenv("PROXY_ROTATION_ENABLED"), False)
    )
    proxy_mode = (
        proxy_cfg.get("mode") or os.getenv("PROXY_ROTATION_MODE", "per_task")
    ).lower()
    proxy_pool = proxy_cfg.get("proxy_pool") or os.getenv("PROXY_POOL", "")
    proxy_retry_limit = _as_int(
        proxy_cfg.get("retry_limit"),
        _as_int(os.getenv("PROXY_ROTATION_RETRY_LIMIT"), 2),
    )
    proxy_blacklist_ttl = _as_int(
        proxy_cfg.get("blacklist_ttl_sec"),
        _as_int(os.getenv("PROXY_BLACKLIST_TTL"), 300),
    )

    return {
        "account_enabled": account_enabled,
        "account_mode": account_mode,
        "account_state_dir": account_state_dir,
        "account_retry_limit": max(1, account_retry_limit),
        "account_blacklist_ttl": max(0, account_blacklist_ttl),
        "proxy_enabled": proxy_enabled,
        "proxy_mode": proxy_mode,
        "proxy_pool": proxy_pool,
        "proxy_retry_limit": max(1, proxy_retry_limit),
        "proxy_blacklist_ttl": max(0, proxy_blacklist_ttl),
    }


def _get_ai_analysis_concurrency(task_config: dict) -> int:
    configured = task_config.get("ai_analysis_concurrency")
    default = _as_int(os.getenv("AI_ANALYSIS_CONCURRENCY"), 2)
    return max(1, _as_int(configured, default))


def _get_seller_profile_cache_ttl(task_config: dict) -> int:
    configured = task_config.get("seller_profile_cache_ttl")
    default = _as_int(os.getenv("SELLER_PROFILE_CACHE_TTL"), 1800)
    return max(0, _as_int(configured, default))


def _default_context_options() -> dict:
    return {
        "user_agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
        "viewport": {"width": 412, "height": 915},
        "device_scale_factor": 2.625,
        "is_mobile": True,
        "has_touch": True,
        "locale": "zh-CN",
        "timezone_id": "Asia/Shanghai",
        "permissions": ["geolocation"],
        "geolocation": {"longitude": 121.4737, "latitude": 31.2304},
        "color_scheme": "light",
    }


def _clean_kwargs(options: dict) -> dict:
    return {k: v for k, v in options.items() if v is not None}


# 浏览器扩展（Cookie-Editor / EditThisCookie）使用的是另一套 sameSite 词汇表，
# Playwright 只接受 Strict / Lax / None，直接透传会报错或被静默丢弃。
_SAME_SITE_MAP = {
    "no_restriction": "None",
    "none": "None",
    "lax": "Lax",
    "strict": "Strict",
    "unspecified": "Lax",
}


def _normalize_cookies(raw_cookies) -> list:
    """将各种浏览器导出的 cookie 统一成 Playwright 接受的字段与取值。

    同时兼容两种过期时间字段（expires / expirationDate）与扩展的 sameSite 取值。
    """
    cookies = []
    for item in raw_cookies or []:
        if not isinstance(item, dict):
            continue

        name = item.get("name")
        domain = item.get("domain")
        if not name or not domain:
            continue

        cookie = {
            "name": str(name),
            "value": item.get("value") or "",
            "domain": str(domain),
            "path": item.get("path") or "/",
            "secure": bool(item.get("secure", False)),
            "httpOnly": bool(item.get("httpOnly", False)),
        }

        # session cookie 没有过期时间，Playwright 会按会话 cookie 处理。
        expires = item.get("expires", item.get("expirationDate"))
        if isinstance(expires, (int, float)) and not isinstance(expires, bool):
            if expires > 0:
                cookie["expires"] = float(expires)

        raw_same_site = item.get("sameSite")
        if raw_same_site is None:
            raw_same_site = item.get("same_site")
        if raw_same_site:
            mapped = _SAME_SITE_MAP.get(str(raw_same_site).strip().lower())
            if mapped:
                cookie["sameSite"] = mapped

        cookies.append(cookie)
    return cookies


def _as_storage_state(data) -> Optional[dict]:
    """把登录态导出数据统一成 Playwright 的 storage_state 字典。

    支持裸 cookies 数组（Cookie-Editor / EditThisCookie 导出）、标准的
    ``{"cookies": [...], "origins": [...]}``，以及扩展增强快照中的 cookies 字段。
    返回 None 表示无法识别，调用方应回退到原始文件路径。
    """
    if isinstance(data, list):
        raw_cookies = data
        raw_origins = None
    elif isinstance(data, dict) and isinstance(data.get("cookies"), list):
        raw_cookies = data["cookies"]
        raw_origins = data.get("origins")
    else:
        return None

    cookies = _normalize_cookies(raw_cookies)
    if not cookies:
        return None

    return {
        "cookies": cookies,
        "origins": raw_origins if isinstance(raw_origins, list) else [],
    }


def _persistent_profile_dir() -> Optional[str]:
    """返回持久化 profile 目录；未配置时返回 None（沿用旧行为）。"""
    return BROWSER_USER_DATA_DIR or None


def _storage_state_cookies(storage_state) -> list:
    """从传给 context 的 storage_state 中取出归一化后的 cookie 列表。"""
    if isinstance(storage_state, dict):
        cookies = storage_state.get("cookies")
        return cookies if isinstance(cookies, list) else []
    if isinstance(storage_state, str) and os.path.exists(storage_state):
        try:
            with open(storage_state, "r", encoding="utf-8") as f:
                return (_as_storage_state(json.load(f)) or {}).get("cookies") or []
        except Exception as e:
            print(f"警告：读取登录状态文件失败，将不使用 cookie: {e}")
    return []


async def _open_browser_context(
    playwright,
    *,
    storage_state,
    context_kwargs: dict,
    proxy_server: Optional[str] = None,
):
    """创建浏览器 context，返回 (browser, context)。

    未配置 BROWSER_USER_DATA_DIR（默认）时沿用旧行为：launch() + new_context()
    每次得到一个全新的空 profile，再把 storage_state 注入进去。

    配置了 BROWSER_USER_DATA_DIR 时改用 launch_persistent_context：设备指纹
    跨运行稳定，登录态由 profile 持有并自然刷新。注意 launch_persistent_context
    不接受 storage_state（Playwright 1.55），因此用 add_cookies 按需补充。

    cookie 合并策略：只补 profile 里**缺失**的 cookie（按 name/domain/path 判断），
    已存在的一律保留。原因是：
    - profile 里已存在的 token（如 _m_h5_tk）会被闲鱼自然刷新，用导出文件
      覆盖会把它们打回过期状态；
    - 而 Chrome 不持久化 session cookie（cookie2 / _tb_token_ / csg 等），
      重开浏览器就丢，必须从登录态文件补回来，否则下一次运行可能直接掉登录。
    """
    launch_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-web-security",
        "--disable-features=IsolateOrigins,site-per-process",
    ]
    launch_kwargs = {"headless": RUN_HEADLESS, "args": launch_args}
    if proxy_server:
        launch_kwargs["proxy"] = {"server": proxy_server}
    launch_kwargs["channel"] = _resolve_browser_channel()

    profile_dir = _persistent_profile_dir()
    if profile_dir:
        os.makedirs(profile_dir, exist_ok=True)
        print(f"使用持久化浏览器 profile: {profile_dir}")
        context = await playwright.chromium.launch_persistent_context(
            profile_dir, **context_kwargs, **launch_kwargs
        )
        try:
            existing = await context.cookies()
        except Exception:
            existing = []
        wanted = _storage_state_cookies(storage_state)
        have = {(c.get("name"), c.get("domain"), c.get("path")) for c in existing}
        missing = [
            c
            for c in wanted
            if (c.get("name"), c.get("domain"), c.get("path")) not in have
        ]
        if missing:
            await context.add_cookies(missing)
        if not existing:
            print(f"profile 为空，已用登录态文件引导 {len(missing)} 条 cookie。")
        elif missing:
            print(
                f"profile 已有 {len(existing)} 条 cookie，"
                f"补充缺失的 {len(missing)} 条（多为 session cookie）。"
            )
        else:
            print(f"profile 已有 {len(existing)} 条 cookie，全部复用。")
        return None, context

    browser = await playwright.chromium.launch(**launch_kwargs)
    context = await browser.new_context(storage_state=storage_state, **context_kwargs)
    return browser, context


def _looks_like_mobile(ua: str) -> Optional[bool]:
    if not ua:
        return None
    ua_lower = ua.lower()
    if "mobile" in ua_lower or "android" in ua_lower or "iphone" in ua_lower:
        return True
    if "windows" in ua_lower or "macintosh" in ua_lower:
        return False
    return None


def _build_context_overrides(snapshot: dict) -> dict:
    env = snapshot.get("env") or {}
    headers = snapshot.get("headers") or {}
    navigator = env.get("navigator") or {}
    screen = env.get("screen") or {}
    intl = env.get("intl") or {}

    overrides = {}

    ua = (
        headers.get("User-Agent")
        or headers.get("user-agent")
        or navigator.get("userAgent")
    )
    if ua:
        overrides["user_agent"] = ua

    accept_language = headers.get("Accept-Language") or headers.get("accept-language")
    locale = None
    if accept_language:
        locale = accept_language.split(",")[0].strip()
    elif navigator.get("language"):
        locale = navigator["language"]
    if locale:
        overrides["locale"] = locale

    tz = intl.get("timeZone")
    if tz:
        overrides["timezone_id"] = tz

    width = screen.get("width")
    height = screen.get("height")
    if isinstance(width, (int, float)) and isinstance(height, (int, float)):
        overrides["viewport"] = {"width": int(width), "height": int(height)}

    dpr = screen.get("devicePixelRatio")
    if isinstance(dpr, (int, float)):
        overrides["device_scale_factor"] = float(dpr)

    touch_points = navigator.get("maxTouchPoints")
    if isinstance(touch_points, (int, float)):
        overrides["has_touch"] = touch_points > 0

    mobile_flag = _looks_like_mobile(ua or "")
    if mobile_flag is not None:
        overrides["is_mobile"] = mobile_flag

    return _clean_kwargs(overrides)


def _build_extra_headers(raw_headers: Optional[dict]) -> dict:
    if not raw_headers:
        return {}
    excluded = {"cookie", "content-length"}
    headers = {}
    for key, value in raw_headers.items():
        if not key or key.lower() in excluded or value is None:
            continue
        headers[key] = value
    return headers


async def scrape_user_profile(context, user_id: str) -> dict:
    """
    【新版】访问指定用户的个人主页，按顺序采集其摘要信息、完整的商品列表和完整的评价列表。
    """
    print(f"   -> 开始采集用户ID: {user_id} 的完整信息...")
    profile_data = {}
    page = await context.new_page()

    # 为各项异步任务准备Future和数据容器
    head_api_future = asyncio.get_event_loop().create_future()

    all_items, all_ratings = [], []
    stop_item_scrolling, stop_rating_scrolling = asyncio.Event(), asyncio.Event()

    async def handle_response(response: Response):
        # 捕获头部摘要API
        if (
            "mtop.idle.web.user.page.head" in response.url
            and not head_api_future.done()
        ):
            try:
                head_api_future.set_result(await response.json())
                print(f"      [API捕获] 用户头部信息... 成功")
            except Exception as e:
                if not head_api_future.done():
                    head_api_future.set_exception(e)

        # 捕获商品列表API
        elif "mtop.idle.web.xyh.item.list" in response.url:
            try:
                data = await response.json()
                all_items.extend(data.get("data", {}).get("cardList", []))
                print(f"      [API捕获] 商品列表... 当前已捕获 {len(all_items)} 件")
                if not data.get("data", {}).get("nextPage", True):
                    stop_item_scrolling.set()
            except Exception as e:
                stop_item_scrolling.set()

        # 捕获评价列表API
        elif "mtop.idle.web.trade.rate.list" in response.url:
            try:
                data = await response.json()
                all_ratings.extend(data.get("data", {}).get("cardList", []))
                print(f"      [API捕获] 评价列表... 当前已捕获 {len(all_ratings)} 条")
                if not data.get("data", {}).get("nextPage", True):
                    stop_rating_scrolling.set()
            except Exception as e:
                stop_rating_scrolling.set()

    page.on("response", handle_response)

    try:
        # --- 任务1: 导航并采集头部信息 ---
        await page.goto(
            f"https://www.goofish.com/personal?userId={user_id}",
            wait_until="domcontentloaded",
            timeout=20000,
        )
        head_data = await asyncio.wait_for(head_api_future, timeout=15)
        profile_data = await parse_user_head_data(head_data)

        # --- 任务2: 滚动加载所有商品 (默认页面) ---
        print("      [采集阶段] 开始采集该用户的商品列表...")
        await random_sleep(2, 4)  # 等待第一页商品API完成
        while not stop_item_scrolling.is_set():
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            try:
                await asyncio.wait_for(stop_item_scrolling.wait(), timeout=8)
            except asyncio.TimeoutError:
                print("      [滚动超时] 商品列表可能已加载完毕。")
                break
        profile_data["卖家发布的商品列表"] = await _parse_user_items_data(all_items)

        # --- 任务3: 点击并采集所有评价 ---
        print("      [采集阶段] 开始采集该用户的评价列表...")
        rating_tab_locator = page.locator("//div[text()='信用及评价']/ancestor::li")
        if await rating_tab_locator.count() > 0:
            await rating_tab_locator.click()
            await random_sleep(3, 5)  # 等待第一页评价API完成

            while not stop_rating_scrolling.is_set():
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                try:
                    await asyncio.wait_for(stop_rating_scrolling.wait(), timeout=8)
                except asyncio.TimeoutError:
                    print("      [滚动超时] 评价列表可能已加载完毕。")
                    break

            profile_data["卖家收到的评价列表"] = await parse_ratings_data(all_ratings)
            reputation_stats = await calculate_reputation_from_ratings(all_ratings)
            profile_data.update(reputation_stats)
        else:
            print("      [警告] 未找到评价选项卡，跳过评价采集。")

    except Exception as e:
        print(f"   [错误] 采集用户 {user_id} 信息时发生错误: {e}")
    finally:
        page.remove_listener("response", handle_response)
        await page.close()
        print(f"   -> 用户 {user_id} 信息采集完成。")

    return profile_data


async def scrape_xianyu(task_config: dict, debug_limit: int = 0):
    """
    【核心执行器】
    根据单个任务配置，异步爬取闲鱼商品数据，并对每个新发现的商品进行实时的、独立的AI分析和通知。
    """
    keyword = task_config["keyword"]
    max_pages = task_config.get("max_pages", 1)
    personal_only = task_config.get("personal_only", False)
    min_price = task_config.get("min_price")
    max_price = task_config.get("max_price")
    ai_prompt_text = task_config.get("ai_prompt_text", "")
    analyze_images = _should_analyze_images(task_config)
    decision_mode = str(task_config.get("decision_mode", "ai")).strip().lower()
    if decision_mode not in {"ai", "keyword"}:
        decision_mode = "ai"
    keyword_rules = task_config.get("keyword_rules") or []
    free_shipping = task_config.get("free_shipping", False)
    raw_new_publish = task_config.get("new_publish_option") or ""
    new_publish_option = raw_new_publish.strip()
    if new_publish_option == "__none__":
        new_publish_option = ""
    region_filter = (task_config.get("region") or "").strip()

    processed_links = set()
    history_run_id = datetime.now().strftime("%Y%m%d%H%M%S")
    history_seen_item_ids: set[str] = set()
    historical_snapshots = load_price_snapshots(keyword)
    result_filename = build_result_filename(keyword)
    processed_links = load_processed_link_keys(keyword)
    if processed_links:
        print(f"LOG: 发现已存在结果集 {result_filename}，已加载 {len(processed_links)} 个历史商品用于去重。")
    else:
        print(f"LOG: 结果集 {result_filename} 当前为空，将写入新记录。")

    rotation_settings = _get_rotation_settings(task_config)
    account_items = load_state_files(rotation_settings["account_state_dir"])
    runtime_plan = resolve_account_runtime_plan(
        strategy=task_config.get("account_strategy"),
        account_state_file=task_config.get("account_state_file"),
        has_root_state_file=os.path.exists(STATE_FILE),
        available_account_files=account_items,
    )
    forced_account = runtime_plan["forced_account"]
    if runtime_plan["prefer_root_state"]:
        account_items = [STATE_FILE]
        rotation_settings["account_enabled"] = False
    elif runtime_plan["use_account_pool"]:
        rotation_settings["account_enabled"] = True
    else:
        rotation_settings["account_enabled"] = False

    account_pool = RotationPool(
        account_items, rotation_settings["account_blacklist_ttl"], "account"
    )
    proxy_pool = RotationPool(
        parse_proxy_pool(rotation_settings["proxy_pool"]),
        rotation_settings["proxy_blacklist_ttl"],
        "proxy",
    )

    selected_account: Optional[RotationItem] = None
    selected_proxy: Optional[RotationItem] = None

    def _select_account(force_new: bool = False) -> Optional[RotationItem]:
        nonlocal selected_account
        if forced_account:
            return RotationItem(value=forced_account)
        if not rotation_settings["account_enabled"]:
            if os.path.exists(STATE_FILE):
                return RotationItem(value=STATE_FILE)
            return None
        if (
            rotation_settings["account_mode"] == "per_task"
            and selected_account
            and not force_new
        ):
            return selected_account
        picked = account_pool.pick_random()
        return picked or selected_account

    def _select_proxy(force_new: bool = False) -> Optional[RotationItem]:
        nonlocal selected_proxy
        if not rotation_settings["proxy_enabled"]:
            return None
        if (
            rotation_settings["proxy_mode"] == "per_task"
            and selected_proxy
            and not force_new
        ):
            return selected_proxy
        picked = proxy_pool.pick_random()
        return picked or selected_proxy

    async def _run_scrape_attempt(state_file: str, proxy_server: Optional[str]) -> int:
        processed_item_count = 0
        stop_scraping = False

        if not os.path.exists(state_file):
            raise FileNotFoundError(f"登录状态文件不存在: {state_file}")

        snapshot_data = None
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                snapshot_data = json.load(f)
        except Exception as e:
            print(f"警告：读取登录状态文件失败，将直接按路径使用: {e}")

        async with async_playwright() as p:
            context_kwargs = _default_context_options()
            storage_state_arg = state_file
            analysis_dispatcher: Optional[ItemAnalysisDispatcher] = None
            browser = None
            context = None

            if isinstance(snapshot_data, dict):
                # 新版扩展导出的增强快照，包含环境和Header
                if any(
                    key in snapshot_data
                    for key in ("env", "headers", "page", "storage")
                ):
                    print(f"检测到增强浏览器快照，应用环境参数: {state_file}")
                    storage_state_arg = {
                        "cookies": _normalize_cookies(snapshot_data.get("cookies")),
                        "origins": snapshot_data.get("origins") or [],
                    }
                    context_kwargs.update(_build_context_overrides(snapshot_data))
                    extra_headers = _build_extra_headers(snapshot_data.get("headers"))
                    if extra_headers:
                        context_kwargs["extra_http_headers"] = extra_headers
                else:
                    normalized_state = _as_storage_state(snapshot_data)
                    if normalized_state is not None:
                        storage_state_arg = normalized_state
            elif isinstance(snapshot_data, list):
                # 裸 cookies 数组：Playwright 无法直接读取，会静默加载 0 条 cookie，
                # 最终表现为“登录态失效”。这里统一转换成 storage_state。
                normalized_state = _as_storage_state(snapshot_data)
                if normalized_state is not None:
                    print(
                        f"检测到 cookies 数组格式登录态，已转换 "
                        f"{len(normalized_state['cookies'])} 条 cookie: {state_file}"
                    )
                    storage_state_arg = normalized_state
                else:
                    print(f"警告：登录状态文件 {state_file} 未解析出任何 cookie。")

            context_kwargs = _clean_kwargs(context_kwargs)
            browser, context = await _open_browser_context(
                p,
                storage_state=storage_state_arg,
                context_kwargs=context_kwargs,
                proxy_server=proxy_server,
            )

            # 未登录时后续只会得到一次登录页重定向，提前给出可操作的提示。
            if not await context.cookies():
                print(
                    f"警告：登录状态 {state_file} 未加载到任何 cookie，"
                    "任务很可能因未登录而失败，请重新导出登录态。"
                )
            seller_profile_cache = SellerProfileCache(
                ttl_seconds=_get_seller_profile_cache_ttl(task_config)
            )
            analysis_dispatcher = ItemAnalysisDispatcher(
                concurrency=_get_ai_analysis_concurrency(task_config),
                skip_ai_analysis=SKIP_AI_ANALYSIS,
                seller_loader=lambda user_id: seller_profile_cache.get_or_load(
                    str(user_id),
                    lambda seller_key: scrape_user_profile(context, seller_key),
                ),
                image_downloader=download_all_images,
                ai_analyzer=get_ai_analysis,
                notifier=send_ntfy_notification,
                saver=save_to_jsonl,
            )

            # 增强反检测脚本（模拟真实移动设备）
            await context.add_init_script("""
                // 移除webdriver标识
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});

                // 模拟真实移动设备的navigator属性
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh', 'en-US', 'en']});

                // 添加chrome对象
                window.chrome = {runtime: {}, loadTimes: function() {}, csi: function() {}};

                // 模拟触摸支持
                Object.defineProperty(navigator, 'maxTouchPoints', {get: () => 5});

                // 覆盖permissions查询（避免暴露自动化）
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({state: Notification.permission}) :
                        originalQuery(parameters)
                );
            """)

            page = await context.new_page()

            try:
                # 步骤 0 - 模拟真实用户：先访问首页（重要的反检测措施）
                log_time("步骤 0 - 模拟真实用户访问首页...")
                await page.goto(
                    "https://www.goofish.com/",
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                log_time("[反爬] 在首页停留，模拟浏览...")
                await random_sleep(1, 2)

                # 模拟随机滚动（移动设备的触摸滚动）
                await page.evaluate("window.scrollBy(0, Math.random() * 500 + 200)")
                await random_sleep(1, 2)

                log_time("步骤 1 - 导航到搜索结果页...")
                # 使用 'q' 参数构建正确的搜索URL，并进行URL编码
                params = {"q": keyword}
                search_url = f"https://www.goofish.com/search?{urlencode(params)}"
                log_time(f"目标URL: {search_url}")

                # 先监听搜索接口响应，再执行导航，避免错过首次请求
                async with page.expect_response(
                    is_search_results_response, timeout=30000
                ) as initial_response_info:
                    await page.goto(
                        search_url, wait_until="domcontentloaded", timeout=60000
                    )
                if _is_login_url(page.url):
                    raise LoginRequiredError(
                        f"Login required: redirected to {page.url} (cookies/state likely expired)"
                    )

                # 捕获初始搜索的API数据
                initial_response = await initial_response_info.value

                # 等待页面加载出关键筛选元素，以确认已成功进入搜索结果页
                try:
                    await page.wait_for_selector("text=新发布", timeout=15000)
                except PlaywrightTimeoutError as e:
                    if _is_login_url(page.url):
                        raise LoginRequiredError(
                            f"Login required: redirected to {page.url} (cookies/state likely expired)"
                        ) from e
                    raise

                # 模拟真实用户行为：页面加载后的初始停留和浏览
                log_time("[反爬] 模拟用户查看页面...")
                await random_sleep(1, 3)

                # --- 新增：检查是否存在验证弹窗 ---
                baxia_dialog = page.locator("div.baxia-dialog-mask")
                middleware_widget = page.locator("div.J_MIDDLEWARE_FRAME_WIDGET")
                try:
                    # 等待弹窗在2秒内出现。如果出现，则执行块内代码。
                    await baxia_dialog.wait_for(state="visible", timeout=2000)
                    print(
                        "\n==================== CRITICAL BLOCK DETECTED ===================="
                    )
                    print("检测到闲鱼反爬虫验证弹窗 (baxia-dialog)，无法继续操作。")
                    print("这通常是因为操作过于频繁或被识别为机器人。")
                    print("建议：")
                    print("1. 停止脚本一段时间再试。")
                    print(
                        "2. (推荐) 在 .env 文件中设置 RUN_HEADLESS=false，以非无头模式运行，这有助于绕过检测。"
                    )
                    print(f"任务 '{keyword}' 将在此处中止。")
                    print(
                        "==================================================================="
                    )
                    raise RiskControlError("baxia-dialog")
                except PlaywrightTimeoutError:
                    # 2秒内弹窗未出现，这是正常情况，继续执行
                    pass

                # 检查是否有J_MIDDLEWARE_FRAME_WIDGET覆盖层
                try:
                    await middleware_widget.wait_for(state="visible", timeout=2000)
                    print(
                        "\n==================== CRITICAL BLOCK DETECTED ===================="
                    )
                    print(
                        "检测到闲鱼反爬虫验证弹窗 (J_MIDDLEWARE_FRAME_WIDGET)，无法继续操作。"
                    )
                    print("这通常是因为操作过于频繁或被识别为机器人。")
                    print("建议：")
                    print("1. 停止脚本一段时间再试。")
                    print("2. (推荐) 更新登录状态文件，确保登录状态有效。")
                    print("3. 降低任务执行频率，避免被识别为机器人。")
                    print(f"任务 '{keyword}' 将在此处中止。")
                    print(
                        "==================================================================="
                    )
                    raise RiskControlError("J_MIDDLEWARE_FRAME_WIDGET")
                except PlaywrightTimeoutError:
                    # 2秒内弹窗未出现，这是正常情况，继续执行
                    pass
                # --- 结束新增 ---

                try:
                    await page.click("div[class*='closeIconBg']", timeout=3000)
                    print("LOG: 已关闭广告弹窗。")
                except PlaywrightTimeoutError:
                    print("LOG: 未检测到广告弹窗。")

                final_response = None
                log_time("步骤 2 - 应用筛选条件...")
                if new_publish_option:
                    try:
                        await page.click("text=新发布")
                        await random_sleep(1, 2)  # 原来是 (1.5, 2.5)
                        async with page.expect_response(
                            is_search_results_response, timeout=20000
                        ) as response_info:
                            await page.click(f"text={new_publish_option}")
                            # --- 修改: 增加排序后的等待时间 ---
                            await random_sleep(2, 4)  # 原来是 (3, 5)
                        final_response = await response_info.value
                    except PlaywrightTimeoutError:
                        log_time(
                            f"新发布筛选 '{new_publish_option}' 请求超时，继续执行。"
                        )
                    except Exception as e:
                        print(f"LOG: 应用新发布筛选失败: {e}")

                if personal_only:
                    async with page.expect_response(
                        is_search_results_response, timeout=20000
                    ) as response_info:
                        await page.click("text=个人闲置")
                        # --- 修改: 将固定等待改为随机等待，并加长 ---
                        await random_sleep(2, 4)  # 原来是 asyncio.sleep(5)
                    final_response = await response_info.value

                if free_shipping:
                    try:
                        async with page.expect_response(
                            is_search_results_response, timeout=20000
                        ) as response_info:
                            await page.click("text=包邮")
                            await random_sleep(2, 4)
                        final_response = await response_info.value
                    except PlaywrightTimeoutError:
                        log_time("包邮筛选请求超时，继续执行。")
                    except Exception as e:
                        print(f"LOG: 应用包邮筛选失败: {e}")

                if region_filter:
                    try:
                        area_trigger = page.get_by_text("区域", exact=True)
                        if await area_trigger.count():
                            await area_trigger.first.click()
                            await random_sleep(1.5, 2)
                            popover_candidates = page.locator("div.ant-popover")
                            popover = popover_candidates.filter(
                                has=page.locator(
                                    ".areaWrap--FaZHsn8E, [class*='areaWrap']"
                                )
                            ).last
                            if not await popover.count():
                                popover = popover_candidates.filter(
                                    has=page.get_by_text("重新定位")
                                ).last
                            if not await popover.count():
                                popover = popover_candidates.filter(
                                    has=page.get_by_text("查看")
                                ).last
                            if not await popover.count():
                                print("LOG: 未找到区域弹窗，跳过区域筛选。")
                                raise PlaywrightTimeoutError("region-popover-not-found")
                            await popover.wait_for(state="visible", timeout=5000)

                            # 列表容器：第一层 children 即省/市/区三列，不再强依赖具体类名，提升鲁棒性
                            area_wrap = popover.locator(
                                ".areaWrap--FaZHsn8E, [class*='areaWrap']"
                            ).first
                            await area_wrap.wait_for(state="visible", timeout=3000)
                            columns = area_wrap.locator(":scope > div")
                            col_prov = columns.nth(0)
                            col_city = columns.nth(1)
                            col_dist = columns.nth(2)

                            region_parts = [
                                p.strip() for p in region_filter.split("/") if p.strip()
                            ]

                            async def _click_in_column(
                                column_locator, text_value: str, desc: str
                            ) -> None:
                                option = column_locator.locator(
                                    ".provItem--QAdOx8nD", has_text=text_value
                                ).first
                                if await option.count():
                                    await option.click()
                                    await random_sleep(1.5, 2)
                                    try:
                                        await option.wait_for(
                                            state="attached", timeout=1500
                                        )
                                        await option.wait_for(
                                            state="visible", timeout=1500
                                        )
                                    except PlaywrightTimeoutError:
                                        pass
                                else:
                                    print(f"LOG: 未找到{desc} '{text_value}'，跳过。")

                            if len(region_parts) >= 1:
                                await _click_in_column(
                                    col_prov, region_parts[0], "省份"
                                )
                                await random_sleep(1, 2)
                            if len(region_parts) >= 2:
                                await _click_in_column(
                                    col_city, region_parts[1], "城市"
                                )
                                await random_sleep(1, 2)
                            if len(region_parts) >= 3:
                                await _click_in_column(
                                    col_dist, region_parts[2], "区/县"
                                )
                                await random_sleep(1, 2)

                            search_btn = popover.locator(
                                "div.searchBtn--Ic6RKcAb"
                            ).first
                            if await search_btn.count():
                                try:
                                    async with page.expect_response(
                                        is_search_results_response,
                                        timeout=20000,
                                    ) as response_info:
                                        await search_btn.click()
                                        await random_sleep(2, 3)
                                    final_response = await response_info.value
                                except PlaywrightTimeoutError:
                                    log_time("区域筛选提交超时，继续执行。")
                            else:
                                print(
                                    "LOG: 未找到区域弹窗的“查看XX件宝贝”按钮，跳过提交。"
                                )
                        else:
                            print("LOG: 未找到区域筛选触发器。")
                    except PlaywrightTimeoutError:
                        log_time(f"区域筛选 '{region_filter}' 请求超时，继续执行。")
                    except Exception as e:
                        print(f"LOG: 应用区域筛选 '{region_filter}' 失败: {e}")

                if min_price or max_price:
                    price_container = page.locator(
                        'div[class*="search-price-input-container"]'
                    ).first
                    if await price_container.is_visible():
                        if min_price:
                            await price_container.get_by_placeholder("¥").first.fill(
                                min_price
                            )
                            # --- 修改: 将固定等待改为随机等待 ---
                            await random_sleep(1, 2.5)  # 原来是 asyncio.sleep(5)
                        if max_price:
                            await (
                                price_container.get_by_placeholder("¥")
                                .nth(1)
                                .fill(max_price)
                            )
                            # --- 修改: 将固定等待改为随机等待 ---
                            await random_sleep(1, 2.5)  # 原来是 asyncio.sleep(5)

                        async with page.expect_response(
                            is_search_results_response, timeout=20000
                        ) as response_info:
                            await page.keyboard.press("Tab")
                            # --- 修改: 增加确认价格后的等待时间 ---
                            await random_sleep(2, 4)  # 原来是 asyncio.sleep(5)
                        final_response = await response_info.value
                    else:
                        print("LOG: 警告 - 未找到价格输入容器。")

                log_time("所有筛选已完成，开始处理商品列表...")

                current_response = (
                    final_response
                    if final_response and final_response.ok
                    else initial_response
                )
                for page_num in range(1, max_pages + 1):
                    if stop_scraping:
                        break
                    log_time(f"开始处理第 {page_num}/{max_pages} 页 ...")

                    if page_num > 1:
                        page_advance_result = await advance_search_page(
                            page=page,
                            page_num=page_num,
                        )
                        if not page_advance_result.advanced:
                            break
                        current_response = page_advance_result.response

                    if not (current_response and current_response.ok):
                        log_time(f"第 {page_num} 页响应无效，跳过。")
                        continue

                    basic_items = await _parse_search_results_json(
                        await current_response.json(), f"第 {page_num} 页"
                    )
                    if not basic_items:
                        break
                    historical_snapshots.extend(
                        record_market_snapshots(
                            keyword=keyword,
                            task_name=task_config.get("task_name", "Untitled Task"),
                            items=basic_items,
                            run_id=history_run_id,
                            snapshot_time=datetime.now().isoformat(),
                            seen_item_ids=history_seen_item_ids,
                        )
                    )

                    total_items_on_page = len(basic_items)
                    for i, item_data in enumerate(basic_items, 1):
                        if debug_limit > 0 and processed_item_count >= debug_limit:
                            log_time(
                                f"已达到调试上限 ({debug_limit})，停止获取新商品。"
                            )
                            stop_scraping = True
                            break

                        unique_key = get_link_unique_key(item_data["商品链接"])
                        if unique_key in processed_links:
                            log_time(
                                f"[页内进度 {i}/{total_items_on_page}] 商品 '{item_data['商品标题'][:20]}...' 已存在，跳过。"
                            )
                            continue

                        log_time(
                            f"[页内进度 {i}/{total_items_on_page}] 发现新商品，获取详情: {item_data['商品标题'][:30]}..."
                        )
                        # --- 修改: 访问详情页前的等待时间，模拟用户在列表页上看了一会儿 ---
                        await random_sleep(2, 4)  # 原来是 (2, 4)

                        detail_page = await context.new_page()
                        try:
                            async with detail_page.expect_response(
                                lambda r: DETAIL_API_URL_PATTERN in r.url, timeout=25000
                            ) as detail_info:
                                await detail_page.goto(
                                    item_data["商品链接"],
                                    wait_until="domcontentloaded",
                                    timeout=25000,
                                )

                            detail_response = await detail_info.value
                            if detail_response.ok:
                                detail_json = await detail_response.json()

                                ret_string = str(
                                    await safe_get(detail_json, "ret", default=[])
                                )
                                if "FAIL_SYS_USER_VALIDATE" in ret_string:
                                    print(
                                        "\n==================== CRITICAL BLOCK DETECTED ===================="
                                    )
                                    print(
                                        "检测到闲鱼反爬虫验证 (FAIL_SYS_USER_VALIDATE)，程序将终止。"
                                    )
                                    long_sleep_duration = random.randint(3, 60)
                                    print(
                                        f"为避免账户风险，将执行一次长时间休眠 ({long_sleep_duration} 秒) 后再退出..."
                                    )
                                    await asyncio.sleep(long_sleep_duration)
                                    print("长时间休眠结束，现在将安全退出。")
                                    print(
                                        "==================================================================="
                                    )
                                    raise RiskControlError("FAIL_SYS_USER_VALIDATE")

                                # 解析商品详情数据并更新 item_data
                                item_do = await safe_get(
                                    detail_json, "data", "itemDO", default={}
                                )
                                seller_do = await safe_get(
                                    detail_json, "data", "sellerDO", default={}
                                )

                                reg_days_raw = await safe_get(
                                    seller_do, "userRegDay", default=0
                                )
                                registration_duration_text = format_registration_days(
                                    reg_days_raw
                                )

                                # --- START: 新增代码块 ---

                                # 1. 提取卖家的芝麻信用信息
                                zhima_credit_text = await safe_get(
                                    seller_do, "zhimaLevelInfo", "levelName"
                                )

                                # 2. 提取该商品的完整图片列表
                                image_infos = await safe_get(
                                    item_do, "imageInfos", default=[]
                                )
                                if image_infos:
                                    # 使用列表推导式获取所有有效的图片URL
                                    all_image_urls = [
                                        img.get("url")
                                        for img in image_infos
                                        if img.get("url")
                                    ]
                                    if all_image_urls:
                                        # 用新的字段存储图片列表，替换掉旧的单个链接
                                        item_data["商品图片列表"] = all_image_urls
                                        # (可选) 仍然保留主图链接，以防万一
                                        item_data["商品主图链接"] = all_image_urls[0]

                                # --- END: 新增代码块 ---
                                item_data["“想要”人数"] = await safe_get(
                                    item_do,
                                    "wantCnt",
                                    default=item_data.get("“想要”人数", "NaN"),
                                )
                                item_data["浏览量"] = await safe_get(
                                    item_do, "browseCnt", default="-"
                                )
                                # ...[此处可添加更多从详情页解析出的商品信息]...

                                user_id = await safe_get(seller_do, "sellerId")

                                # 构建基础记录
                                final_record = {
                                    "爬取时间": datetime.now().isoformat(),
                                    "搜索关键字": keyword,
                                    "任务名称": task_config.get(
                                        "task_name", "Untitled Task"
                                    ),
                                    "商品信息": item_data,
                                    "卖家信息": {},
                                }
                                price_reference = build_market_reference(
                                    keyword=keyword,
                                    item=item_data,
                                    current_market_items=basic_items,
                                    historical_snapshots=historical_snapshots,
                                )
                                final_record["价格参考"] = price_reference
                                final_record["price_insight"] = price_reference.get(
                                    "本商品价格位置", {}
                                )

                                analysis_dispatcher.submit(
                                    ItemAnalysisJob(
                                        keyword=keyword,
                                        task_name=task_config.get(
                                            "task_name", "Untitled Task"
                                        ),
                                        decision_mode=decision_mode,
                                        analyze_images=analyze_images,
                                        prompt_text=ai_prompt_text,
                                        keyword_rules=tuple(keyword_rules or []),
                                        final_record=final_record,
                                        seller_id=str(user_id) if user_id else None,
                                        zhima_credit_text=zhima_credit_text,
                                        registration_duration_text=registration_duration_text,
                                    )
                                )

                                processed_links.add(unique_key)
                                processed_item_count += 1
                                log_time(
                                    f"商品已提交后台分析。累计处理 {processed_item_count} 个新商品。"
                                )

                                # --- 修改: 增加单个商品处理后的主要延迟 ---
                                log_time(
                                    "[反爬] 执行一次主要的随机延迟以模拟用户浏览间隔..."
                                )
                                await random_sleep(5, 10)
                            else:
                                print(
                                    f"   错误: 获取商品详情API响应失败，状态码: {detail_response.status}"
                                )
                                if AI_DEBUG_MODE:
                                    print(
                                        f"--- [DETAIL DEBUG] FAILED RESPONSE from {item_data['商品链接']} ---"
                                    )
                                    try:
                                        print(await detail_response.text())
                                    except Exception as e:
                                        print(f"无法读取响应内容: {e}")
                                    print(
                                        "----------------------------------------------------"
                                    )

                        except PlaywrightTimeoutError:
                            print(f"   错误: 访问商品详情页或等待API响应超时。")
                        except Exception as e:
                            if _is_abort_signal(e):
                                # 风控/登录失效必须交给 scrape_xianyu 的尝试循环处理，
                                # 不能在这里被降级成“继续下一个商品”。
                                raise
                            print(f"   错误: 处理商品详情时发生未知错误: {e}")
                        finally:
                            await detail_page.close()
                            # --- 修改: 增加关闭页面后的短暂整理时间 ---
                            await random_sleep(2, 4)  # 原来是 (1, 2.5)

                    # --- 新增: 在处理完一页所有商品后，翻页前，增加一个更长的“休息”时间 ---
                    if not stop_scraping and page_num < max_pages:
                        print(
                            f"--- 第 {page_num} 页处理完毕，准备翻页。执行一次页面间的长时休息... ---"
                        )
                        await random_sleep(10, 15)

            except PlaywrightTimeoutError as e:
                if _is_login_url(page.url):
                    raise LoginRequiredError(
                        f"Login required: redirected to {page.url} (cookies/state likely expired)"
                    ) from e
                print(f"\n操作超时错误: 页面元素或网络响应未在规定时间内出现。\n{e}")
                raise
            except asyncio.CancelledError:
                log_time("收到取消信号，正在终止当前爬虫任务...")
                raise
            except Exception as e:
                if type(e).__name__ == "TargetClosedError":
                    log_time("浏览器已关闭，忽略后续异常（可能是任务被停止）。")
                    return processed_item_count
                if "passport.goofish.com" in str(e):
                    raise LoginRequiredError(
                        f"Login required: redirected to passport flow ({e})"
                    ) from e
                print(f"\n爬取过程中发生未知错误: {e}")
                raise
            finally:
                if analysis_dispatcher is not None:
                    log_time("等待后台分析任务完成...")
                    await analysis_dispatcher.join()
                log_time("任务执行完毕，浏览器将在5秒后自动关闭...")
                await asyncio.sleep(5)
                if debug_limit:
                    # 非交互环境（cron / 后台任务 / 重定向 stdin）读不到输入，
                    # input() 会抛 EOFError：既会把一次成功的尝试误判为失败并触发重试，
                    # 也会跳过后面的 browser.close() 导致浏览器进程泄漏。
                    try:
                        input("按回车键关闭浏览器...")
                    except (EOFError, KeyboardInterrupt):
                        print("（非交互环境，跳过等待回车）")
                if browser is not None:
                    await browser.close()
                elif context is not None:
                    # 持久化 profile 场景没有单独的 browser 对象，
                    # 关闭 context 即等同于关闭浏览器。
                    await context.close()

        return processed_item_count

    processed_item_count = 0
    attempt_limit = max(
        rotation_settings["account_retry_limit"],
        rotation_settings["proxy_retry_limit"],
        1,
    )
    last_error = ""
    last_state_path: Optional[str] = None

    # If this task is already in a paused state, skip immediately.
    task_name_for_guard = task_config.get("task_name", "未命名任务")
    pause_cookie_path = None
    if (
        isinstance(task_config.get("account_state_file"), str)
        and task_config.get("account_state_file").strip()
    ):
        pause_cookie_path = task_config.get("account_state_file").strip()
    elif os.path.exists(STATE_FILE):
        pause_cookie_path = STATE_FILE

    decision = FAILURE_GUARD.should_skip_start(
        task_name_for_guard, cookie_path=pause_cookie_path
    )
    if decision.skip:
        print(
            f"[FailureGuard] 任务 '{task_name_for_guard}' 已暂停重试 (连续失败 {decision.consecutive_failures}/{FAILURE_GUARD.threshold})"
        )
        if decision.should_notify:
            try:
                await send_ntfy_notification(
                    {
                        "商品标题": f"[任务暂停] {task_name_for_guard}",
                        "当前售价": "N/A",
                        "商品链接": "#",
                    },
                    "任务处于暂停状态，将跳过执行。\n"
                    f"原因: {decision.reason}\n"
                    f"连续失败: {decision.consecutive_failures}/{FAILURE_GUARD.threshold}\n"
                    f"暂停到: {decision.paused_until.strftime('%Y-%m-%d %H:%M:%S') if decision.paused_until else 'N/A'}\n"
                    "修复方法: 更新登录态/cookies文件后会自动恢复。",
                )
            except Exception as e:
                print(f"发送任务暂停通知失败: {e}")

        cleanup_task_images(task_config.get("task_name", "default"))
        return 0

    for attempt in range(1, attempt_limit + 1):
        if attempt == 1:
            selected_account = _select_account()
            selected_proxy = _select_proxy()
        else:
            if (
                rotation_settings["account_enabled"]
                and rotation_settings["account_mode"] == "on_failure"
            ):
                account_pool.mark_bad(selected_account, last_error)
                selected_account = _select_account(force_new=True)
            if (
                rotation_settings["proxy_enabled"]
                and rotation_settings["proxy_mode"] == "on_failure"
            ):
                proxy_pool.mark_bad(selected_proxy, last_error)
                selected_proxy = _select_proxy(force_new=True)

        if rotation_settings["account_enabled"] and not selected_account:
            last_error = "未找到可用的登录状态文件，无法继续执行任务。"
            print(last_error)
            break
        if not rotation_settings["account_enabled"] and not selected_account:
            last_error = "未找到可用的登录状态文件，无法继续执行任务。"
            print(last_error)
            break
        if rotation_settings["proxy_enabled"] and not selected_proxy:
            last_error = "未找到可用的代理地址，无法继续执行任务。"
            print(last_error)
            break

        state_path = selected_account.value if selected_account else STATE_FILE
        last_state_path = state_path
        proxy_server = selected_proxy.value if selected_proxy else None
        if rotation_settings["account_enabled"]:
            print(f"账号轮换：使用登录状态 {state_path}")
        if rotation_settings["proxy_enabled"] and proxy_server:
            print(f"IP 轮换：使用代理 {proxy_server}")

        try:
            processed_item_count += await _run_scrape_attempt(state_path, proxy_server)
            last_error = ""
            FAILURE_GUARD.record_success(task_name_for_guard)
            break
        except LoginRequiredError as e:
            last_error = str(e)
            print(f"检测到登录失效/重定向: {e}")
            break
        except RiskControlError as e:
            last_error = str(e)
            print(f"检测到风控或验证触发: {e}")
            # 风控验证通常不是简单轮换能解决的，避免无意义重试。
            break
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            print(f"本次尝试失败: {last_error}")
            if attempt < attempt_limit:
                print("将尝试轮换账号/IP 后重试...")

    if last_error:
        await _notify_task_failure(task_config, last_error, cookie_path=last_state_path)

    # 清理任务图片目录
    cleanup_task_images(task_config.get("task_name", "default"))

    return processed_item_count
