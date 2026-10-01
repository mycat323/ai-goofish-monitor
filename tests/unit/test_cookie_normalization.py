"""登录态导出格式的兼容性测试。

背景：`state/*.json` 可能来自两类导出工具，字段规范并不一致：

- 项目自带的 chrome-extension：增强快照 dict，cookies 用 `expires` +
  `sameSite: Strict|Lax|None`（Playwright 原生格式）。
- Cookie-Editor / EditThisCookie 等通用扩展：裸 cookies 数组，用
  `expirationDate` + `sameSite: no_restriction|lax|strict|unspecified`。

Playwright 对第二种格式不会报错，而是静默加载 0 条 cookie，最终表现为
"登录态失效 / cookies(expires)"，非常难排查。这里锁定转换行为。
"""

from src.scraper import _as_storage_state, _normalize_cookies


def _cookie_editor_export() -> list:
    """Cookie-Editor 风格的导出（裸数组 + expirationDate + no_restriction）。"""
    return [
        {
            "domain": ".goofish.com",
            "expirationDate": 1790830884.34,
            "hostOnly": False,
            "httpOnly": False,
            "name": "_m_h5_tk_enc",
            "path": "/",
            "sameSite": "no_restriction",
            "secure": True,
            "session": False,
            "storeId": "0",
            "value": "abc",
        },
        {
            "domain": ".goofish.com",
            "httpOnly": True,
            "name": "cookie2",
            "path": "/",
            "sameSite": "unspecified",
            "secure": True,
            "session": True,
            "value": "session-value",
        },
        {
            "domain": ".goofish.com",
            "name": "no_same_site",
            "path": "/",
            "value": "v",
        },
        {
            "domain": ".goofish.com",
            "expirationDate": 1790830884.34,
            "name": "unb",
            "path": "/",
            "sameSite": "lax",
            "secure": True,
            "value": "12345",
        },
    ]


def test_normalize_cookies_maps_extension_vocabulary_to_playwright():
    cookies = _normalize_cookies(_cookie_editor_export())

    assert len(cookies) == 4

    by_name = {c["name"]: c for c in cookies}

    # sameSite 必须翻译成 Playwright 只接受的 Strict / Lax / None
    assert by_name["_m_h5_tk_enc"]["sameSite"] == "None"
    assert by_name["unb"]["sameSite"] == "Lax"
    # Chrome 扩展把 unspecified 视为 Lax
    assert by_name["cookie2"]["sameSite"] == "Lax"
    # 完全缺省 sameSite 的 cookie 不带该字段，由 Playwright 取默认值
    assert "sameSite" not in by_name["no_same_site"]

    # expirationDate 必须改名为 expires
    assert by_name["_m_h5_tk_enc"]["expires"] == 1790830884.34
    assert all("expirationDate" not in c for c in cookies)

    # 会话 cookie 没有 expires
    assert "expires" not in by_name["cookie2"]

    # 扩展私有的字段不应泄漏给 Playwright
    allowed = {"name", "value", "domain", "path", "secure", "httpOnly", "expires", "sameSite"}
    assert all(set(c) <= allowed for c in cookies)


def test_as_storage_state_wraps_bare_cookie_array():
    """裸数组必须被包成 storage_state，否则 Playwright 会静默加载 0 条 cookie。"""
    state = _as_storage_state(_cookie_editor_export())

    assert state is not None
    assert len(state["cookies"]) == 4
    assert state["origins"] == []


def test_as_storage_state_accepts_standard_and_snapshot_shapes():
    cookies = _cookie_editor_export()

    standard = _as_storage_state({"cookies": cookies, "origins": [{"origin": "https://www.goofish.com"}]})
    assert standard is not None
    assert len(standard["cookies"]) == 4
    assert standard["origins"] == [{"origin": "https://www.goofish.com"}]

    # 增强快照只取 cookies，忽略 env/headers 等字段
    snapshot = _as_storage_state({"cookies": cookies, "env": {"navigator": {}}, "headers": {}})
    assert snapshot is not None
    assert len(snapshot["cookies"]) == 4


def test_as_storage_state_returns_none_when_nothing_usable():
    # 无法识别的结构，调用方应回退到原始文件路径
    assert _as_storage_state(None) is None
    assert _as_storage_state("state.json") is None
    assert _as_storage_state({}) is None
    assert _as_storage_state([]) is None
    # cookies 存在但全是无效条目
    assert _as_storage_state([{"value": "no-name"}]) is None


def test_normalize_cookies_skips_entries_without_name_or_domain():
    cookies = _normalize_cookies(
        [
            {"name": "keep", "domain": ".goofish.com", "value": "v"},
            {"name": "", "domain": ".goofish.com", "value": "no-name"},
            {"name": "no-domain", "value": "v"},
            "not-a-dict",
            None,
        ]
    )

    assert [c["name"] for c in cookies] == ["keep"]
