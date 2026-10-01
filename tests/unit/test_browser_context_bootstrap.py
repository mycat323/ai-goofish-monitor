"""浏览器 context 创建策略的测试。

背景：项目原本每次都 `new_context()`，得到一个全新的空浏览器配置
（无历史/缓存/本地存储，设备指纹每次都不同），再把 storage_state 注入进去。
对闲鱼的风控来说，这等于「陌生新设备带着老会话访问」，每次运行都触发一次该
信号。configured `BROWSER_USER_DATA_DIR` 后改用 `launch_persistent_context`，
设备指纹跨运行稳定、登录态由 profile 持有并自然刷新。
"""

from __future__ import annotations

import json
import types

import pytest

from src import scraper
from src.scraper import (
    _open_browser_context,
    _persistent_profile_dir,
    _storage_state_cookies,
)


class _FakeContext:
    def __init__(self, cookies=None):
        self._cookies = list(cookies or [])
        self.added = []
        self.closed = False

    async def cookies(self):
        return list(self._cookies)

    async def add_cookies(self, cookies):
        self.added.extend(cookies)
        self._cookies.extend(cookies)

    async def close(self):
        self.closed = True


class _FakeBrowser:
    def __init__(self, context):
        self._context = context
        self.new_context_kwargs = []
        self.closed = False

    async def new_context(self, **kwargs):
        self.new_context_kwargs.append(kwargs)
        return self._context

    async def close(self):
        self.closed = True


class _FakeChromium:
    def __init__(self, context):
        self._context = context
        self.launch_kwargs = []
        self.persistent_calls = []

    async def launch(self, **kwargs):
        self.launch_kwargs.append(kwargs)
        return _FakeBrowser(self._context)

    async def launch_persistent_context(self, user_data_dir, **kwargs):
        self.persistent_calls.append((user_data_dir, kwargs))
        return self._context


def _playwright_with(context):
    chromium = _FakeChromium(context)
    return types.SimpleNamespace(chromium=chromium), chromium


@pytest.fixture(autouse=True)
def _stable_channel(monkeypatch):
    monkeypatch.setattr(scraper, "_resolve_browser_channel", lambda: "chromium")


COOKIES = [
    {"name": "_m_h5_tk", "value": "abc", "domain": ".goofish.com", "path": "/"},
    {"name": "unb", "value": "123", "domain": ".goofish.com", "path": "/"},
]


def test_default_path_uses_new_context_and_injects_storage_state(monkeypatch):
    """未配置 profile 目录时必须保持旧行为，保证零回归。"""
    monkeypatch.setattr(scraper, "BROWSER_USER_DATA_DIR", "")

    ctx = _FakeContext()
    pw, chromium = _playwright_with(ctx)
    state = {"cookies": COOKIES, "origins": []}

    import asyncio

    browser, context = asyncio.run(
        _open_browser_context(pw, storage_state=state, context_kwargs={"locale": "zh-CN"})
    )

    assert chromium.launch_kwargs, "旧路径应调用 launch()"
    assert not chromium.persistent_calls, "旧路径不应使用持久化 profile"
    assert context is ctx
    assert browser is not None
    assert browser.new_context_kwargs[0]["storage_state"] == state


def test_persistent_profile_is_used_when_configured(monkeypatch, tmp_path):
    monkeypatch.setattr(scraper, "BROWSER_USER_DATA_DIR", str(tmp_path / "profile"))

    ctx = _FakeContext()
    pw, chromium = _playwright_with(ctx)

    import asyncio

    browser, context = asyncio.run(
        _open_browser_context(
            pw, storage_state={"cookies": COOKIES, "origins": []}, context_kwargs={}
        )
    )

    assert browser is None, "持久化 profile 没有独立的 browser 对象"
    assert context is ctx
    assert len(chromium.persistent_calls) == 1
    profile_dir, kwargs = chromium.persistent_calls[0]
    assert profile_dir == str(tmp_path / "profile")
    assert (tmp_path / "profile").is_dir(), "目录应被自动创建"
    assert kwargs["headless"] in (True, False)
    assert "args" in kwargs


def test_persistent_profile_bootstraps_cookies_only_when_empty(monkeypatch, tmp_path):
    """首次使用（profile 无 cookie）用登录态文件引导一次。"""
    monkeypatch.setattr(scraper, "BROWSER_USER_DATA_DIR", str(tmp_path / "profile"))

    ctx = _FakeContext(cookies=[])
    pw, _ = _playwright_with(ctx)

    import asyncio

    asyncio.run(
        _open_browser_context(
            pw, storage_state={"cookies": COOKIES, "origins": []}, context_kwargs={}
        )
    )

    assert [c["name"] for c in ctx.added] == ["_m_h5_tk", "unb"]


def test_persistent_profile_does_not_overwrite_existing_cookies(monkeypatch, tmp_path):
    """profile 里已有的 cookie 必须保留，不能被导出文件覆盖。

    profile 中的 token（如 _m_h5_tk）会被闲鱼自然刷新，用导出文件覆盖会把
    它打回过期状态——这正是之前 "登录态失效" 的成因之一。
    """
    monkeypatch.setattr(scraper, "BROWSER_USER_DATA_DIR", str(tmp_path / "profile"))

    stale = {"name": "_m_h5_tk", "value": "stale", "domain": ".goofish.com", "path": "/"}
    fresh = {"name": "_m_h5_tk", "value": "refreshed-by-site", "domain": ".goofish.com", "path": "/"}
    ctx = _FakeContext(cookies=[fresh])
    pw, _ = _playwright_with(ctx)

    import asyncio

    asyncio.run(
        _open_browser_context(
            pw, storage_state={"cookies": [stale], "origins": []}, context_kwargs={}
        )
    )

    assert ctx.added == [], "同名/域/路径的 cookie 不应被覆盖"
    assert ctx._cookies[0]["value"] == "refreshed-by-site"


def test_persistent_profile_restores_dropped_session_cookies(monkeypatch, tmp_path):
    """Chrome 不持久化 session cookie，重开浏览器后必须从登录态文件补回。

    cookie2 / _tb_token_ / csg 等属于鉴权关键 cookie，丢失会直接掉登录。
    """
    monkeypatch.setattr(scraper, "BROWSER_USER_DATA_DIR", str(tmp_path / "profile"))

    kept = {"name": "unb", "value": "123", "domain": ".goofish.com", "path": "/"}
    dropped = {"name": "cookie2", "value": "session", "domain": ".goofish.com", "path": "/"}
    ctx = _FakeContext(cookies=[kept])
    pw, _ = _playwright_with(ctx)

    import asyncio

    asyncio.run(
        _open_browser_context(
            pw, storage_state={"cookies": [kept, dropped], "origins": []}, context_kwargs={}
        )
    )

    assert [c["name"] for c in ctx.added] == ["cookie2"]


def test_storage_state_cookies_accepts_dict_path_and_junk(tmp_path):
    assert _storage_state_cookies({"cookies": COOKIES}) == COOKIES
    assert _storage_state_cookies({"origins": []}) == []
    assert _storage_state_cookies(None) == []

    # 裸数组（Cookie-Editor 导出）也要能读出来
    raw = tmp_path / "state.json"
    raw.write_text(json.dumps(COOKIES), encoding="utf-8")
    assert [c["name"] for c in _storage_state_cookies(str(raw))] == ["_m_h5_tk", "unb"]

    # 破损文件不应抛异常
    broken = tmp_path / "broken.json"
    broken.write_text("{not json", encoding="utf-8")
    assert _storage_state_cookies(str(broken)) == []
    assert _storage_state_cookies(str(tmp_path / "missing.json")) == []


def test_persistent_profile_dir_defaults_to_disabled(monkeypatch):
    monkeypatch.setattr(scraper, "BROWSER_USER_DATA_DIR", "")
    assert _persistent_profile_dir() is None

    monkeypatch.setattr(scraper, "BROWSER_USER_DATA_DIR", "state/browser-profile")
    assert _persistent_profile_dir() == "state/browser-profile"
