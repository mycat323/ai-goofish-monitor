"""分析标准（criteria）生成的质量守卫测试。

背景（真实事故）：`prompts/macbook_pro_m1_64g_criteria.txt` 只有 3253 字节、
结尾停在句子中间（"…但图片为仓库多台、"），「第二部分：详细分析指南」整段丢失。

根因是 `_request_generated_text` 里 `max_output_tokens=800`：参考范例
`macbook_criteria.txt` 本身就有 4774 字节，远超 800 token，所以生成**必然被截断**。

后果不是报错，而是静默降级：AI 只剩「画像优先原则」可用，没有详细的评分纬度，
于是看到商家/成色差就直接否决——判定明显偏严。

审计发现至少两个文件受此影响：`macpro_m1_64g_criteria.txt`（截断）、
`apple_watch_s10_criteria.txt`（41 字节空壳）。
"""

from __future__ import annotations

import asyncio
import io
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from src import prompt_utils
from src.prompt_utils import (
    CRITERIA_MAX_OUTPUT_TOKENS,
    _warn_if_truncated,
    criteria_looks_complete,
)

PROMPTS_DIR = Path(prompt_utils.__file__).resolve().parent.parent / "prompts"

REAL_TRUNCATED_SAMPLE = (
    "### **第一部分：核心分析原则**\n\n"
    "3. **图片至上原则**: 必须以图片信息为最终裁决依据。例如：文本写“64G”，"
    "但图片系统信息显示“32GB”，则内存项 FAIL；文本写“个人自用”，但图片为仓库多台、"
)


def test_output_token_cap_is_large_enough_for_a_full_criteria_document():
    """800 装不下一份完整的分析标准——这正是截断的根因。"""
    assert CRITERIA_MAX_OUTPUT_TOKENS >= 2000, (
        "上限过小会让生成结果必然被截断（原值为 800）"
    )


def test_request_uses_the_raised_token_cap(monkeypatch):
    """行为验证：真正传给 AI 的 max_output_tokens 必须是新上限。"""
    captured = {}

    class _FakeClient:
        async def _call_ai(self, messages, **kwargs):
            captured.update(kwargs)
            return "### 第一部分\n\n### 第二部分：详细分析指南\n\n1. 型号芯片。"

    text = asyncio.run(prompt_utils._request_generated_text(_FakeClient(), "prompt"))

    assert captured["max_output_tokens"] == CRITERIA_MAX_OUTPUT_TOKENS
    assert text.startswith("### 第一部分")


@pytest.mark.parametrize(
    "text,expected",
    [
        ("### 第一部分\n\n### 第二部分：详细分析指南\n\n1. 型号芯片。", True),
        ("### 第一部分\n\n### 第二部分\n\n1. 型号芯片。\n7. 卖家信用。", True),
        (REAL_TRUNCATED_SAMPLE, False),          # 不含第二部分
        ("### 第一部分\n\n1. 画像优先原则。", False),  # 缺第二部分
        ("### 第一部分\n### 第二部分\n结尾没有标点", False),
        ("", False),
        ("   ", False),
    ],
)
def test_criteria_completeness_detection(text, expected):
    assert criteria_looks_complete(text) is expected


def test_truncation_warning_names_the_missing_section():
    buf = io.StringIO()
    with redirect_stdout(buf):
        _warn_if_truncated(REAL_TRUNCATED_SAMPLE)

    message = buf.getvalue()
    assert "第二部分" in message
    assert "警告" in message


def test_complete_document_produces_no_warning():
    buf = io.StringIO()
    with redirect_stdout(buf):
        _warn_if_truncated("### 第一部分\n\n### 第二部分：详细分析指南\n\n1. 型号芯片。")

    assert buf.getvalue() == ""


def test_empty_generation_produces_a_warning():
    buf = io.StringIO()
    with redirect_stdout(buf):
        _warn_if_truncated("   ")

    assert "为空" in buf.getvalue()


#: 按任务生成的 criteria 属于运行时数据（prompts/ 被 .gitignore 忽略），
#: 全新克隆里并不存在，因此相关断言必须允许缺席。
RUNTIME_CRITERIA = PROMPTS_DIR / "macbook_pro_m1_64g_criteria.txt"


def _runtime_criteria_text() -> str:
    if not RUNTIME_CRITERIA.exists():
        pytest.skip(f"{RUNTIME_CRITERIA.name} 是按任务生成的运行时文件，当前工作区没有")
    return RUNTIME_CRITERIA.read_text(encoding="utf-8")


def test_reference_criteria_is_complete():
    """被跟踪的参考范例必须完整（它是生成时的模板）。"""
    path = PROMPTS_DIR / "macbook_criteria.txt"
    text = path.read_text(encoding="utf-8")

    assert criteria_looks_complete(text), f"{path.name} 似乎被截断"


def test_macbook_pro_criteria_file_is_complete():
    """回归守卫：当前任务的 criteria 必须完整（它曾被截断过）。"""
    text = _runtime_criteria_text()

    assert criteria_looks_complete(text), f"{RUNTIME_CRITERIA.name} 似乎被截断"
    assert "第二部分" in text


def test_criteria_relaxes_seller_type_and_condition():
    """按需求：卖家身份与成色只能标记、不能否决。"""
    text = _runtime_criteria_text()

    assert "标记而非否决" in text
    for token in ("FLAGGED_SELLER_TYPE", "FLAGGED_HEAVY", "FLAGGED_REPAIR"):
        assert token in text, f"缺少标记项 {token}"

    # 必须明确禁止因卖家身份/成色否决
    assert "绝不能因为" in text
    # 硬性原则仍需保留（芯片/内存/交易意图）
    for token in ("芯片", "内存", "交易意图", "可售真实性"):
        assert token in text
