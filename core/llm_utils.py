"""LLM response helpers shared by Anthropic-compatible providers."""
import os
from typing import Any, Dict, Iterable, List


def llm_extra_kwargs() -> Dict[str, Any]:
    """Provider-specific request options for Anthropic-compatible APIs.

    DeepSeek v4 等推理模型默认返回 thinking 块：既消耗输出 token，也会让
    短 JSON 调用在 token 上限内拿不到正文。设置 ECHOMIND_DISABLE_THINKING=true
    可显式请求纯文本输出；默认关闭该选项，保持官方 Anthropic 行为不变。
    """
    if os.getenv("ECHOMIND_DISABLE_THINKING", "false").lower() in {"1", "true", "yes"}:
        return {"extra_body": {"thinking": {"type": "disabled"}}}
    return {}


def extract_text_content(content: Iterable[Any], separator: str = "\n") -> str:
    """Return text blocks from Anthropic-style response content."""
    texts: List[str] = []
    for block in content or []:
        if isinstance(block, str):
            texts.append(block)
            continue

        block_type = getattr(block, "type", None)
        text = getattr(block, "text", None)
        if isinstance(block, dict):
            block_type = block.get("type", block_type)
            text = block.get("text", text)

        if isinstance(text, str) and (block_type in (None, "text")):
            texts.append(text)

    return separator.join(t for t in texts if t)
