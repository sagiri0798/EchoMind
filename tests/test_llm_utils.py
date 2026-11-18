"""LLM 请求选项回归测试。"""
from core.llm_utils import llm_extra_kwargs


def test_thinking_is_disabled_only_when_configured(monkeypatch):
    monkeypatch.delenv("ECHOMIND_DISABLE_THINKING", raising=False)
    assert llm_extra_kwargs() == {}

    monkeypatch.setenv("ECHOMIND_DISABLE_THINKING", "true")
    assert llm_extra_kwargs() == {"extra_body": {"thinking": {"type": "disabled"}}}
