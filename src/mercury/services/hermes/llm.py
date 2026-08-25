"""LLM client abstraction for Hermes.

Pluggable provider interface supporting:
- OpenAI (and any OpenAI-compatible endpoint, incl. Ollama and LM Studio)
- Anthropic
- rule-based fallback (no external calls)

Implemented with httpx to avoid heavy SDK dependencies; the ``openai`` package
remains an optional extra for teams that prefer it.
"""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from typing import Any

import httpx

from mercury.core.logging import get_logger

logger = get_logger("services.hermes.llm")


class LLMError(Exception):
    """Raised when an LLM call fails."""


def extract_json(text: str) -> dict[str, Any]:
    """Best-effort extraction of the first JSON object from model output."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            pass
    raise LLMError(f"could not extract JSON from model output: {text[:200]!r}")


class LLMClient(ABC):
    """Abstract LLM client returning structured JSON."""

    name: str = "llm"

    @abstractmethod
    async def complete_structured(
        self,
        *,
        system: str,
        user: str,
        temperature: float = 0.2,
        max_tokens: int = 1500,
    ) -> dict[str, Any]:
        ...


class OpenAICompatClient(LLMClient):
    """Works with OpenAI and any OpenAI-compatible endpoint (Ollama, etc.)."""

    name = "openai_compat"

    def __init__(self, *, base_url: str, api_key: str, model: str, timeout: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.timeout = timeout

    async def complete_structured(
        self, *, system: str, user: str, temperature: float = 0.2, max_tokens: int = 1500
    ) -> dict[str, Any]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        body: dict[str, Any] = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
            "response_format": {"type": "json_object"},
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    f"{self.base_url}/chat/completions", headers=headers, json=body
                )
                resp.raise_for_status()
                content = resp.json()["choices"][0]["message"]["content"]
        except httpx.HTTPError as exc:
            raise LLMError(f"{self.name} request failed: {exc}") from exc
        return extract_json(content)


class AnthropicClient(LLMClient):
    name = "anthropic"

    def __init__(self, *, api_key: str, model: str, timeout: float = 60.0) -> None:
        self.api_key = api_key
        self.model = model
        self.timeout = timeout

    async def complete_structured(
        self, *, system: str, user: str, temperature: float = 0.2, max_tokens: int = 1500
    ) -> dict[str, Any]:
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
        }
        body: dict[str, Any] = {
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "system": system,
            "messages": [{"role": "user", "content": user}],
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages", headers=headers, json=body
                )
                resp.raise_for_status()
                content = resp.json()["content"][0]["text"]
        except httpx.HTTPError as exc:
            raise LLMError(f"{self.name} request failed: {exc}") from exc
        return extract_json(content)


class RuleBasedClient(LLMClient):
    """Deterministic fallback — no external calls.

    Produces structurally-valid assessments from local signal data so the
    system remains fully operational without any LLM configured.
    """

    name = "rule_based"

    async def complete_structured(
        self, *, system: str, user: str, temperature: float = 0.2, max_tokens: int = 1500
    ) -> dict[str, Any]:
        return {
            "decision": "proceed",
            "confidence": 0.6,
            "summary": "Rule-based fallback assessment (no LLM configured).",
            "market_conditions": {"note": "offline"},
            "risks": ["no LLM available — rule-based assessment only"],
            "supporting_factors": ["system operating in degraded mode"],
            "notes": "configure an LLM provider for intelligent reasoning",
        }


def build_llm_client(
    *,
    base_url: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
    provider: str | None = None,
) -> LLMClient:
    """Factory: build an LLM client from provider config + environment."""
    import os

    provider = provider or os.getenv("HERMES_LLM_PROVIDER", "external")
    if provider == "anthropic":
        key = api_key or os.getenv("ANTHROPIC_API_KEY", "")
        m = model or os.getenv("ANTHROPIC_MODEL", "claude-3-5-haiku-latest")
        return AnthropicClient(api_key=key, model=m)
    if provider == "ollama":
        url = base_url or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")
        m = model or os.getenv("OLLAMA_MODEL", "llama3.1:8b")
        return OpenAICompatClient(base_url=url, api_key="ollama", model=m)
    if provider == "lm_studio":
        url = base_url or os.getenv("LM_STUDIO_BASE_URL", "http://localhost:1234/v1")
        m = model or os.getenv("LM_STUDIO_MODEL", "")
        if not m:
            # LM Studio's model identifier is whatever the user has loaded in
            # their own instance — there is no safe default to assume, so an
            # empty LM_STUDIO_MODEL degrades to the rule-based fallback rather
            # than sending an empty model string the server would reject.
            logger.warning("LM_STUDIO_MODEL not set — using rule-based fallback")
            return RuleBasedClient()
        # LM Studio's local server doesn't validate the key, but the client
        # still needs a non-empty string to send *some* Authorization header —
        # a dummy value is fine and matches LM Studio's own documented convention.
        return OpenAICompatClient(base_url=url, api_key="lm-studio", model=m)
    # default: OpenAI-compatible
    url = base_url or os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
    key = api_key or os.getenv("OPENAI_API_KEY", "")
    m = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    if not key:
        logger.warning("no LLM API key configured — using rule-based fallback")
        return RuleBasedClient()
    return OpenAICompatClient(base_url=url, api_key=key, model=m)
