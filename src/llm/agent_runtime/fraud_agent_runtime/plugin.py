"""Runs each model call from the agent's Langfuse prompt.

Langfuse is the source of the system prompt, its model settings and the A/B test. The `production` version of the
agent's prompt may name a test in its config: `{"abTest": {"label": "variant-b", "share": 0.5}}`. Each conversation
falls on one side by a hash of its session id, so it keeps its variant for every model call. The chosen version's
config (`temperature`, `top_p`, `max_tokens`) sets the model, and the call is linked to that version in Langfuse.
"""

import hashlib
from typing import Any, Optional, Protocol

from google.adk.agents.callback_context import CallbackContext
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.adk.plugins.base_plugin import BasePlugin
from langfuse._client.attributes import LangfuseOtelSpanAttributes
from opentelemetry import trace

PRODUCTION = "production"
MODEL_SETTINGS = ("temperature", "top_p", "max_tokens")


class Prompt(Protocol):
    name: str
    version: int
    prompt: str
    config: dict[str, Any]


class PromptSource(Protocol):
    def get_prompt(
        self, name: str, *, label: Optional[str] = None, cache_ttl_seconds: Optional[int] = None
    ) -> Prompt: ...


def variant_label(prompt: Prompt, session_id: str) -> str:
    test = (prompt.config or {}).get("abTest")
    if not test or not test.get("label"):
        return PRODUCTION
    share = float(test.get("share", 0.5))
    bucket = int(hashlib.sha256(f"{prompt.name}:{session_id}".encode()).hexdigest()[:8], 16) / 0xFFFFFFFF
    return test["label"] if bucket < share else PRODUCTION


class LangfusePromptPlugin(BasePlugin):
    def __init__(self, prompts: PromptSource, prompt_name: str, cache_ttl_seconds: int = 60):
        super().__init__(name="langfuse_prompts")
        self.prompts = prompts
        self.prompt_name = prompt_name
        self.cache_ttl_seconds = cache_ttl_seconds
        self.model = None
        self.defaults: dict[str, Any] = {}

    def bind(self, model) -> None:
        """The agent's model, whose settings each call takes from the chosen prompt version."""
        self.model = model
        self.defaults = {setting: getattr(model, setting, None) for setting in MODEL_SETTINGS}

    def choose(self, session_id: str) -> tuple[Prompt, str]:
        production = self.prompts.get_prompt(
            self.prompt_name, label=PRODUCTION, cache_ttl_seconds=self.cache_ttl_seconds
        )
        label = variant_label(production, session_id)
        if label == PRODUCTION:
            return production, label
        return self.prompts.get_prompt(self.prompt_name, label=label, cache_ttl_seconds=self.cache_ttl_seconds), label

    async def before_model_callback(
        self, *, callback_context: CallbackContext, llm_request: LlmRequest
    ) -> Optional[LlmResponse]:
        prompt, label = self.choose(callback_context.session.id)
        existing = llm_request.config.system_instruction
        if isinstance(existing, str) and existing.strip():
            llm_request.config.system_instruction = f"{prompt.prompt}\n\n{existing}"
        else:
            llm_request.config.system_instruction = prompt.prompt
        # Substrate runs one conversation per actor, so setting the shared model object cannot leak into another
        # conversation's call.
        if self.model is not None:
            config = prompt.config or {}
            for setting in MODEL_SETTINGS:
                setattr(self.model, setting, config.get(setting, self.defaults.get(setting)))
        # ADK runs this callback inside the call_llm span, so these attributes land on the model call.
        span = trace.get_current_span()
        span.set_attribute(LangfuseOtelSpanAttributes.OBSERVATION_PROMPT_NAME, prompt.name)
        span.set_attribute(LangfuseOtelSpanAttributes.OBSERVATION_PROMPT_VERSION, prompt.version)
        span.set_attribute(LangfuseOtelSpanAttributes.TRACE_TAGS, [f"variant:{label}"])
        return None
