from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest
from google.adk.models.llm_request import LlmRequest
from google.genai import types
from kagent.config.v1 import adk_pb2

from fraud_agent_runtime import entrypoint, plugin


@dataclass
class FakePrompt:
    name: str
    version: int
    prompt: str
    config: dict = field(default_factory=dict)


class FakePrompts:
    def __init__(self, prompts: dict[str, FakePrompt]):
        self.prompts = prompts
        self.calls = []

    def get_prompt(self, name, *, label=None, cache_ttl_seconds=None):
        self.calls.append((name, label, cache_ttl_seconds))
        return self.prompts[label]


class FakeSpan:
    def __init__(self):
        self.attributes = {}

    def set_attribute(self, key, value):
        self.attributes[key] = value


@pytest.fixture
def span(monkeypatch):
    fake = FakeSpan()
    monkeypatch.setattr(plugin.trace, "get_current_span", lambda: fake)
    return fake


def context(session_id: str):
    return SimpleNamespace(session=SimpleNamespace(id=session_id))


def request(instruction=None):
    return LlmRequest(config=types.GenerateContentConfig(system_instruction=instruction))


PRODUCTION = FakePrompt("evidence-agent-system-prompt", 3, "You are read-only.")
TESTED = FakePrompt(
    "evidence-agent-system-prompt",
    3,
    "You are read-only.",
    {"abTest": {"label": "variant-b", "share": 0.5}},
)
VARIANT = FakePrompt("evidence-agent-system-prompt", 4, "You are read-only, briefly.", {"temperature": 0.2})


async def test_the_production_prompt_becomes_the_system_instruction_and_is_linked(span):
    prompts = FakePrompts({"production": PRODUCTION})
    instance = plugin.LangfusePromptPlugin(prompts, "evidence-agent-system-prompt")
    llm_request = request()

    assert await instance.before_model_callback(callback_context=context("s1"), llm_request=llm_request) is None

    assert llm_request.config.system_instruction == "You are read-only."
    assert prompts.calls == [("evidence-agent-system-prompt", "production", 60)]
    assert span.attributes["langfuse.observation.prompt.name"] == "evidence-agent-system-prompt"
    assert span.attributes["langfuse.observation.prompt.version"] == 3
    assert span.attributes["langfuse.trace.tags"] == ["variant:production"]


async def test_the_prompt_comes_before_kagents_own_guidance(span):
    instance = plugin.LangfusePromptPlugin(FakePrompts({"production": PRODUCTION}), "p")
    llm_request = request("Use load_skill before following a skill.")

    await instance.before_model_callback(callback_context=context("s1"), llm_request=llm_request)

    assert llm_request.config.system_instruction == "You are read-only.\n\nUse load_skill before following a skill."


@pytest.mark.parametrize("share", [0.0, 1.0])
def test_a_share_of_zero_or_one_sends_every_conversation_to_one_side(share):
    prompt = FakePrompt("p", 1, "x", {"abTest": {"label": "variant-b", "share": share}})
    labels = {plugin.variant_label(prompt, f"session-{i}") for i in range(50)}
    assert labels == ({"production"} if share == 0.0 else {"variant-b"})


def test_a_conversation_keeps_its_variant_and_the_split_follows_the_share():
    labels = [plugin.variant_label(TESTED, f"session-{i}") for i in range(2000)]
    assert labels[:10] == [plugin.variant_label(TESTED, f"session-{i}") for i in range(10)]
    assert 0.45 < labels.count("variant-b") / len(labels) < 0.55


@pytest.mark.parametrize("config", [{}, {"abTest": {}}, {"abTest": {"share": 1.0}}])
def test_without_a_named_variant_every_conversation_uses_production(config):
    assert plugin.variant_label(FakePrompt("p", 1, "x", config), "s") == "production"


async def test_the_variant_sets_its_model_settings_and_production_restores_the_defaults(span, monkeypatch):
    prompts = FakePrompts({"production": TESTED, "variant-b": VARIANT})
    instance = plugin.LangfusePromptPlugin(prompts, "evidence-agent-system-prompt")
    model = SimpleNamespace(temperature=None, top_p=0.8, max_tokens=None)
    instance.bind(model)

    monkeypatch.setattr(plugin, "variant_label", lambda prompt, session_id: "variant-b")
    llm_request = request()
    await instance.before_model_callback(callback_context=context("s1"), llm_request=llm_request)
    assert llm_request.config.system_instruction == "You are read-only, briefly."
    assert (model.temperature, model.top_p) == (0.2, 0.8)
    assert span.attributes["langfuse.observation.prompt.version"] == 4
    assert span.attributes["langfuse.trace.tags"] == ["variant:variant-b"]

    monkeypatch.setattr(plugin, "variant_label", lambda prompt, session_id: "production")
    await instance.before_model_callback(callback_context=context("s2"), llm_request=request())
    assert (model.temperature, model.top_p) == (None, 0.8)


def test_remote_agents_become_a2a_tools_with_a_placeholder_key():
    agent = adk_pb2.ADKAgentConfig(name="coordinator")
    entrypoint.add_remote_agents(
        agent,
        [
            {
                "name": "evidence_agent",
                "description": "Explains declines.",
                "url": "http://agentgateway.llm.svc.cluster.local/a2a/evidence-agent/",
            }
        ],
    )
    [remote] = agent.remote_agents
    assert remote.url == "http://agentgateway.llm.svc.cluster.local/a2a/evidence-agent"
    assert dict(remote.headers) == {"Authorization": "placeholder"}
    assert remote.isolate_sessions is True


def test_the_prompt_plugin_reads_langfuse_from_the_environment(monkeypatch):
    created = {}

    class FakeLangfuse:
        def __init__(self, **kwargs):
            created.update(kwargs)

    monkeypatch.setattr("langfuse.Langfuse", FakeLangfuse)
    monkeypatch.setenv("LANGFUSE_HOST", "http://langfuse-web.llm.svc.cluster.local:3000")
    monkeypatch.setenv("LANGFUSE_PROMPT", "coordinator-system-prompt")
    monkeypatch.delenv("LANGFUSE_PUBLIC_KEY", raising=False)

    instance = entrypoint.prompt_plugin()

    assert instance.prompt_name == "coordinator-system-prompt"
    assert created["public_key"] == "placeholder"
    assert created["tracing_enabled"] is False
