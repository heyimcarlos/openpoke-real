"""Interaction agent helpers for prompt construction."""

import re
from html import escape
from pathlib import Path
from typing import Dict, List

from ...services.execution import get_agent_roster

_prompt_path = Path(__file__).parent / "system_prompt.md"
SYSTEM_PROMPT = _prompt_path.read_text(encoding="utf-8").strip()
MAX_VISIBLE_CONTEXTS = 20


# Load and return the pre-defined system prompt from markdown file
def build_system_prompt() -> str:
    """Return the static system prompt for the interaction agent."""
    return SYSTEM_PROMPT


# Build structured message with conversation history, active agents, and current turn
def prepare_message_with_history(
    latest_text: str,
    transcript: str,
    message_type: str = "user",
) -> List[Dict[str, str]]:
    """Compose a message that bundles history, roster, and the latest turn."""
    sections: List[str] = []

    sections.append(_render_conversation_history(transcript))
    sections.append(
        f"<active_agents>\n{_render_active_agents(latest_text)}\n</active_agents>"
    )
    sections.append(_render_current_turn(latest_text, message_type))

    content = "\n\n".join(sections)
    return [{"role": "user", "content": content}]


# Format conversation transcript into XML tags for LLM context
def _render_conversation_history(transcript: str) -> str:
    history = transcript.strip()
    if not history:
        history = "None"
    return f"<conversation_history>\n{history}\n</conversation_history>"


# Format currently active execution agents into XML tags for LLM awareness
def _render_active_agents(latest_text: str) -> str:
    roster = get_agent_roster()
    roster.load()
    agents = _select_agent_contexts(latest_text, roster.get_agents())

    if not agents:
        return "None"

    rendered: List[str] = []
    for agent_name in agents:
        name = escape(agent_name or "agent", quote=True)
        rendered.append(f'<agent name="{name}" />')

    return "\n".join(rendered)


def _select_agent_contexts(latest_text: str, agents: List[str]) -> List[str]:
    """Select relevant context names first, then fill from most recent."""
    latest_tokens = _tokens(latest_text)
    ranked = [
        (len(_tokens(name) & latest_tokens), index, name)
        for index, name in enumerate(agents)
    ]
    relevant = sorted(
        (item for item in ranked if item[0]),
        key=lambda item: (-item[0], -item[1], item[2].casefold()),
    )
    selected = relevant[:MAX_VISIBLE_CONTEXTS]
    selected_indexes = {item[1] for item in selected}
    for item in reversed(ranked):
        if len(selected) == MAX_VISIBLE_CONTEXTS:
            break
        if item[1] not in selected_indexes:
            selected.append(item)
    return [item[2] for item in selected]


def _tokens(text: str) -> set[str]:
    return {
        normalized
        for token in re.findall(r"\w+", text.casefold())
        if (normalized := token.strip("_"))
    }


# Wrap the current message in appropriate XML tags based on sender type
def _render_current_turn(latest_text: str, message_type: str) -> str:
    tag = "new_agent_message" if message_type == "agent" else "new_user_message"
    body = latest_text.strip()
    return f"<{tag}>\n{body}\n</{tag}>"
