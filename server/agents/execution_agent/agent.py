"""Execution Agent implementation."""

import hashlib
from pathlib import Path
from typing import Optional

from ...services.execution import get_execution_agent_logs


# Load system prompt template from file
_prompt_path = Path(__file__).parent / "system_prompt.md"
if _prompt_path.exists():
    SYSTEM_PROMPT_TEMPLATE = _prompt_path.read_text(encoding="utf-8").strip()
else:
    # Placeholder template - you'll replace this with actual instructions
    SYSTEM_PROMPT_TEMPLATE = """You are an execution agent responsible for completing specific tasks using available tools.

Agent Name: {agent_name}
Purpose: {agent_purpose}

Instructions:
[TO BE FILLED IN BY USER]

You have access to Gmail tools to help complete your tasks. When given instructions:
1. Analyze what needs to be done
2. Use the appropriate tools to complete the task
3. Provide clear status updates on your actions

Be thorough, accurate, and efficient in your execution."""


def execution_storage_key(
    tenant_id: str,
    actor_id: str,
    agent_name: str,
) -> str:
    identity = f"{tenant_id}\x00{actor_id}\x00{agent_name}"
    return "agent-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()


class ExecutionAgent:
    """Manages state and history for an execution agent."""

    # Initialize execution agent with name, conversation limits, and log store access
    def __init__(
        self,
        name: str,
        conversation_limit: Optional[int] = None,
        *,
        storage_key: Optional[str] = None,
    ):
        """
        Initialize an execution agent.

        Args:
            name: Human-readable agent name (e.g., 'conversation with keith')
            conversation_limit: Optional limit on past conversations to include (None = all)
        """
        self.name = name
        self.storage_key = storage_key or name
        self.conversation_limit = conversation_limit
        self._log_store = get_execution_agent_logs()

    # Generate system prompt template with agent name and purpose derived from name
    def build_system_prompt(self) -> str:
        """Build the system prompt for this agent."""
        agent_purpose = f"Handle tasks related to: {self.name}"

        return SYSTEM_PROMPT_TEMPLATE.format(
            agent_name=self.name,
            agent_purpose=agent_purpose
        )

    # Combine base system prompt with conversation history, applying conversation limits
    def build_system_prompt_with_history(self) -> str:
        """
        Build system prompt including agent history.

        Returns:
            System prompt with embedded history transcript
        """
        base_prompt = self.build_system_prompt()

        # Load history transcript
        transcript = self._log_store.load_transcript(self.storage_key)

        if transcript:
            # Apply conversation limit if needed
            if self.conversation_limit and self.conversation_limit > 0:
                # Parse entries and limit them
                lines = transcript.split('\n')
                request_count = sum(1 for line in lines if '<agent_request' in line)

                if request_count > self.conversation_limit:
                    # Find where to cut
                    kept_requests = 0
                    cutoff_index = len(lines)
                    for i in range(len(lines) - 1, -1, -1):
                        if '<agent_request' in lines[i]:
                            kept_requests += 1
                            if kept_requests == self.conversation_limit:
                                cutoff_index = i
                                break
                    transcript = '\n'.join(lines[cutoff_index:])

            return f"{base_prompt}\n\n# Execution History\n\n{transcript}"

        return base_prompt

    # Log the agent's final response to the execution log store
    def record_response(self, response: str) -> None:
        """Record agent's response to the log."""
        self._log_store.record_agent_response(self.storage_key, response)

    def record_request(self, instructions: str) -> None:
        """Record the current request under the tenant-scoped storage key."""
        self._log_store.record_request(self.storage_key, instructions)
