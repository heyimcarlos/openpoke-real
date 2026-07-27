# OpenPoke 🌴

OpenPoke is a simplified, open-source take on [Interaction Company’s](https://interaction.co/about) [Poke](https://poke.com/) assistant—built to show how a multi-agent orchestration stack can feel genuinely useful. It keeps the handful of things Poke is great at (email triage, reminders, and persistent agents) while staying easy to spin up locally.

- Multi-agent FastAPI backend that mirrors Poke's interaction/execution split, powered by [OpenRouter](https://openrouter.ai/).
- Gmail tooling via [Composio](https://composio.dev/) for drafting/replying/forwarding without leaving chat.
- Trigger scheduler and background watchers for reminders and "important email" alerts.
- Next.js web UI that proxies everything through the shared `.env`, so plugging in API keys is the only setup.

## Requirements
- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- Node.js 18+
- npm 9+

## Quickstart
1. **Clone and enter the repo.**
   ```bash
   git clone https://github.com/heyimcarlos/openpoke-real
   cd openpoke-real
   ```
2. **Create a shared env file.** Copy the template and open it in your editor:
   ```bash
   cp .env.example .env
   ```
3. **Get your API keys and add them to `.env`:**
   
   **OpenRouter (Required)**
   - Create an account at [openrouter.ai](https://openrouter.ai/)
   - Generate an API key
   - Replace `your_openrouter_api_key_here` with your actual key in `.env`
   
   **Composio (Required for Gmail)**
   - Sign in at [composio.dev](https://composio.dev/)
   - Create an API key
   - Set up Gmail integration and get your auth config ID
   - Replace `your_composio_api_key_here` and `your_gmail_auth_config_id_here` in `.env`
   - Use one local Composio user ID in `OPENPOKE_LOCAL_COMPOSIO_USER_ID`.
     The signed local bearer token must carry the same value in its
     `composio_user_id` claim.
   - Keep `OPENPOKE_CHAT_ALLOWED_TENANT_ID` and
     `OPENPOKE_CHAT_ALLOWED_ACTOR_ID` set to the single local principal.
     The token helper uses these values by default.
4. **Start local PostgreSQL:**
   ```bash
   docker compose --env-file compose.test.env -f compose.test.yaml up -d --wait
   ```
   The example database URL points to this local-only instance.
5. **Install the locked backend environment:**
   ```bash
   uv sync --locked --group dev
   ```
6. **Mint a short-lived local chat token.**
   ```bash
   uv run --locked python -m scripts.mint_local_chat_token
   ```
   Copy the output into `OPENPOKE_WEB_BEARER_TOKEN` in `.env`. The token is
   loaded only by the server-side Next.js proxy. This token and
   `OPENPOKE_LOCAL_COMPOSIO_USER_ID` are single-user development shims. The
   proxy rejects them when `NODE_ENV=production`. A production UI must
   authenticate each browser user and resolve that identity server-side.
7. **Install frontend dependencies:**
   ```bash
   npm install --prefix web
   ```
8. **Start the FastAPI server:**
   ```bash
   uv run --locked python -m server.server --reload
   ```
9. **Start the durable execution worker (new terminal):**
   ```bash
   uv run --locked python -m server.worker
   ```
10. **Start the Next.js app (new terminal):**
   ```bash
   npm run dev --prefix web
   ```
11. **Connect Gmail for email workflows.** With the services running, open [http://localhost:3000](http://localhost:3000), head to *Settings → Gmail*, and complete the Composio OAuth flow. This step is required for email drafting, replies, and the important-email monitor.

The web app proxies API calls to the Python server using the values in `.env`, so keeping both processes running is required for end-to-end flows.

### Current durability boundary

PostgreSQL is authoritative for accepted execution tasks, leases, attempts, and
typed results. Conversation history is still file-backed. A crash after database
completion but before conversation projection can omit the UI update. A worker
crash after an external tool side effect but before database completion can
redeliver that side effect. Durable conversation turns and effect idempotency
remain later work.

The file-backed conversation and orchestrator state is one shared thread, not
tenant-partitioned storage. Chat authentication therefore fails closed unless
one allowed tenant and actor are configured, and rejects valid tokens for any
other principal. Multi-principal chat requires durable, tenant-scoped
conversation state.

The durable worker never falls back to process-global Gmail state. The local
token's signed `composio_user_id` claim must match the server-side OAuth user ID.
Mapping OAuth connections into production identity-provider claims remains an
auth-service responsibility.

## Task ledger integration tests

The durable task-control-plane tests use PostgreSQL and never call a model
provider:

```bash
docker compose --env-file compose.test.env -f compose.test.yaml up -d --wait
uv sync --locked --group dev
uv run --locked pytest -q
docker compose --env-file compose.test.env -f compose.test.yaml down
```

The test database listens only on `127.0.0.1:55432` and uses trust
authentication for this disposable local environment.

## Project Layout
- `server/` – FastAPI application and agents
- `web/` – Next.js app
- `server/data/` – runtime data (ignored by git)

## License
MIT — see [LICENSE](LICENSE).
