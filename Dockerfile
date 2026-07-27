FROM ghcr.io/astral-sh/uv:0.8.0 AS uv

FROM python:3.11-slim-bookworm

COPY --from=uv /uv /usr/local/bin/uv

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY server ./server

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin openpoke \
    && mkdir -p /app/server/data \
    && chown -R openpoke:openpoke /app/server/data

USER 10001

EXPOSE 8080

ENTRYPOINT [".venv/bin/python"]
CMD ["-m", "server.server", "--host", "0.0.0.0", "--port", "8080"]
