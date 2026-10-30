FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.8.4 /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1 PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-dev
COPY ingest ./ingest
COPY simulator ./simulator
COPY data ./data
# Default role: backend daemon. compose overrides the command for the api and simulator roles.
CMD ["python", "-m", "ingest.backend"]
