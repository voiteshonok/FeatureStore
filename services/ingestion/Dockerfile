FROM python:3.12-slim-bookworm AS build

COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /uvx /bin/

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

COPY pyproject.toml uv.lock ./
COPY libs libs
COPY services services
COPY simulator simulator

RUN uv sync --frozen --package ingestion --no-dev --no-editable

FROM python:3.12-slim-bookworm

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin app
WORKDIR /app
COPY --from=build --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER app
EXPOSE 8000
CMD ["uvicorn", "ingestion.app:app", "--host", "0.0.0.0", "--port", "8000"]
