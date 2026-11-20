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
COPY experiments/baseline experiments/baseline

RUN uv sync --frozen --package simulator --no-dev --no-editable \
    && /app/.venv/bin/python -c "import simulator.config, pathlib; path = pathlib.Path(simulator.config.__file__).with_name('profile.yaml'); assert path.is_file(), path"

FROM python:3.12-slim-bookworm

RUN useradd --create-home --uid 10001 --shell /usr/sbin/nologin app
WORKDIR /app
COPY --from=build --chown=app:app /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1
USER app
ENTRYPOINT ["simulator"]
CMD ["--tps", "200", "--duration", "10m", "--seed", "42", "--url", "http://localhost:8000"]
