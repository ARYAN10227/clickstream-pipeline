FROM python:3.12.15-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv

RUN pip install --no-cache-dir uv==0.11.32

WORKDIR /app

# Dependencies first so this layer is reused until pyproject.toml or uv.lock change.
# SPEC.md is the package readme, which the build backend needs.
COPY pyproject.toml uv.lock SPEC.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src/ ./src/
RUN uv sync --frozen --no-dev

RUN useradd --system --uid 10001 --no-create-home clickpipe
USER clickpipe

ENV PATH="/app/.venv/bin:${PATH}"
CMD ["python", "-m", "clickpipe.main"]
