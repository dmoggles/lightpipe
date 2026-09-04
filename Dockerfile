FROM ghcr.io/astral-sh/uv:0.8 AS uv

FROM python:3.12-slim
COPY --from=uv /uv /uvx /bin/
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
COPY examples ./examples
RUN uv sync --frozen --no-dev --extra all
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
CMD ["lightpipe", "--help"]
