# mesonet-aq nightly pipeline: ECS Fargate one-shot (terraform/pipeline.tf).
# Dependencies are installed from uv.lock, so the image matches CI exactly.
FROM ghcr.io/astral-sh/uv:0.9.25 AS uv

FROM python:3.12-slim AS build
COPY --from=uv /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PROJECT_ENVIRONMENT=/venv
WORKDIR /src
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src/ ./src/
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim
COPY --from=build /venv /venv
ENV PATH=/venv/bin:$PATH PYTHONUNBUFFERED=1
RUN useradd --system --uid 10001 app
USER app
ENTRYPOINT ["mesonet-aq"]
CMD ["run"]
