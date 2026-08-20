FROM python:3.13-slim

#####################
## Dockerfile padrão
#####################
ENV DO_NOT_TRACK=1
COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /uvx /bin/
WORKDIR /opt/prefect/pipelines_v3_rj_sms/
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev
ENV PATH="/opt/prefect/pipelines_v3_rj_sms/.venv/bin:$PATH"
COPY ./pipelines /opt/prefect/pipelines_v3_rj_sms/pipelines

#####################
## Firefox
#####################
RUN apt-get update && \
  apt-get install -y --no-install-recommends ca-certificates firefox-esr && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*
