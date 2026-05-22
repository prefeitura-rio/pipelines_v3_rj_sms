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
## SQL Server
#####################
RUN uv add 'python-tds>=1.17.0'

# O Cloud SQL Auth Proxy abre uma porta local no container e encaminha
# a conexão para a instância Cloud SQL configurada em DATABASE_CONNECTION_NAME.
# Ref: https://cloud.google.com/sql/docs/sqlserver/connect-auth-proxy
RUN apt-get update && \
  apt-get install -y --no-install-recommends ca-certificates curl && \
  curl -fsSL -o /usr/local/bin/cloud-sql-proxy \
    https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.18.3/cloud-sql-proxy.linux.amd64 && \
  chmod +x /usr/local/bin/cloud-sql-proxy && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*
