FROM python:3.13-slim-bookworm

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
RUN uv add 'python-tds>=1.17.0' 'sqlalchemy-pytds>=1.0.2' 'pyodbc>=5.2.0'

# O Cloud SQL Auth Proxy abre uma porta local no container e encaminha
# a conexão para a instância Cloud SQL configurada em DATABASE_CONNECTION_NAME.
# Ref: https://cloud.google.com/sql/docs/sqlserver/connect-auth-proxy
RUN apt-get update && \
  apt-get install -y --no-install-recommends ca-certificates curl gnupg unixodbc && \
  curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
  curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
    -o /etc/apt/sources.list.d/mssql-release.list && \
  apt-get update && \
  ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 unixodbc-dev && \
  curl -fsSL -o /usr/local/bin/cloud-sql-proxy \
    https://storage.googleapis.com/cloud-sql-connectors/cloud-sql-proxy/v2.18.3/cloud-sql-proxy.linux.amd64 && \
  chmod +x /usr/local/bin/cloud-sql-proxy && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*
