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
## SQL Server + DuckDB
#####################
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    unixodbc-dev && \
  curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
  echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
    > /etc/apt/sources.list.d/mssql-release.list && \
  apt-get update && \
  ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/*

RUN uv add 'duckdb>=1.4.0' 'pyodbc>=5.3.0'
