FROM python:3.13-slim

#####################
## Dockerfile padrão
#####################
ENV DO_NOT_TRACK=1
# Instala `uv` (Python compatível)
COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /uvx /bin/
# Copia dependências do projeto, instala
WORKDIR /opt/prefect/pipelines_v3_rj_sms/
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev
# Adiciona .venv ao PATH
ENV PATH="/opt/prefect/pipelines_v3_rj_sms/.venv/bin:$PATH"
# Copia os flows
COPY ./pipelines /opt/prefect/pipelines_v3_rj_sms/pipelines


#####################
## MySQL
#####################
RUN apt-get update && \
  apt install -y --no-install-recommends \
    wget lsb-release gnupg \
    gzip ca-certificates && \
    update-ca-certificates

RUN wget https://dev.mysql.com/get/mysql-apt-config_0.8.39-1_all.deb && \
  DEBIAN_FRONTEND=noninteractive dpkg -i mysql-apt-config_0.8.39-1_all.deb && \
  apt-get update && \
  apt-get install -y --no-install-recommends mysql-client && \
  rm -f mysql-apt-config_0.8.39-1_all.deb && \
  rm -rf /var/lib/apt/lists/*
