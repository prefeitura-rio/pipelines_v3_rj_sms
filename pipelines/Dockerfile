# Dockerfile para o Job que vai executar o flow
FROM python:3.13-slim

# Por via das dúvidas
ENV DO_NOT_TRACK=1

# Instala `uv` (Python compatível)
# [Ref] https://docs.astral.sh/uv/guides/integration/docker/#installing-uv
# [Ref] https://github.com/astral-sh/uv/pkgs/container/uv/versions
COPY --from=ghcr.io/astral-sh/uv:0.10 /uv /uvx /bin/

# Copia dependências do projeto, instala
WORKDIR /opt/prefect/pipelines_v3_rj_sms/
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev
# Adiciona .venv ao PATH
ENV PATH="/opt/prefect/pipelines_v3_rj_sms/.venv/bin:$PATH"

# Copia os flows
COPY ./pipelines /opt/prefect/pipelines_v3_rj_sms/pipelines

# Fim! Prefect cuida a partir daqui
