#!/bin/bash

# Baixa o código na branch especificada,
# descomprime o arquivo, e copia só a pasta desejada
ORG=prefeitura-rio
REPO=pipelines_v3_rj_sms
BRANCH=main
FOLDER_PATH=server
curl https://github.com/${ORG}/${REPO}/archive/refs/heads/${BRANCH}.tar.gz \
  | tar -xz --strip=2 "${REPO}-${BRANCH}/${FOLDER_PATH}"
