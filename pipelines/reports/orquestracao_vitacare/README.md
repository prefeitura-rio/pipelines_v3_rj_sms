# Orquestração: Vitacare Histórico

Este diretório contém o pipeline orquestrador que coordena a extração mensal dos dados históricos do Vitacare, executando três flows em sequência para garantir que os dados cheguem ao Datalake.

## Funcionamento

O orquestrador (`orquestracao_vitacare`) executa os seguintes flows em sequência:

1. **Google Drive → GCS** (`gdrive_to_gcs`): Baixa os backups do Vitacare armazenados no Google Drive e faz upload para o bucket `vitacare_backups_gdrive` no GCS.
2. **Backup SQL Server → Cloud SQL** (`sqlserver_backup`): Restaura os arquivos `.BAK` do GCS para uma instância Cloud SQL (nome: `vitacare`).
3. **Extração: Vitacare Histórico** (`vitacare_historico`): Conecta na instância Cloud SQL via Cloud SQL Proxy e extrai todas as tabelas para o BigQuery, processando os CNES de forma paralela (limite de concorrência configurável).

## Agendamentos

- **Frequência:** Mensal
- **Dia/Hora:** Dia 7 de cada mês às 16:00
- **Environment:** prod

## Fluxo dos Dados

```
Google Drive
    ↓ (gdrive_to_gcs)
GCS Bucket (vitacare_backups_gdrive)
    ↓ (sqlserver_backup)
Cloud SQL (vitacare)
    ↓ (vitacare_historico)
BigQuery
```
