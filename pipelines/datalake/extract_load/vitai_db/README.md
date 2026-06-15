# Extração e Carga de Dados - Prontuário Vitai (Rio Saúde)

Este diretório contém os códigos referentes ao pipeline de extração e carga (Extract & Load) de dados do banco de dados do **Prontuário Vitai** (sistema utilizado pela Rio Saúde) para o Datalake da Secretaria Municipal de Saúde do Rio de Janeiro.

## Objetivo

O pipeline tem como objetivo realizar a ingestão contínua e incremental de diversas tabelas do banco de dados relacional do Prontuário Vitai, transportando essas informações para o Google BigQuery e Google Cloud Storage, onde são armazenadas como tabelas _BigLake_ no dataset `brutos_prontuario_vitai`.

## Funcionamento

O fluxo principal (`vitai_db_extraction`) realiza as seguintes etapas:

1. **Obtenção de Credenciais**: Resgata a URL de conexão com o banco do Vitai de forma segura usando o Infisical (path `/prontuario-vitai`).
2. **Definição de Janela Temporal**: Determina o intervalo de tempo para a extração baseando-se em uma coluna de data/hora (ex: `created_at` ou `datahora`). Se não for fornecido, extrai os últimos 7 dias por padrão.
3. **Paginação de Consultas**: Para evitar sobrecarga no banco de dados e estouro de memória, a tarefa conta o número de registros no intervalo e gera múltiplas consultas paginadas (utilizando `LIMIT` e `OFFSET`) com base no parâmetro `batch_size`.
4. **Extração de Dados**: As consultas são executadas em paralelo de forma controlada (rate limit de "um por segundo").
5. **Tratamento Inicial**: 
   - Se a coluna `id` estiver presente, ela é renomeada para `gid`.
   - Adiciona colunas de controle do datalake (`datalake_loaded_at` e `partition_date`).
6. **Carga no Datalake**: Os *DataFrames* resultantes são carregados em paralelo no datalake (formato Parquet) no modo de adição (`append`), particionados diariamente.

## Agendamentos (Schedules)

Conforme configurado em `schedules.py`, os dados são extraídos a cada **12 horas**. O pipeline processa tabelas de dois schemas principais (`basecentral` e `dtw`), incluindo:

* **Pacientes e Atendimentos:** `paciente`, `boletim`, `atendimento`, `internacao`, `alta`, `classificacao_risco`.
* **Clínico:** `alergia`, `diagnostico`, `exame`, `prescricao`, `item_prescricao`, `cirurgia`, `relato_cirurgico`, `resumo_alta`.
* **Faturamento (dtw):** `fat_boletim`, `fat_atendimento`, `fat_internacao`, `fat_recem_nascido`.
* **Cadastros:** `profissional`, `m_estabelecimento`.

