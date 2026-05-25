"""
Schedules for the vitai dump pipeline
"""

from pipelines.utils.schedules import create_schedule_list

flow_parameters = [
  {
    "environment": "prod",
    "table_name": "paciente",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__paciente_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "boletim",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__boletim_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "alergia",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__alergia_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "atendimento",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__atendimento_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "cirurgia",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__cirurgia_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "classificacao_risco",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__classificacao_risco_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "diagnostico",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__diagnostico_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "exame",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__exame_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "profissional",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__profissional_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "m_estabelecimento",
    "schema_name": "basecentral",
    "datetime_column": "datahora",
    "target_name": "basecentral__m_estabelecimento_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "relato_cirurgico",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__relato_cirurgico_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "resumo_alta",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__resumo_alta_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "internacao",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__internacao_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "alta",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__alta_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "fat_boletim",
    "schema_name": "dtw",
    "datetime_column": "datahora",
    "target_name": "dtw__fat_boletim_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "fat_atendimento",
    "schema_name": "dtw",
    "datetime_column": "data_fim",
    "target_name": "dtw__fat_atendimento_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "fat_internacao",
    "schema_name": "dtw",
    "datetime_column": "data_saida",
    "target_name": "dtw__fat_internacao_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "item_prescricao",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__item_prescricao_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "prescricao",
    "schema_name": "basecentral",
    "datetime_column": "created_at",
    "target_name": "basecentral__prescricao_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
  {
    "environment": "prod",
    "table_name": "fat_recem_nascido",
    "schema_name": "dtw",
    "datetime_column": "created_at",
    "target_name": "dtw__fat_recem_nascido_eventos",
    "partition_column": "datalake_loaded_at",
    "batch_size": 10000,
  },
]

daily_parameters = [{**p, "relative_date": "D-3"} for p in flow_parameters]
monthly_parameters = [{**p, "relative_date": "M-1"} for p in flow_parameters]

daily_schedules = create_schedule_list(
  parameters_list=daily_parameters, interval="12-hours", config={"hour": 2, "minute": 0}
)

monthly_schedules = create_schedule_list(
  parameters_list=monthly_parameters,
  interval="monthly",
  config={"day": 1, "hour": 2, "minute": 0},
)

schedules = daily_schedules + monthly_schedules
