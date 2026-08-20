from enum import Enum


class constants(Enum):
  TABLES = {
    # Agendamento
    "absenteismo": 10022,
    "agendas": 10010,
    # Ambulatório
    "ambulatorio_pacientes": 10026,
    # Assistência ao Paciente
    "escalas_assistenciais": 10014,
    "prescricao_medica_itens": 10029,
    "procedimentos_realizados": 10002,
    # Atendimento Domiciliar
    "captacoes_ad_encerradas": 10025,
    "captacoes_ad_iniciadas": 10024,
    "atendimento_domiciliar_entradas": 10015,
    "atendimento_domiciliar_escalas": 10012,
    "atendimento_domiciliar_linhas_cuidado": 10032,
    "atendimento_domiciliar_pacientes": 10011,
    "atendimento_domiciliar_saidas": 10016,
    # Centro Cirúrgico
    "cirurgias": 10027,
    # Documento do Paciente
    "documentos_emitidos": 10028,
    # Estoque
    "estoque_entradas": 10009,
    "estoque_saidas": 10008,
    # Faturamento
    "faturamento_convenio_particular": 10031,
    "faturamento_sih_sus": 10005,
    "producao_sadt_sih_sus": 10007,
    # Internação
    "internacao_diarias": 10013,
    "leitos_dia": 10018,
    "movimentacao_periodo": 10023,
    "pacientes_dia": 10019,
    # NHE
    "notificacoes": 10033,
    # Nutrição e Dietética
    "pacientes_snd": 10006,
    # Obstetrícia
    "neonatologia": 10035,
    # Pronto atendimento
    "emergencia_pacientes_atendimento": 10004,
    "emergencia_pacientes_registrados": 10001,
    # Qualidade
    "pesquisas_periodo": 10020,
    # Recursos Humanos
    "plantoes": 10034,
    # SADT
    "exames_solicitados": 10003,
    "procedimentos_especiais": 10017,
    # Sistema
    "log_acessos": 10021,
  }

  INFISICAL_PATH = "/sarah_padi"

  CNES = "5462886"

  CNES_LIST = [
    "6694330",  # PADI MIGUEL COUTO
    "4092104",  # PADI PAULINO WERNECK
    "4466403",  # PADI FARMARCIA
    "6694101",  # PADI SALGADO FILHO
    "4337557",  # PADI ALBERT SCHWEITZER
    "7110340",  # PADI FRANCISCO DA SILVA TELLES
    "7063679",  # PADI LOURENÇO JORGE
    "2976706",  # PADI ROCHA FARIA
    "7110324",  # PADI PEDRO II
  ]
