from enum import Enum


class constants(Enum):
  {
    # Agendamento
    10022: 'absenteismo',
    10010: 'agendas',

    # Ambulatório
    10026: 'ambulatorio_pacientes',

    # Assistência ao Paciente
    10014: 'escalas_assistenciais',
    10029: 'prescricao_medica_itens',
    10002: 'procedimentos_realizados',

    # Atendimento Domiciliar 
    10025: 'captacoes_ad_encerradas',
    10024: 'captacoes_ad_iniciadas',
    10015: 'atendimento_domiciliar_entradas',
    10012: 'atendimento_domiciliar_escalas',
    10032: 'atendimento_domiciliar_linhas_cuidado',
    10011: 'atendimento_domiciliar_pacientes',
    10016: 'atendimento_domiciliar_saidas',

    # Centro Cirúrgico
    10027: 'cirurgias',

    # Documento do Paciente
    10028: 'documentos_emitidos',

    # Estoque
    10009: 'estoque_entradas',
    10008: 'estoque_saidas',

    # Faturamento
    10031: 'faturamento_convenio_particular',
    10005: 'faturamento_sih_sus',
    10007: 'producao_sadt_sih_sus',

    # Internação
    10013: 'internacao_diarias',
    10018: 'leitos_dia',
    10023: 'movimentacao_periodo',
    10019: 'pacientes_dia',

    # NHE
    10033: 'notificacoes',

    # Nutrição e Dietética
    10006: 'pacientes_snd',

    # Obstetrícia 
    10035: 'neonatologia',

    # Pronto atendimento 
    10004: 'emergencia_pacientes_atendimento',
    10001: 'emergencia_pacientes_registrados',

    # Qualidade
    10020: 'pesquisas_periodo',

    # Recursos Humanos
    10034: 'plantoes',

    # SADT
    10003: 'exames_solicitados',
    10017: 'procedimentos_especiais',

    # Sistema
    10021: 'log_acessos'
    }