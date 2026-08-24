# Extração e Carga de Dados - Prontuário SARAH PADI

Este diretório contém os códigos referentes ao pipeline de extração e carga (Extract & Load) de dados da API do sistema **SARAH PADI** (sistema utilizado pela rede PADI da Secretaria Municipal de Saúde do Rio de Janeiro) para o Datalake da SMS-RJ.

## Objetivo

O pipeline tem como objetivo realizar a ingestão diária de diversas tabelas clínicas e operacionais do sistema SARAH PADI, consultando sua API REST (BIS — BI System) e transportando essas informações para o Google BigQuery, onde são armazenadas no dataset `brutos_prontuario_sarah_padi`.

## Funcionamento

O fluxo principal (`padi_extraction`) realiza as seguintes etapas:

1. **Obtenção de Credenciais**: Resgata de forma segura as cinco credenciais necessárias do Infisical (path `/sarah_padi`): `user`, `password`, `access-token`, `auth-url` e `bis-url`.
2. **Parsing da Data**: Normaliza a data de referência fornecida para o formato `dd/mm/yyyy`. Caso nenhuma data seja informada, utiliza D-1 como padrão.
3. **Autenticação**: Realiza autenticação na API do SARAH PADI enviando a senha codificada em Base64, obtendo um token de sessão.
4. **Extração de Dados**: Consulta a API BIS via método `getFatos`, passando o código CNES da unidade, o identificador da tabela e a data de referência. Os registros retornados recebem a coluna de controle `extracted_at`.
5. **Carga no Datalake**: O DataFrame resultante é carregado no datalake em modo de adição (`append`), em formato CSV com delimitador `;`, particionado pela coluna `extracted_at`.

## Agendamentos (Schedules)

Conforme configurado em `schedules.py`, os dados são extraídos diariamente às **00:05**. O pipeline processa todas as 36 tabelas disponíveis, com execuções escalonadas em intervalos de 1 minuto entre si. A data de referência é sempre D-1.

## Tabelas

As tabelas extraídas estão organizadas pelos seguintes domínios:

- **Agendamento:** `absenteismo`, `agendas`
- **Ambulatório:** `ambulatorio_pacientes`
- **Assistência ao Paciente:** `escalas_assistenciais`, `prescricao_medica_itens`, `procedimentos_realizados`
- **Atendimento Domiciliar:** `captacoes_ad_encerradas`, `captacoes_ad_iniciadas`, `atendimento_domiciliar_entradas`, `atendimento_domiciliar_escalas`, `atendimento_domiciliar_linhas_cuidado`, `atendimento_domiciliar_pacientes`, `atendimento_domiciliar_saidas`
- **Centro Cirúrgico:** `cirurgias`
- **Documento do Paciente:** `documentos_emitidos`
- **Estoque:** `estoque_entradas`, `estoque_saidas`
- **Faturamento:** `faturamento_convenio_particular`, `faturamento_sih_sus`, `producao_sadt_sih_sus`
- **Internação:** `internacao_diarias`, `leitos_dia`, `movimentacao_periodo`, `pacientes_dia`
- **NHE:** `notificacoes`
- **Nutrição e Dietética:** `pacientes_snd`
- **Obstetrícia:** `neonatologia`
- **Pronto Atendimento:** `emergencia_pacientes_atendimento`, `emergencia_pacientes_registrados`
- **Qualidade:** `pesquisas_periodo`
- **Recursos Humanos:** `plantoes`
- **SADT:** `exames_solicitados`, `procedimentos_especiais`
- **Sistema:** `log_acessos`

## Atualizações
