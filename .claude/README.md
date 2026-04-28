# Claude Code para migração

Situ: temos um monte de código no Prefect v1 que precisa ser migrado para o v3.
Uma forma oferecida pela Iplan é usar o Claude Code para isso.

> [!WARNING]
> A chave do Claude provida deve ser usada exclusivamente para migração.
> Abaixo, uma opção dada é executar o Claude dentro de um Docker, para que não
> haja nenhuma oportunidade de uso indevido. Um pouco de trabalho manual
> adicional será necessário nesse caso, para fazer commit dessas modificações.

## Setup

### Em Docker
Pré-configuração, caso você não queira instalar nada direto no seu computadorzinho:
```sh
$ mkdir -p /tmp/claude && cd /tmp/claude
$ git clone https://github.com/prefeitura-rio/pipelines_rj_sms
$ git clone https://github.com/prefeitura-rio/pipelines_v3_rj_sms
$ docker run --name claudio -it -v /tmp/claude:/data ubuntu:latest /bin/bash
# (a partir daqui, todos os comandos devem ser executados dentro do Docker)
$ apt update
$ apt install nodejs npm nano
$ npm install -g @anthropic-ai/claude-code
$ cd /data
```

Também coloque a pasta de skill (i.e. "migrate-v1-to-v3") em `/tmp/claude`.
Ela terá que ser copiada para a pasta de configuração do Claude sempre que for
modificada:

```sh
$ cp -r /caminho/local/da/pasta/migrate-v1-to-v3 ~/.claude/skills/migrate-v1-to-v3
```

Caso você saia do Docker e queira rodá-lo novamente:

```sh
$ docker start claudio && docker exec -it claudio /bin/bash
```


### Configuração

```sh
$ mkdir -p ~/.config/claude
$ nano ~/.config/claude/rj-ia-desenvolvimento-sms.json
# Cola o conteúdo do JSON, fecha e salva o arquivo [Ctrl+O -> Enter]
$ nano ~/.bashrc
# Cola esses `export`s no final do arquivo:
#   export GOOGLE_APPLICATION_CREDENTIALS=~/.config/claude/rj-ia-desenvolvimento-sms.json
#   export CLAUDE_CODE_USE_VERTEX=1
#   export ANTHROPIC_VERTEX_PROJECT_ID=rj-ia-desenvolvimento
#   export ANTHROPIC_VERTEX_REGION=us-east5
# Fecha e salva [Ctrl+O -> Enter]
# Agora cola os mesmos `export`s como comandos, pra ativar já nessa sessão
# Cria pasta de skills que vamos precisar
$ mkdir -p ~/.claude/skills
# Copia a skill de migração, de onde você a colocou para a pasta correta
$ cp -r migrate-v1-to-v3 ~/.claude/skills/migrate-v1-to-v3
# Aqui, já esteja no diretório onde os repositórios v1 e v3 se localizam
$ claude
# (...configuração de tema, aviso de segurança...)
```

Pronto!


## Migração
Para migrar, esteja dentro da TUI do Claude (executando `$ claude` na pasta
mãe dos respositórios).

Prompts:

> migrate flow (nome do flow)

Ex.: "migrate flow diario_oficial_rj"

> [!WARNING]
> A migração automática por si só não garante que o flow funciona! Sempre
> confira o código gerado e execute o flow localmente antes de pedir aprovação
> de um PR de migração.

Encontrou uma situação que o Claude não migra corretamente? Você pode editar o
arquivo de skill dele para que migrações futuras sejam corretas. Tenha em mente
que ele pode não seguir totalmente à risca as instruções escritas, ou na ordem
correta.

Iteração:
* Editar o SKILL.md
* Se estiver editando fora da pasta do Claude, é necessário copiá-la novamente:
  `$ cp -r migrate-v1-to-v3 ~/.claude/skills/migrate-v1-to-v3`

Ao adicionar / editar conteúdo na skill, lembre-se de ser o mais enxuto possível.
O custo de execução é por tokens, então quanto menos palavras e mais sucinto,
melhor.
