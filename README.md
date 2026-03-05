# 📊 Pipelines **RJ‑SMS**

> Ambiente para extração e carga de dados brutos, em **Python + Prefect v3**, que abastece o Data Lake (**Google Cloud Storage / Google Big Query**) da Secretaria Municipal de Saúde do Rio de Janeiro (**SMS Rio**).

Administrador: [Pedro Marques](https://github.com/TanookiVerde) (S/CIT)


## ⚙️ Configuração de ambiente

### 1 - Ambiente

```sh
$ uv sync && source .venv/bin/activate
```


### 2 - Variáveis de ambiente

Crie um arquivo `.env` na raiz do repositório, baseado no arquivo `.env.example`:

```env
INFISICAL_ADDRESS=xxxxxxxxxxxxxxxx
INFISICAL_TOKEN=xxxxxxxxxxxxxxxx
INFISICAL_PROJECT_ID=xxxxxxxxxxxxxxxx
```

Peça as credenciais do Infisical ao administrador do projeto.


### 3 - VS Code & debugging

A pasta `.vscode/` possui o arquivo `launch.json`. Ele permite que o projeto seja executado e depurado localmente, de dentro do VS Code, através da aba de Debugging. Ao clicar no ícone *Play* (ou a tecla F5), o script `localrun.py` será executado.


### 4 - Configurando a seleção de casos
> Os chamados "cases" são configurações de flows com parâmetros especificos. Eles são definidos com parâmetros padrão no arquivo `localrun.cases.yaml`. Ao executar localmente, o script `localrun.py` procura o _case_ desejado (definido no arquivo `localrun.selected.yaml`).

Crie um arquivo `localrun.selected.yaml` com conteúdo semelhante ao abaixo.

```yaml
selected_case: "nome-do-case"
override_params:
  environment: "dev"
  parametro1: "valor"
  parametro2: "valor"
```

O arquivo acima diz que o caso de slug `nome-do-case` será executado. Além disso, ele especifica que o parâmetro "environment" terá o valor de "dev" ao invés do valor padrão. Os valores padrão, novamente, podem ser definidos em `localrun.cases.yaml`.

Antes de executar, confira se seu ambiente está executando com o ambiente correto (python 3.14.x), dentro do `.venv/`. Isso pode ser alterado pela interface, no canto inferior direito, ao abrir um arquivo .py.


### 5 - Definindo novos casos
Edite o arquivo `localrun.cases.yaml` criando novos casos. São os campos:
- `case_slug`: Apelido do caso que serve como identificador.
- `flow_path`: Caminho do módulo python em que o flow está definido. O mesmo usando no import do python.
- `flow_name`: Nome da variável que o flow está armazenado dentro do módulo definido em `flow_path`.
- `params`: Valores padrão de parâmetros que serão utilizados na execução do flow.

O exemplo abaixo representa a definição de um caso:

```yaml
cases:
  - case_slug: "report-endpoint-health"
    flow_path: "pipelines.reports.endpoint_health.flows"
    flow_name: "disponibilidade_api"
    params:
      environment: "dev"
```


---

## 🤝 Como contribuir com o projeto
Não esqueça de conferir se você está logado no seu ambiente com a sua conta certa do GitHub. Contas externas ao projeto terão restrições, por exemplo, na execução automática de workflows.

1. **Branch:**
  Sempre abra suas branchs de trabalho no formato `staging/<sua-feature>`. O título deve sempre que possível respeitar a estrutura: `[<PROJETO>] <acao>: <breve_descricao>`. Por exemplo, a adição de uma nova fonte para um projeto de previsão do tempo poderia ser `[CLIMA] feat: adiciona open-meteo`.

2. **Commits semânticos:**
  Utilize ações semânticas convencionais. Por exemplo, `feat:` para novas features; `fix:` para consertos de bugs; `chore:` para tarefas de manutenção em geral; `docs:` para mudanças exclusivamente de documentação; etc.

3. Abra um **pull request**  (não é necessário incluir um reviewer).

> - Cada pull request em branch iniciando com `staging/` irá disparar as rotinas do Github que:
>    - Verificam formatação
>    - Registram flows em staging (ambiente de testes)
> - Você acompanha o status destas rotinas na própria página do seu PR
> - Após serem registrados, flows aparecem no servidor Prefect; você pode executá-los por lá

4. Valide no Prefect o funcionamento do flow usando o projeto "staging", sempre com `environment: "dev"`!

> **Obs**: solicite endereço e credenciais do Prefect ao administrador do projeto.

5. Com execução validada, acione o administrador no servidor do Discord do S/CIT.

**Teste e verifique os dados!**
