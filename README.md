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

### 4 - Executando flows localmente
> Os chamados "cases" são configurações de flows com parâmetros especificos. Eles são definidos com parâmetros padrão no arquivo `localrun.cases.yaml`. Ao executar localmente, o script `localrun.py` procura o _case_ desejado (definido no arquivo `localrun.selected.yaml`).

Toda vez que criar um flow novo, não se esqueça de incluir suas configurações no arquivo `localrun.cases.yaml`. Este possui os campos:
- `case_slug`: Apelido do caso que serve como identificador.
- `flow_path`: Caminho do módulo python em que o flow está definido. O mesmo usando no import do python.
- `flow_name`: Nome da variável que o flow está armazenado dentro do módulo definido em `flow_path`.
- `params`: Valores padrão de parâmetros que serão utilizados na execução do flow.

Por exemplo:

```yaml
cases:
  - case_slug: "report-endpoint-health"
    flow_path: "pipelines.reports.endpoint_health.flows"
    flow_name: "disponibilidade_api"
    params:
      environment: "dev"
```

Para definir qual flow você gostaria de executar localmente, crie um arquivo `localrun.selected.yaml` com conteúdo semelhante ao abaixo:

```yaml
selected_case: "nome-do-case"
override_params:
  environment: "dev"
  parametro1: "valor"
  parametro2: "valor"
```

Essa configuração executará o caso com slug `nome-do-case`. Além disso, ela especifica que o parâmetro "environment" terá o valor de "dev" ao invés do valor padrão. Os valores padrão para execução local, novamente, são definidos em `localrun.cases.yaml`.

Antes de executar, confira se seu ambiente está executando com o ambiente correto (python 3.13.x), dentro do `.venv/`. Isso pode ser alterado pela interface do VS Code, no canto inferior direito, ao abrir um arquivo .py.

### 5 - Git Hooks (Pre-push)
Para garantir que o código suba sempre formatado para o repositório, ative o hook de pre-push em sua máquina. Ele executará a formatação e o linter automaticamente a cada `git push`.

```sh
$ uv run pre-commit install
```
> **Nota:** Se você já contribuía com o projeto e possuía hooks configurados para `pre-commit`, limpe a configuração antiga rodando `$ uv run pre-commit uninstall` antes do comando de instalação.

---

## 🧑🏽‍💻 Contribuições ao projeto
Não esqueça de conferir se você está logado no seu ambiente com a sua conta certa do GitHub. Contas externas ao projeto terão restrições, por exemplo, na execução automática de workflows.

1. **Git:**
Sempre abra suas branchs de trabalho no formato `staging/<sua-feature>`; deploys em staging são automáticos para branches iniciando com `staging/`.

PRs e commits devem, quando possível, respeitar a estrutura: `[<PROJETO>] <acao>: <breve descrição>`. Por exemplo, a adição de uma nova fonte para um projeto de previsão do tempo poderia ser `[Clima] feat: adiciona open-meteo`.

2. **Flows:**
Ao criar um flow novo, siga o formato usado pelo repositório: decorator `@flow`, identificação do "dono" do flow (i.e. quem será notificado em caso de erros), nome adequado no formato `categoria: Nome sucinto` (onde "categoria" é um de "Extração", "Migração", "Transformação" ou "Report"), ...

Todo arquivo `flows.py` requer uma variável `_flows` na sua raiz, que informa ao deploy as configurações dos flows sendo definidos. Use a função auxiliar `flow_config()` de `pipelines.utils.prefect` para facilitar essa configuração. Na dúvida, explore outros flows similares pré-existentes e veja como eles estão definidos.

3. **Commits semânticos:**
Utilize ações semânticas convencionais. Por exemplo, `feat:` para novas features; `fix:` para consertos de bugs; `chore:` para tarefas de manutenção em geral; `docs:` para mudanças exclusivamente de documentação; etc.

4. **Formatação:**
Antes de submeter um pull request, lembre-se de usar o linter/formatador automático: `uv run task lint` (para conferir); `uv run task lint-autofix` (para consertar o que for possível automaticamente).

5. Abra um **pull request**  (não é necessário incluir um reviewer).
> - Cada pull request em branch iniciando com `staging/` irá disparar as rotinas do Github que:
>    - Verificam formatação
>    - Registram flows em staging (ambiente de testes)
> - Você acompanha o status destas rotinas na própria página do seu PR
> - Após serem registrados, flows aparecem no servidor Prefect; você pode executá-los por lá

6. Valide no Prefect o funcionamento do flow usando o projeto "staging", sempre com `environment: "dev"`!

> **Obs**: solicite endereço e credenciais do Prefect ao administrador do projeto.

7. Com execução validada, acione o administrador no servidor do Discord do S/CIT.

**Teste e verifique os dados!**
