# Servidor pipelines

Inclui:
* Servidor Prefect UI\
  Não inclui os workers!
* Servidor Infisical


## Shared
Os serviços de Postgres e Redis são compartilhados entre o Prefect e o Infisical.
Em desenvolvimento, você deve subí-los antes dos outros:

```sh
$ docker compose up shared-postgres shared-redis --build
```


## Prefect

### Deploy do servidor
```sh
$ docker compose up prefect-server prefect-services --build
```

```sh
$ docker compose up prefect-worker --build
```

### Pós-instalação
É necessário configurar um Work Pool. Vá em "Work Pools" → "Create Work Pool"
→ "Google Cloud Run V2". Fora o nome do Work Pool, nada precisa ser preenchido
de imediato, pois pode ser alterado posteriormente.

É interessante configurar um limite de flows paralelos ("Flow Run Concurrency").

... JSON de credenciais ...

#### Work Pool



## Infisical

### Pré-instalação
Crie um arquivo `.env` baseado no arquivo `.env.example`.

Para as variáveis `ENCRYPTION_KEY` e `AUTH_SECRET`, você pode executar os
seguintes comandos:<sup>[[Infisical Docs]](https://infisical.com/docs/self-hosting/configuration/envars#general-platform)</sup>
```sh
# ENCRYPTION_KEY
$ openssl rand -hex 16
# AUTH_SECRET
$ openssl rand -base64 32
# AUTHENTIK_SECRET_KEY
$ openssl rand -base64 60 | tr -d '\n'
```


### Deploy
Para subir o servidor:

```sh
$ docker compose up infisical-backend --build
```

### Pós-instalação

#### Conta de administrador
Na primeira execução, ele abre a tela de cadastro da conta de administrador.
Não temos um servidor SMTP, então os emails não precisam ser reais; ex. `admin@teste.local`
A senha tem mínimo de caracteres e requer letras e dígitos pelo menos.

#### Configurações relevantes
Em "Server Console" > "General", é interessante desabilitar "Allow user signups".

Na página da organização:
* Na aba "Settings", é interessante modificar o nome (por padrão, "Admin Org") para
algo mais relevante.
* Na aba "Overview", é possível criar os projetos (de tipo "Secrets Management") para
conter os secrets.

Na página de projeto (criado acima), na aba "Settings", é possível obter o ID
do projeto (via botão "Copy Project ID"), a ser usado na variável de ambiente
`INFISICAL_PROJECT_ID`.

Na página de projeto, na aba "Access Control", em "Service Tokens", é possível criar
os tokens de acesso para uso pelo Prefect pelo botão "Create Token". Lembre-se
de configurar o prazo de validade do token que, por padrão, é 1 dia. É provavelmente
prudente não permitir a escrita de secrets pelo token.

Ao criar o token, uma janelinha irá abrir com seu valor. Deve ser algo parecido com
`st.00000000-0000-0000-0000-...`. Esse valor é usado na variável de ambiente
`INFISICAL_TOKEN`. Ele só é mostrado uma vez, então se for perdido, outro terá
que ser criado em seu lugar.


#### Conta de usuários
Para cadastrar outro usuário, é necessário ir na página da organização,
na aba "Access Control", e clicar no botão "Invite Users to Organization".
Um ou múltiplos endereços de email (que, novamente, não precisam ser reais)
podem ser inseridos, lembrando de selecionar o projeto a qual eles pertencem.

Como a instância do Infisical não possui SMTP configurado, um popup irá lhe
informar um URL onde o usuário pode fazer o próprio cadastro. Será algo
parecido com: `https://.../signupinvite?token=...&to=usuario.teste@infisical.local&organization_id=...`

Importante ressaltar: ao clicar em "Confirm Email", o token será invalidado;
se o usuário não completar o cadastro na mesma sessão, outro link terá que ser
gerado.

A senha de usuário, contraintuitivamente, possui mais restrições do que a senha
de administrador. Além de mínimo de caracteres e obrigatoriedade de letras e
dígitos, ela também não pode ter aparecido em vazamentos de senhas anteriores.


## Nginx Proxy Manager (NPM)
> [!NOTE]
> As documentações do NPM e do Authentik, aqui, usam `dominio.local` como domínio falso. Este deve ser substituído pelo seu domínio real, ou então por algum equivalente a localhost (adicionado ao arquivo de `hosts`) se você estiver desenvolvendo localmente.

Grande parte dessa documentação teve como base [este tutorial](https://joshrnoll.com/implementing-sso-using-authentik-and-nginx-reverse-proxy-manager/), escrito por Josh Noll.

```sh
$ docker compose up nginx-manager --build
```

Navegue até `http://localhost:81/`. Crie um login para o administrador.

Em seguida, é hora de adicionar os proxies. Em "Proxy Hosts", clique no botão "Add Proxy Host":

> Domain Names: npm.dominio.local \
> Scheme: HTTP, nginx-manager, 81

(você agora pode acessar esse painel de configuração em `http://npm.dominio.local`)

> Domain Names: auth.dominio.local \
> Scheme: HTTP, authentik-server, 9000

> Domain Name: pipelines.dominio.local \
> Scheme: HTTP, prefect-server, 4200 \
> Aqui, é necessário também clicar na engrenagem ⚙️ e adicionar o conteúdo do arquivo `./settings/pipelines.nginx.conf` na caixa de texto. Essa é a configuração que confere se o usuário possui login ativo e, se sim, permite acesso ao Prefect.

> Domain Names: infisical.dominio.local \
> Scheme: HTTP, infisical-backend, 8080


## Authentik
```sh
$ docker compose up authentik-server authentik-worker --build
```

Ele demora bastante a subir na primeira execução.

Navegue até `http://localhost:9000/if/flow/initial-setup/`. Crie um login para o administrador. Em seguida, clique no botão "Admin interface".

Barra lateral, "Applications" > "Applications", botão "Create with Provider":

> Nome "nginx", slug "nginx", policy ANY \
> Proxy Provider \
> Authorization flow "implicit" \
> Forward auth (single application): "http://pipelines.dominio.local" \
> Advanced flow settings > Authentication flow "default-authentication..."

Barra lateral, "Applications" > "Outposts", clique em editar o já criado "Embedded Outpost". ("ah, eu apaguei": Create > Type "Proxy", Integration "----")

Adicione o nginx à lista de Selected Applications. Abra "Advanced Settings" e configure `authentik_host` como "http://auth.dominio.local/" (por padrão é "http://localhost:9000").


## TODO
- Worker Pool via Google Cloud Run
  - [Google Cloud Run Worker Guide](https://docs.prefect.io/integrations/prefect-gcp/gcp-worker-guide) \
- VM
  - One-liner de importação do repositório?
  - Domínio configurado
    - HTTPS
- Funções de auxílio todas dos flows do Prefect
  - Acesso a BigQuery, Cloud Storage, ...
  - dbt
    - Ver se dá pra printar logs mais descritivos do dbt dessa vez :/
- Identificação de usuário logado no Prefect (opcional) (queria muito)
  - Ou gambiarra com `<iframe>`\
    (mais complicado do que parece, acho que perderia URL trocadas em transição de página)
  - Ou customizar imagem do container(?)\
    (meio que travaria a versão "pra sempre")
  - Aproveitar que já tô mexendo na encanação e colocar um
`* { transition: 50ms !important }` ou algo parecido, as transições por padrão
são de **250 ms** sem motivo aparente?? [baldes de tempo jogado fora à toa](https://github.com/PrefectHQ/prefect/blob/2b1c62f299d880e471c734a6c1fe6c18de6dc3e0/ui/src/pages/AppRouterView.vue#L142)
