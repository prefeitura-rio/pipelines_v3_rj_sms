# Servidor pipelines

Inclui:
* Nginx Proxy Manager
* Servidor Authentik
* Servidor Prefect UI\
  Não inclui os workers!
* Servidor Infisical


## Pré-instalação
Crie um arquivo `.env` baseado no arquivo `.env.example`.

Você pode executar os seguintes comandos para gerar chaves aleatórias:<sup>[[Infisical Docs]](https://infisical.com/docs/self-hosting/configuration/envars#general-platform)</sup>
```sh
# ENCRYPTION_KEY
$ openssl rand -hex 16
# AUTH_SECRET
$ openssl rand -base64 32
# AUTHENTIK_SECRET_KEY
$ openssl rand -base64 60 | tr -d '\n'
```

Se seu sistema estará em um servidor, i.e. não será exclusivamente na máquina local e acessível via localhost, então também configure as seguintes variáveis. Nessa documentação, usaremos "dominio.local" como domínio falso de exemplo.

```sh
# Endereço público do Infisical:
SITE_URL="https://infisical.dominio.local"
# Endereço e porta públicos do Prefect:
PREFECT_SERVER_API_HOST="pipelines.dominio.local"
PREFECT_SERVER_API_PORT="443"
```

Em desenvolvimento local, é possível editar o arquivo de `hosts` da máquina para criar subdomínios que podem ajudar a seguir o passo a passo; ex. "127.0.0.1 → pipelines.dominio.local".


Algumas referências úteis:
* [Install Docker Engine on Debian](https://docs.docker.com/engine/install/debian/)


## Shared
Os serviços de Postgres e Redis são compartilhados entre o Prefect e o Infisical.
Em desenvolvimento, você deve subí-los antes dos outros:

```sh
$ docker compose up -d shared-postgres shared-redis --build
```


## Nginx Proxy Manager (NPM)
> [!NOTE]
> As documentações do NPM e do Authentik, aqui, usam `dominio.local` como domínio falso. Este deve ser substituído pelo seu domínio real, ou então por algum equivalente a localhost (adicionado ao arquivo de `hosts`) se você estiver desenvolvendo localmente.

Grande parte dessa documentação teve como base [este tutorial](https://joshrnoll.com/implementing-sso-using-authentik-and-nginx-reverse-proxy-manager/), escrito por Josh Noll.

```sh
$ docker compose up -d nginx-manager --build
```

Para abrir a UI do NPM, é necessário acessar a porta 81. Em situações em que as únicas portas disponíveis na VM são 80 e 443, é necessário fazer uma configuração temporária do sistema primeiro:

1. Vá em `docker-compose.yml`, sob `nginx-manager` → `ports`, troque `"80:80"` por `7080:80`;
2. Reinicie o container: `docker compose down nginx-manager`, `docker compose up -d nginx-manager --build`;
3. Siga o [guia de instalação do nginx](https://nginx.org/en/linux_packages.html#Debian) para instalá-lo na máquina;
4. Copie o pequeno conteúdo do arquivo `settings/setup.nginx.conf` para `/etc/nginx/conf.d/default.conf` da máquina (fora do docker!!) e reinicie o serviço do Nginx (`systemctl restart nginx`).

Navegue até "http://&lt;IP do servidor&gt;:81/" (se estiver rodando local, "localhost:81"), ou :80 caso tenha feito a configuração de Nginx acima. Crie um login para o administrador.

> [!NOTE]
> Se, para a conta de administrador, você escolher um endereço de email fake – como "admin@npm.local" – será necessário criar uma segunda conta de usuário com um endereço de email real para poder emitir certificados de HTTPS. É possível fazer isso na aba "Users".

Em seguida, é hora de adicionar os proxies. Em "Proxy Hosts", clique no botão "Add Proxy Host":

> Domain Names: npm.dominio.local \
> Scheme: HTTP, nginx-manager, 81

A partir de agora, você pode acessar esse painel de configuração em "http://npm.dominio.local:80". Após confirmar que o serviço está disponível nesse endereço, você pode (e deve!) desfazer a modificação temporária anterior:

1. Pare e desative o Nginx temporário com `systemctl stop nginx` e `systemctl disable nginx`;
2. Vá em `docker-compose.yml`, sob `nginx-manager` → `ports`, troque `"7080:80"` por `80:80`;
3. Reinicie o container: `docker compose down nginx-manager`, `docker compose up -d nginx-manager --build`;

> [!NOTE]
> A recomendação por desabilitar o Nginx da máquina pressupõe que sua máquina expõe as portas 80 e 443 para a Internet por padrão. Se esse não é o caso, configurações adicionais específicas serão necessárias. Caso você não tenha precisado da configuração de Nginx temporária, é altamente recomendável que você configure sua máquina/firewall para não permitir acessos da Internet em qualquer porta.

Para cada um dos Proxy Hosts configurados (incluindo o primeiro, acima), vá na aba "SSL", selecione "Request a new Certificate" no dropdown, selecione "Force SSL", "HSTS Enabled" e "HTTP/2 Support".¹

> Domain Names: auth.dominio.local \
> Scheme: HTTP, authentik-server, 9000

> Domain Name: pipelines.dominio.local \
> Scheme: HTTP, prefect-server, 4200 \
> Aqui, é necessário também clicar na engrenagem ⚙️ e adicionar o conteúdo do arquivo `./settings/pipelines.nginx.conf` na caixa de texto. Essa é a configuração que confere se o usuário possui login ativo e, se sim, permite acesso ao Prefect.

> Domain Names: infisical.dominio.local \
> Scheme: HTTP, infisical-backend, 8080


¹ "Force SSL" redireciona tráfego HTTP para HTTPS, tornando impossível o acesso via porta 80. Contudo, nessa configuração, ainda é possível ter uma conexão não encriptada via HTTPS – então habilitamos "HSTS", que bloqueia conexões que não possuam criptografia habilitada.


## Authentik
```sh
$ docker compose up -d authentik-server authentik-worker --build
```

Ele demora bastante a subir na primeira execução – tipo literalmente uns 5 minutos. Se você quiser, suba pela primeira vez sem o `-d` para ver a hora em que os logs de inicialização se acalmam e, no fim dos passos seguintes de configuração, Ctrl+C e bote pra rodar de novo.

Navegue até "https://auth.dominio.local/if/flow/initial-setup/" (ou "localhost:9000/..."). Crie um login para o administrador. É possível que você receba uma página de "Not Found"; se isso aconteceu, veja § "Acesso inicial ao Authentik 'not found'" no fim do README.

Em seguida, clique no botão "Admin interface". Barra lateral, "Applications" → "Applications", botão "Create with Provider":

> Nome "nginx", slug "nginx", policy ANY \
> Proxy Provider \
> Authorization flow "implicit" \
> Forward auth (single application): "https://pipelines.dominio.local" \
> Token validity: "days=365" \
> Advanced flow settings → Authentication flow "default-authentication..."

Barra lateral, "Applications" → "Outposts", clique em editar o já criado "Embedded Outpost". ("ah, eu apaguei": Create → Type "Proxy", Integration "----")

Adicione o nginx à lista de Selected Applications. Abra "Advanced Settings" e garanta que `authentik_host` é "https://auth.dominio.local/" (pode ser ex. "http://localhost:9000").

Você pode criar usuários em "Directory" → "Users" → "New User". Pode ser interessante colocar o usuário em uma pasta que não só "user"; p.ex. "user/organizacao". Depois de criado, é possível configurar uma senha para o usuário clicando na setinha ao seu lado na lista e, em seguida, em "Set password". O usuário pode trocar a própria senha fazendo login diretamente em "http://auth.dominio.local", clicando na engrenagem, e em "Change password".

Para que o Worker consiga acessar a API do Prefect, ele vai precisar se autenticar no Authentik; para isso, você precisa criar uma conta de serviço. "Directory" → "Users" → "New Service Account", Username "prefect-worker-service-account" (ou algo parecido), desmarque "Expiring" – queremos que a conta seja permanente. Em seguida, em "Directory" → "Tokens and App passwords" → "Create", Identifier "prefect-worker-app-password" (ou algo parecido), User criado acima, Intent "App password", e desmarque "Expiring" novamente.


## Prefect

### Deploy do servidor
Se você estiver executando localmente, então basta:

```sh
$ docker compose up -d prefect-server prefect-services --build
```

Contudo, em um servidor, é importante configurar o domínio e porta da API que o cliente vai tentar acessar. Isso pode ser feito pelo arquivo `.env` ou direto na execução do container. Por exemplo:

```sh
$ PREFECT_SERVER_API_HOST="pipelines.dominio.local" \
PREFECT_SERVER_API_PORT="443" \
docker compose up -d prefect-server prefect-services --build
```

Navegue até "http://pipelines.dominio.local". Se tudo correu bem, você deve ser redirecionado para uma tela de login em "http://auth.dominio.local".


### Pós-instalação
> Parte desse tutorial tem como base o [guia de intgração Prefect—Cloud Run](https://docs.prefect.io/integrations/prefect-gcp/gcp-worker-guide)

É necessário configurar um Work Pool. Vá em "Work Pools" → "Create Work Pool" → "Google Cloud Run V2".

* O nome que você escolher será posteriormente usado para identificar essa Work Pool; eu recomendaria algo simples, `[a-z\-]`, como "gcp-wp".
* É interessante configurar um limite de flows paralelos ("Flow Run Concurrency").
* Em "GcpCredentials", clique no botão de "Add +". Block Name: "prefect-cloud-run" (aqui é livre, mas faz sentido ser isso, né?); Service Account Info: copie e cole o JSON da conta de serviço. Botão de "Create".
* Em "Service Account Name", cole o email (normalmente @&lt;projeto&gt;.iam.gserviceaccount.com) da conta de serviço.
* Clique no botão no final da página para criar a Work Pool. Você talvez precise marcar os campos "Prefect API Key Secret" e "Prefect API Auth String Secret" como nulos para conseguir fazer isso.

Vá ao painel de administrador do Authentik. Em "Applications" → "Providers" → "Provider for nginx" → "Authentication", copie o campo "Client ID". Este será substituído no comando abaixo, como `<CLIENT_ID>`. Em "Directory" → "Tokens and App passwords", copie a App Password criada anteriormente, com nome "prefect-worker-app-password". Esta será `<APP_PASSWORD>` no comando abaixo.

Em seguida, execute o seguinte comando no seu terminal:

```sh
$ curl -L "https://auth.dominio.local/application/o/token/" \
  --header "Host: auth.dominio.local" \
  --header "Content-Type: application/x-www-form-urlencoded" \
  --data "grant_type=client_credentials&client_id=<CLIENT_ID>&username=prefect-worker-service-account&password=<APP_PASSWORD>&scope=profile"
```

Ele deve te retornar algo como:

```json
{
  "access_token": "xxxxxxxx.xxxxxxxx.xxxxxxxx",
  "token_type": "Bearer",
  "scope": "profile",
  "expires_in": 31536000,
  // ... //
}
```

O campo `access_token` é o "JWT", o token que o Prefect Worker terá que usar para passar pela tela de login do Authentik. Na configuração do Authentik, definimos que os tokens desse "client" durariam 365 dias (31,536,000 segundos); isso pode ser modificado na interface de administrador do Authentik, em "Applications" → "Providers" → "Provider for nginx" → "Token validity". Outra requisição terá que ser feita para obter o JWT com novo prazo de validade.

A seguir, vamos fazer deploy do Prefect Worker no Google Cloud Run. Se você preferir, ao invés dos comandos a seguir, também é possível fazer essa etapa através do [site do Google Cloud Run](https://console.cloud.google.com/run/services):
```sh
$ gcloud auth login
# "<PROJECT_ID>" é o nome do seu projeto no Google Cloud
$ gcloud config set project <PROJECT_ID>
# "<BEARER_TOKEN>" é o JWT obtido acima, através de `curl`
# "<SERVICE_ACCOUNT>" o nome da conta de serviço
# "<WORK_POOL_NAME>" o nome da Work Pool que você criou acima
$ gcloud run deploy prefect-worker --image=prefecthq/prefect-gcp:latest \
--set-env-vars='PREFECT_API_URL="https://pipelines.dominio.local"' \
--set-env-vars='PREFECT_API_KEY="<BEARER_TOKEN>"' \
--service-account <SERVICE_ACCOUNT>@<PROJECT_ID>.iam.gserviceaccount.com \
--no-cpu-throttling \
--min-instances 1 --max-instances 20 \
--startup-probe httpGet.port=8080,httpGet.path=/health,initialDelaySeconds=100,periodSeconds=20,timeoutSeconds=20 \
--args "prefect","worker","start","--install-policy","never","--with-healthcheck","-p","<WORK_POOL_NAME>","-t","cloud-run-v2"
```

...


## Infisical

### Deploy
Para subir o servidor:

```sh
$ docker compose up -d infisical-backend --build
```

Ele pode demorar alguns minutos pra subir; execute sem a flag `-d` inicialmente para ver o output do container. Ele só realmente iniciou quando diz:

> Welcome back! \
>  \
> To access Infisical Administrator Panel open \
> http://localhost:8080/admin \
>  \
> To access Infisical server \
> http://localhost:8080


### Pós-instalação

#### Conta de administrador
Na primeira execução, ele abre a tela de cadastro da conta de administrador.
Não temos um servidor SMTP, então os emails não precisam ser reais; ex. `admin@test.local`
A senha tem mínimo de caracteres e requer letras e dígitos pelo menos.

#### Configurações relevantes
Em "Server Console" → "General", é interessante desabilitar "Allow user signups".

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
Para cadastrar outro usuário, é necessário ir na página da organização, na aba "Access Control", e clicar no botão "Invite Users to Organization". Um ou múltiplos endereços de email (que, novamente, não precisam ser reais) podem ser inseridos, lembrando de selecionar o projeto a qual eles pertencem.

Como a instância do Infisical não possui SMTP configurado, um popup irá lhe informar um URL onde o usuário pode fazer o próprio cadastro. Será algo parecido com: `https://.../signupinvite?token=...&to=usuario.teste@infisical.local&organization_id=...`.

Se o URL possuir "`localhost`" e você tiver feito deploy para um domínio real, será necessário trocar o domínio manualmente. Em teoria, você deve alterar a variável de ambiente `SITE_URL` ("http://localhost:8080" por padrão) para o URL correto (p.ex. "https://infisical.dominio.local"), mas não funcionou comigo 🤷🏻‍♀️.

Importante ressaltar: ao clicar em "Confirm Email", o token será invalidado; se o usuário não completar o cadastro na mesma sessão, outro link terá que ser gerado.

A senha de usuário, contraintuitivamente, possui mais restrições do que a senha de administrador. Além de mínimo de caracteres e obrigatoriedade de letras e dígitos, ela também não pode ter aparecido em vazamentos de senhas anteriores.


## Troubleshooting

### A configuração salva no Postgres está errada
> Fiz besteira configurando o container X (p.ex. apaguei a conta de admin do Infisical) e meus erros foram calcificados no Postgres. Não quero apagar o volume inteiro, porque ele é compartilhado e eu perderia a configuração dos outros containers também :(

**R:** Você pode apagar somente a database que está incorreta. Lembre de `docker compose down` o container que usa a database incorreta primeiro. Com o container do Postgres executando, faça:
```sh
$ docker exec -it server-shared-postgres-1 /bin/bash
(...):/$ psql -U cit_pipelines postgres
postgres=$ drop database XXXXXXX;
DROP DATABASE
postgres=$ create database XXXXXXX;
CREATE DATABASE
postgres=$ grant all privileges on database "XXXXXXX" to "cit_pipelines";
GRANT
```

Onde `XXXXXXX` é o nome da database; eles são intuitivos mas, para referência, estão na variável `POSTGRES_MULTIPLE_DATABASES` da configuração no Docker Compose do Postgres.


### 502 Bad Gateway!!
**R:** Isso provavelmente significa que o serviço ao qual o NPM aponta não existe, ou ainda não terminou de subir. O docker para o serviço desejado ou (ainda) não está rodando, ou tem outro nome, etc etc.


### "Default Site" / "Congratulations!"
**R:** O serviço ao qual o NPM aponta ou não está executando ou está desabilitado. Se você já tinha configurado o serviço antes de subí-lo, tente desabilitar e reabilitar o proxy host, ou encontrar problemas na configuração. Lembrando que para serviços dentro de um mesmo docker compose, é necessário usar o nome do serviço (ex. "prefect-server") ao invés de "localhost".


### Internal Server Error configurando HTTPS
**R:** No meu caso, isso era porque eu estava usando um endereço de email inválido – p.ex. "admin@npm.local". Ao reiniciar o container sem a flag `-d` (para ver o output) e retentando, vi esse erro:

> [00/00/0000] [00:00:00 AM] [Express  ] › ⚠  warning   Saving debug log to /data/logs/letsencrypt.log \
> Unable to register an account with ACME server. The ACME server believes admin@npm.local is an invalid email address. Please ensure it is a valid email and attempt registration again.

Simplesmente criar uma conta nova de administrador com email válido, e então tentar ativar os certificados a partir dela, foi suficiente para resolver.


### Acesso inicial ao Authentik "not found"
> Subi o docker compose do Authentik e, depois de esperar tempo demais para ele iniciar, me deparo com um "Not Found" em `/if/flow/initial-setup/`!

**R:** Esse problema aparentemente é comum. Vou ser honesto e dizer que eu não sei como eu resolvi. As sugestões online parecem se resumir a "tente derrubar e subir novamente os containers do Authentik" (não funcionou pra mim). Encontrei uma pessoa online dizendo que era só configurar variáveis de ambiente antes de subir o container pela primeira vez – parte do [automated install](https://docs.goauthentik.io/install-config/automated-install) – configurando email e senha para a conta de administrador. Algo tipo assim:

```sh
$ AUTHENTIK_BOOTSTRAP_EMAIL="...@..." AUTHENTIK_BOOTSTRAP_PASSWORD="..." \
docker compose up -d authentik-server authentik-worker
```

(lembrando que isso iria requerer antes apagar a database do Authentik, descrito acima em § "A configuração salva no Postgres está errada")

E, em seguida, ao invés de navegar para `/if/flow/initial-setup/`, o caminho seria `/if/flow/default-authentication-flow/?next=%2F`. Mas, quando eu tentei isso, ao fazer login, recebi erro de usuário e senha incorretos, e o caminho `/if/flow/initial-setup/` estava funcionando. 🤷🏻‍♀️


### Prefect Worker parou de funcionar!!
**R:** Se você está tendo esse erro ~1 ano após o deploy inicial, então é bem possível que o JWT do Worker tenha expirado. Você pode conferir a data de expiração pelo website [jwt.io](https://www.jwt.io/), colando o token no campo e observando a data na propriedade `exp`.

Caso ele esteja expirado, siga os passos para obtenção do JWT em § Prefect > Pós-instalação; i.e.:

```sh
$ curl -L "https://auth.dominio.local/application/o/token/" \
  --header "Host: auth.dominio.local" \
  --header "Content-Type: application/x-www-form-urlencoded" \
  --data "grant_type=client_credentials&client_id=<CLIENT_ID>&username=prefect-worker-service-account&password=<APP_PASSWORD>&scope=profile"
```

Com o novo `access_token` recebido, substitua a variável de ambiente `PREFECT_API_KEY` no deploy do Worker, [na página de configuração do Google Cloud Run](https://console.cloud.google.com/run/services).


### `docker-credential-gcloud` not installed or not available in PATH
**R:** Experimente apagar (ou renomear) o arquivo `~/.docker/config.json`.


## TODO
- Worker Pool via Google Cloud Run ([guia](https://docs.prefect.io/integrations/prefect-gcp/gcp-worker-guide))
  - Conta de serviço pelo Authentik?
- Login via SSO do Google (Authentik, Infisical)
- Funções de auxílio todas dos flows do Prefect
  - Acesso a Cloud Storage, ...
  - dbt
    - Ver se dá pra printar logs mais descritivos do dbt dessa vez :/
- Identificação de usuário logado no Prefect (opcional) (queria muito)
  - Ou gambiarra com `<iframe>`\
    (mais complicado do que parece, acho que perderia URL trocadas em transição de página)
  - Ou customizar imagem do container direto
- WebSocket do Prefect?
