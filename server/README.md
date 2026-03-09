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

### Pré-instalação
...


### Deploy
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
```


### Deploy
Para subir o servidor:

```sh
$ docker compose up infisical-backend --build
```

### Pós-instalação

#### Conta de administrador
Na primeira execução, ele abre a tela de cadastro da conta de administrador.
Não temos um servidor SMTP, então os emails não precisam ser reais; ex. `admin@cit.local`
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


## TODO
- Worker Pool via Google Cloud Run
  - [Google Cloud Run Worker Guide](https://docs.prefect.io/integrations/prefect-gcp/gcp-worker-guide) \
    (acho que não precisa do Worker no Docker Compose afinal de contas...)
- VM
  - One-liner de importação do repositório?
  - Domínio configurado
    - HTTPS
- Funções de auxílio todas dos flows do Prefect
  - Acesso a BigQuery, Cloud Storage, ...
  - dbt
    - Ver se dá pra printar logs mais descritivos do dbt dessa vez :/
- Autenticação\
  SMTR usa [Authentik](https://goauthentik.io/), self-hosted
- Nginx (ou equivalente)
  - Proxy reverso separando Infisical / Prefect / autenticação
- Identificação de usuário logado no Prefect (opcional) (queria muito)
  - Ou gambiarra com `<iframe>`\
    (mais complicado do que parece, acho que perderia URL trocadas em transição de página)
  - Ou customizar imagem do container(?)\
    (meio que travaria a versão "pra sempre")
  - Aproveitar que já tô mexendo na encanação e colocar um
`* { transition: 50ms !important }` ou algo parecido, as transições por padrão
são de **250 ms** sem motivo aparente?? [baldes de tempo jogado fora à toa](https://github.com/PrefectHQ/prefect/blob/2b1c62f299d880e471c734a6c1fe6c18de6dc3e0/ui/src/pages/AppRouterView.vue#L142)
