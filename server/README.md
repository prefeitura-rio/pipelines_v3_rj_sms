# Servidor pipelines

Inclui:
* Servidor Prefect UI
  Não inclui os workers!
* Servidor Infisical


## Shared
Os serviços de Postgres e Redis são compartilhados entre o Prefect e o Infisical.
Em desenvolvimento, você deve subí-los antes dos outros:

```sh
$ docker compose up shared-postgres shared-redis --build
```


## Prefect

```sh
$ docker compose up prefect-server prefect-services --build
```


## Infisical

```sh
$ docker compose up infisical-backend --build
```
