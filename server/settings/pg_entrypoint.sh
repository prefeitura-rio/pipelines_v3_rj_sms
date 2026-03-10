#!/bin/bash
# Problema: gostaríamos de usar um mesmo container de Postgres
# para tanto o Infisical quanto o Prefect; para isso funcionar,
# cada um deve ter seu próprio dataset; mas a imagem só possui
# um parâmetro para dataset, "POSTGRES_DB"; então aqui criamos
# os datasets ao inicializar, via $POSTGRES_MULTIPLE_DATABASES.

set -e  # Stop on error
set -u  # No unset variables

# [Ref] https://dev.to/nietzscheson/multiples-postgres-databases-in-one-service-with-docker-compose-4fdf
function create_user_and_database() {
	local database=$1
	echo "Creating user and database '$database'"
	psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
		CREATE USER "$database";
		CREATE DATABASE "$database";
		GRANT ALL PRIVILEGES ON DATABASE "$database" TO "$POSTGRES_USER";
EOSQL
}

if [ -n "${POSTGRES_MULTIPLE_DATABASES:-}" ]; then
	echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
	for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
		create_user_and_database $db
	done
	echo "Multiple databases created"
fi
