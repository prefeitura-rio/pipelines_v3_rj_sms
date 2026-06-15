# Migração

Pasta para flows de migração, com nomes prefixados por "Migração:".

Flows de migração são um caso especial de flows de extração de dados, pois são pensados para a transferência de dados estruturados de uma plataforma para outra, com tratamento escasso e genérico, e todas as especificidades passadas por parâmetros ao flow.

Por exemplo, o flow de Google Sheets extrai dados de planilhas para tabelas do BigQuery, independentemente de qual planilha, quais colunas ela contém, etc. Ele *migra* planilhas para o datalake.
