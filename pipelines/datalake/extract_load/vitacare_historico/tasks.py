from pipelines.utils.prefect import authenticated_task as task
from pipelines.utils.logger import log

from google.cloud import bigquery

@task
def get_vitacare_unit_info() -> list[dict]:
    query = """
        select distinct
            id_cnes as cnes,
            area_programatica as ap,
            nome_limpo
        from `rj-sms.saude_dados_mestres.estabelecimento`
        where prontuario_versao = 'vitacare'
        and prontuario_episodio_tem_dado = 'sim'
        and id_cnes is not null
    """

    log("Buscando unidades Vitacare no BigQuery")
    try:
        client = bigquery.Client()
        rows = client.query(query).result()

        result = [
            {
                "cnes": row.cnes,
                "ap": row.ap,
                "unit_name": row.nome_limpo,
            }
            for row in rows
        ]

        log(f"Encontradas {len(result)} unidades esperadas no BigQuery")
        return result

    except Exception as exc:
        log(f"Erro ao buscar unidades esperadas no BigQuery: {exc}", level="error")
        raise