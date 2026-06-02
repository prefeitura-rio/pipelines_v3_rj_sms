# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch, exceptions

from pipelines.utils.cleanup import cleanup_bigquery_name
from pipelines.utils.logger import log


def connect_ES(url: str, user: str, password: str) -> Elasticsearch:
  es = Elasticsearch(
    url,
    basic_auth=(user, password),
    request_timeout=600,
    max_retries=5,
    retry_on_timeout=True,
    http_compress=True,
  )
  try:
    es.info()
    log("Conexão com o Elasticsearch estabelecida.")
  except exceptions.ConnectionError as e:
    log(f"Falha na conexão com o Elasticsearch: {repr(e)}")
    raise e
  return es


def build_ES_query(page_size: int, data_inicial: str, data_final: str):
  rj_ibge = "330455"
  # ElasticSearch tem limite de 10k resultados por requisição
  # > "By default, you cannot use from and size to page through
  #    more than 10,000 hits"
  # [Ref] https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results
  page_size = min(max(1, page_size), 10_000)

  return {
    "size": page_size,
    "query": {
      "bool": {
        "must": [
          {"match": {"codigo_central_reguladora": rj_ibge}},
          {
            "range": {
              "data_solicitacao": {
                "gte": data_inicial,
                "lte": data_final,
                "time_zone": "-03:00",
              }
            }
          },
        ]
      }
    },
  }


def table_name_from_resource(resource: str) -> str:
  """
  Retorna o nome de tabela designada para recurso ("index")
  sendo requisitado no ElasticSearch
  """
  if resource.startswith("solicitacao") or resource.startswith("marcacao"):
    return cleanup_bigquery_name(resource, lowercase=True)
  raise NotImplementedError(f"Não há tabela definida por padrão para '{resource}'")
