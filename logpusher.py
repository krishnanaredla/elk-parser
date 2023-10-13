import eland as ed
from elasticsearch import Elasticsearch

ed.csv_to_eland(
     "result/sample.log_structured.csv",
     es_client=Elasticsearch(hosts='http://localhost:9200', http_auth=('elastic', 'mangena')),
     es_dest_index='sample',
     es_refresh=True,
     index_col=0
)