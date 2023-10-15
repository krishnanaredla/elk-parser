from elasticsearch.helpers import parallel_bulk
from collections import deque
from typing import Any, Dict, Generator, List, Tuple, Union
from elasticsearch import Elasticsearch
import uuid
import pandas as pd

DEFAULT_CHUNK_SIZE = 10000
DEFAULT_THREAD_COUNT = 4


def action_generator(
    pd_df: pd.DataFrame,
    es_dest_index: str,
) -> Generator[Dict[str, Any], None, None]:
    for row in pd_df.iterrows():
        values = row[1].to_dict()
        action = {
            "_op_type": "update",
            "_index": es_dest_index,
            "_id": str(uuid.uuid4()),
            "_source": {"doc": values, "doc_as_upsert": True},
        }
        yield action


def pushData(
    es_client: Union[str, List[str], Tuple[str, ...], Elasticsearch],
    pd_df: pd.DataFrame,
    es_dest_index: str,
):
    try:
        deque(
            parallel_bulk(
                client=es_client,
                actions=action_generator(pd_df, es_dest_index),
                chunk_size=int(DEFAULT_CHUNK_SIZE / DEFAULT_THREAD_COUNT),
            ),
            maxlen=0,
        )
    except Exception as e:
        print(e)


df = pd.read_csv("result/Spark_2k.log_structured.csv")
es_client = Elasticsearch(
    hosts="http://localhost:9200", http_auth=("elastic", "mangena")
)
es_index = "logs-pyspark"
pushData(es_client, df, es_index)
