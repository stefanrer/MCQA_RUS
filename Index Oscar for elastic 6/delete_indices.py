from elasticsearch import Elasticsearch
import certifi
from ssl import create_default_context


def delete_indices(elastic_creds: str = "user:pass", elastic_url: str = "localhost:9200", docs_index_name: str = "test.docs", sentences_index_name: str = "test.sentences"):
    context = create_default_context(cafile=certifi.where())
    context.check_hostname = False
    es = Elasticsearch([f"http://{elastic_creds}@{elastic_url}"], timeout=100, max_retries=100, retry_on_timeout=True)
    es.ping()
    if es.indices.exists(index=docs_index_name):
        es.indices.delete(index=docs_index_name)
    if es.indices.exists(index=sentences_index_name):
        es.indices.delete(index=sentences_index_name)


if __name__ == "__main__":
    delete_indices(elastic_creds="user:pass", elastic_url="localhost:9200", docs_index_name="test.docs", sentences_index_name="test.sentences")