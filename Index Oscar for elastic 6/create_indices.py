import os
import json
from delete_indices import delete_indices
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient


def generate_docs_mapping() -> dict:
    """
    Document mapping, contains document weblink and tags
    """
    mapping = {
        "mappings": {
            "doc": {
                "properties": {
                    "n_sents": {"type": "integer"},
                    "tags": {"type": "text"},
                    "link": {"type": "text"},
                    "sents_ids": {"type": "integer"}
                }
            }
        },
        "settings": {
            "number_of_replicas": 0,
            "refresh_interval": '-1',
        }
    }
    print("generated docs mapping")
    return mapping


def generate_sentences_mapping() -> dict:
    """
    Sentence mapping, contains prev/next sentence id, document id, text and russian analyser.
    """
    cpu_count = os.cpu_count()
    num_shards = cpu_count - 1
    mapping = {
        "mappings": {
            "sent": {
                "properties": {
                    "prev_id": {"type": "integer"},
                    "next_id": {"type": "integer"},
                    "doc_id": {"type": "integer"},
                    "n_words": {"type": "integer"},
                    "sentence": {"type": "text", "analyzer": "russian"}
                }
            }
        },
        "settings": {
            "number_of_shards": num_shards,
            "number_of_replicas": 0,
            "refresh_interval": '-1',
        }
    }
    print("generated sentence mapping")
    return mapping


def create_indices(elastic_creds: str = "user:pass", elastic_url: str = "localhost:9200", docs_index_name: str = "test.docs", sentences_index_name: str = "test.sentences"):
    es = Elasticsearch([f"http://{elastic_creds}@{elastic_url}"], timeout=100, max_retries=100, retry_on_timeout=True)
    es.ping()
    delete_indices(elastic_creds, elastic_url, docs_index_name, sentences_index_name)
    es.indices.create(index=docs_index_name, body=generate_docs_mapping())
    es.indices.create(index=sentences_index_name, body=generate_sentences_mapping())


if __name__ == "__main__":
    create_indices(elastic_creds="user:pass", elastic_url="localhost:9200", docs_index_name="test.docs", sentences_index_name="test.sentences")
