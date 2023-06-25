import os
import json
from delete_indeces import delete_indeces
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
                    "link": {"type": "text"}
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


def create_indices():
    es = Elasticsearch()
    es.ping()
    delete_indeces()
    es.indices.create(index='oscar.docs', body=generate_docs_mapping())
    es.indices.create(index='oscar.sentences', body=generate_sentences_mapping())


if __name__ == "__main__":
    create_indices()
