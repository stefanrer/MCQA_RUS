from elasticsearch import Elasticsearch


def delete_indeces():
    es = Elasticsearch()
    es.ping()
    if es.indices.exists(index='oscar.docs'):
        es.indices.delete(index='oscar.docs')
    if es.indices.exists(index='oscar.sentences'):
        es.indices.delete(index='oscar.sentences')


if __name__ == "__main__":
    delete_indeces()