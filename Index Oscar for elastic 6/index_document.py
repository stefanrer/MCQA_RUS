import logging

import elasticsearch
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import RequestError
from es_wrapper import EsWrapper
from create_indices import create_indices
import urllib3
import json
import gzip
import os
import re
import time
import datetime
import argparse


class Indexer:
    """
    Contains methods for loading ES_Wrapped json sentences into ES Database
    """
    rxBadFileName = re.compile('[^\\w_.-]*', flags=re.DOTALL)
    http = urllib3.PoolManager(maxsize=50)

    def __init__(self, data_dir: str = 'Data', elastic_creds: str = "user:pass", elastic_url: str = "localhost:9200"):
        self.name = "ru_oscar"
        self.total_num_words = 0
        self.corpusSizeInBytes = 0
        self.filenames = []
        self.data_dir = data_dir
        es_logger = logging.getLogger('elasticsearch')
        es_logger.setLevel(logging.WARNING)
        # urllib3_log = logging.getLogger("urllib3")
        # urllib3_log.setLevel(logging.CRITICAL)
        logging.basicConfig(filename=os.path.join("Logs", "indexer.log"), filemode="w", level=logging.INFO)
        self.es_wrapper = EsWrapper()
        # Initialize Elasticsearch connection
        self.es = Elasticsearch([f"http://{elastic_creds}@{elastic_url}"], timeout=50, max_retries=5, retry_on_timeout=True)
        logging.info("success connection to Elastic Search")
        print("success connection to Elastic Search")
        self.numSents = 0
        self.sentID = 0  # current sent id
        self.docID = 0  # current doc id
        self.IDRange = []

    def ping_es(self) -> bool:
        return self.es.ping()

    def iterate_json_line_sentences(self, sentences: list):
        self.numSents = 0
        self.IDRange = []
        prev_last = False
        for i in range(len(sentences)):
            sent = {}
            b_last = (i == len(sentences) - 1)
            sent_total_num_words = len(sentences[i].split())
            self.total_num_words += sent_total_num_words
            sent['n_words'] = sent_total_num_words
            if prev_last:
                prev_last = False
            elif self.numSents > 0:
                sent['prev_id'] = self.sentID - 1
            if not b_last and 'last' not in sent:
                sent['next_id'] = self.sentID + 1
            else:
                prev_last = True
            sent['doc_id'] = self.docID
            sent['sentence'] = sentences[i]
            cur_action = {'_index': self.name + '.sentences',
                          '_type': 'sent',
                          '_id': self.sentID,
                          '_source': sent}
            yield cur_action
            self.IDRange.append(self.sentID)
            self.numSents += 1
            self.sentID += 1

    def index_line(self, tags: list, link: str):
        """
        Store the metadata of the source json_line.
        """
        if self.docID % 5000 == 0 and self.docID != 0:
            logging.info(f'Indexed {self.docID} documents')
            print(f'Indexed {self.docID} documents')
        meta = {'tags': tags, 'link': link, 'n_sents': self.numSents, 'sents_ids': self.IDRange}
        try:
            self.es.index(index=self.name + '.docs',
                          id=self.docID,
                          doc_type="doc",
                          body=meta)
        except RequestError as err:
            logging.error('Metadata error: {0}'.format(err))
        self.docID += 1

    # def index_thread_line(self, j_line):
    #     es_ready = self.es_wrapper.create_es_format(json.loads(j_line))
    #     bulk(self.es, self.iterate_json_line_sentences(es_ready.sentences), chunk_size=200, request_timeout=60)
    #     self.index_line(tags=es_ready.tags, link=es_ready.link)

    def index_file(self, data_folder_path = "Data", filepath = "ru_meta_part_test_sample.jsonl.gz"):
        """
        Index file.
        """
        with gzip.open(os.path.join(data_folder_path, filepath), "rt", encoding='UTF-8') as j_file:
            for j_line in j_file:
                es_ready = self.es_wrapper.create_es_format(json.loads(j_line))
                bulk(self.es, self.iterate_json_line_sentences(es_ready.sentences), chunk_size=200,
                     request_timeout=60)
                self.index_line(tags=es_ready.tags, link=es_ready.link)

    def index_dir(self):
        """
        Index directory.
        """
        for root, dirs, files in os.walk(self.data_dir):
            for file in files:
                if ".gz" == os.path.splitext(file)[1]:
                    print(os.path.join(root, file))
                    logging.info(f'Indexing {os.path.join(root, file)}')
                    self.index_file(root, file)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Index corpus in Elasticsearch 6.x.')
    parser.add_argument('--overwrite', type=bool, help='overwrite existing indices')
    parser.add_argument('--data_folder_path', type=str, help='Data Folder Path')
    args = parser.parse_args()
    if args.overwrite:
        create_indices(elastic_creds = "user:pass", elastic_url="localhost:9200", docs_index_name="test.docs", sentences_index_name = "test.sentences")
    x = Indexer(data_dir=args.data_folder_path if args.data_folder_path is not None else "Data", elastic_creds="user:pass", elastic_url="localhost:9200")
    if x.ping_es() is True:
        logging.info("Start indexing")
        print("Start indexing")
        t1 = time.time()
        x.index_dir()
        t2 = time.time()
        total_time = t2 - t1
        time_format = datetime.timedelta(seconds=total_time)
        logging.info(
            f'Corpus indexed in {time_format}: {x.docID} documents, {x.sentID} sentences, {x.total_num_words} words')
        print(
            f'Corpus indexed in {time_format} | {x.docID} documents, {x.sentID} sentences, {x.total_num_words} words')
