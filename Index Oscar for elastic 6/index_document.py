import logging

import elasticsearch
from elasticsearch import Elasticsearch
from tqdm import tqdm
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import RequestError
from es_wrapper import EsWrapper
from create_indices import create_indices
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import Pool
import urllib3
import json
import gzip
import os
import re
import random
import time
import datetime
import argparse



class Indexer:
    """
    Contains methods for loading ES_Wrapped json sentences into ES Database
    """
    rxBadFileName = re.compile('[^\\w_.-]*', flags=re.DOTALL)
    http = urllib3.PoolManager(maxsize=50)

    def __init__(self, data_dir: str = '../Data'):
        self.name = "oscar"
        self.total_num_words = 0
        self.corpusSizeInBytes = 0
        self.filenames = []
        self.data_dir = data_dir
        self.pbar = tqdm(total=72327)
        es_logger = logging.getLogger('elasticsearch')
        es_logger.setLevel(logging.WARNING)
        # urllib3_log = logging.getLogger("urllib3")
        # urllib3_log.setLevel(logging.CRITICAL)
        logging.basicConfig(filename=os.path.join("Logs", "indexer.log"), filemode="w", level=logging.INFO)
        self.es_wrapper = EsWrapper()
        # Initialize Elasticsearch connection
        self.es = Elasticsearch()
        logging.info("default connection to Elastic Search")
        self.rand_range = 1000000
        self.shuffled_ids = [i for i in range(1, self.rand_range)]
        random.shuffle(self.shuffled_ids)
        self.shuffled_ids.insert(0, 0)  # id=0 is special and should not change

        self.sID = 0  # current sentence ID
        self.dID = 0  # current document ID
        self.numSents = 0  # number of sentences in current document

    def ping_es(self) -> bool:
        return self.es.ping()

    def randomize_id(self, real_id: int):
        """
        Return a (relatively) randomized sentence ID. This randomization
        is needed in context-aware word queries where the sentences
        are iterated in the order determined by their IDs.
        """
        if real_id < 0:
            return real_id
        id_start, id_end = real_id // self.rand_range, real_id % self.rand_range
        return id_start * self.rand_range + self.shuffled_ids[id_end]

    def iterate_json_line_sentences(self, sentences: list):
        self.numSents = 0
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
                sent['prev_id'] = self.randomize_id(self.sID - 1)
            if not b_last and 'last' not in sent:
                sent['next_id'] = self.randomize_id(self.sID + 1)
            else:
                prev_last = True
            sent['doc_id'] = self.dID
            sent['sentence'] = sentences[i]
            cur_action = {'_index': self.name + '.sentences',
                          '_type': 'sent',
                          '_id': self.randomize_id(self.sID),
                          '_source': sent}
            yield cur_action
            self.numSents += 1
            self.sID += 1

    def index_line(self, tags: list, link: str):
        """
        Store the metadata of the source json_line.
        """
        if self.dID % 1000 == 0 and self.dID != 0:
            logging.info(f'Indexing document {self.dID}')
        meta = {'tags': tags, 'link': link, 'n_sents': self.numSents}
        self.numSents = 0
        try:
            self.es.index(index=self.name + '.docs',
                          id=self.dID,
                          doc_type="doc",
                          body=meta)
        except RequestError as err:
            logging.error('Metadata error: {0}'.format(err))
        self.dID += 1

    def index_thread_line(self, j_line):
        self.pbar.update(1)
        es_ready = self.es_wrapper.create_es_format(json.loads(j_line))
        bulk(self.es, self.iterate_json_line_sentences(es_ready.sentences), chunk_size=200, request_timeout=60)
        self.index_line(tags=es_ready.tags, link=es_ready.link)

    # def index_dir(self):
    #     """
    #     Index all files from the Data directory, sorted by their size
    #     in decreasing order. Use a previously collected list of filenames
    #     and file sizes. Such sorting helps prevent memory errors
    #     when indexing large corpora, as the default behavior is to load
    #     the whole file is into memory, and there is more free memory
    #     in the beginning of the process. If MemoryError occurs, the
    #     iterative JSON parser is used, which works much slower.
    #     """
    #     for file_name, file_size in sorted(self.filenames, key=lambda p: -p[1]):
    #         logging.info(file_name)
    #         with gzip.open(file_name, "rt", encoding='UTF-8') as j_file:  # Index json lines 1 by 1
    #             for j_line in j_file:
    #                 es_ready = self.es_wrapper.create_es_format(json.loads(j_line))
    #                 bulk(self.es, self.iterate_json_line_sentences(es_ready.sentences), chunk_size=200,
    #                      request_timeout=60)
    #                 self.index_line(tags=es_ready.tags, link=es_ready.link)
    #             # with ThreadPoolExecutor(max_workers=2000) as executor:
    #             #     executor.map(self.index_thread_line, j_file.readlines())

    def index_file(self):
        """
        Index file.
        """
        with gzip.open(os.path.join("Data", "ru_meta_part_test_sample.jsonl.gz"), "rt", encoding='UTF-8') as j_file:
            # with ThreadPoolExecutor(max_workers=100) as executor:
            #     executor.map(self.index_thread_line, j_file.readlines())

            for j_line in j_file:
                es_ready = self.es_wrapper.create_es_format(json.loads(j_line))
                bulk(self.es, self.iterate_json_line_sentences(es_ready.sentences), chunk_size=200,
                     request_timeout=60)
                self.index_line(tags=es_ready.tags, link=es_ready.link)
                self.pbar.update(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Index corpus in Elasticsearch 6.x.')
    parser.add_argument('--overwrite', type=bool, help='overwrite existing indices')
    args = parser.parse_args()
    if args.overwrite:
        create_indices()
    x = Indexer()
    if x.ping_es() is True:
        logging.info("Start indexing")
        t1 = time.time()
        x.index_file()
        t2 = time.time()
        total_time = t2 - t1
        time_format = datetime.timedelta(seconds=total_time)
        logging.info(
            f'Corpus indexed in {time_format}: {x.dID} documents, {x.sID} sentences, {x.total_num_words} words')
        print(
            f'Corpus indexed in {time_format} | {x.dID} documents, {x.sID} sentences, {x.total_num_words} words')
