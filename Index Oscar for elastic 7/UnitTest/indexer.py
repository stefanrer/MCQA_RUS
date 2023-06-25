import logging
import unittest
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import RequestError
from Indexer.prepare_data import PrepareData
from ES_Wrapper.es_wrapper import EsWrapper
from ES_Wrapper.es_wrapper import RussianES
from tqdm import tqdm
import json
import datetime
import os
import re
import time
import random


class Indexer:
    """
    Contains methods for loading ES_Wrapped json sentences into ES Database
    """
    rxBadFileName = re.compile('[^\\w_.-]*', flags=re.DOTALL)

    __slots__ = ('total_num_words', 'corpusSizeInBytes', 'filenames', 'data_dir', 'docMapping', 'sentMapping',
                'overwrite', 'settings', 'name', 'pd', 'es_wrapper', 'es', 'es_ic', 'shuffled_ids', 'sID', 'dID',
                'numSents')

    def __init__(self, overwrite_index: bool = False, data_dir: str = '../Data'):
        self.total_num_words = 0
        self.corpusSizeInBytes = 0
        self.filenames = []
        self.data_dir = data_dir
        self.docMapping = None
        self.sentMapping = None
        self.overwrite = overwrite_index  # whether to overwrite an existing index without asking
        with open('../Indexer/settings.json', 'r', encoding='utf-8') as fSettings:
            self.settings = json.load(fSettings)
        self.name = self.settings['corpus_name'].lower()  # index must be lower case
        logging.basicConfig(filename=os.path.join("../Logs", "indexer.log"), filemode="w", level=logging.INFO)

        self.pd = PrepareData(self.settings)
        self.es_wrapper = EsWrapper()
        # Initialize Elasticsearch connection
        self.es = None
        if 'elastic_url' in self.settings and len(self.settings['elastic_url']) > 0:
            # Connect to a non-default URL or supply username and password
            self.es = Elasticsearch([self.settings['elastic_url']])
            logging.info(f"connected to {self.settings['elastic_url']} Elastic Search Host")
        else:
            self.es = Elasticsearch()
            logging.info("default connection to Elastic Search")

        self.es_ic = IndicesClient(self.es)

        self.shuffled_ids = [i for i in range(1, 1000000)]
        random.shuffle(self.shuffled_ids)
        self.shuffled_ids.insert(0, 0)  # id=0 is special and should not change

        self.sID = 0  # current sentence ID
        self.dID = 0  # current document ID
        self.numSents = 0  # number of sentences in current document

    def ping_es(self) -> bool:
        return self.es.ping()

    def delete_indices(self) -> bool:
        """
        If there already exist indices with the same names,
        ask the user if they want to overwrite them. If they
        say yes, remove the indices and return True. Otherwise,
        return False.
        """
        if not self.overwrite:
            if (self.es_ic.exists(index=self.name + '.docs')
                    or self.es_ic.exists(index=self.name + '.sentences')):
                print('It seems that a corpus named "' + self.name + '" already exists. '
                      + 'Do you want to overwrite it? [y/n]')
                reply = input()
                if reply.lower() != 'y':
                    logging.info('Indexation aborted.')
                    return False
        if self.es_ic.exists(index=self.name + '.docs'):
            self.es_ic.delete(index=self.name + '.docs')
        if self.es_ic.exists(index=self.name + '.sentences'):
            self.es_ic.delete(index=self.name + '.sentences')
        return True

    def create_indices(self):
        """
        Create empty elasticsearch indices for corpus data, using
        mappings provided by PrepareData.
        """
        self.sentMapping = self.pd.generate_sentences_mapping(corpus_size_in_bytes=self.corpusSizeInBytes)
        self.docMapping = self.pd.generate_docs_mapping()
        self.es_ic.create(index=self.name + '.docs',
                          body=self.docMapping)
        self.es_ic.create(index=self.name + '.sentences',
                          body=self.sentMapping)

    def randomize_id(self, real_id: int):
        """
        Return a (relatively) randomized sentence ID. This randomization
        is needed in context-aware word queries where the sentences
        are iterated in the order determined by their IDs.
        """
        if real_id < 0:
            return real_id
        id_start, id_end = real_id // 1000000, real_id % 1000000
        return id_start * 1000000 + self.shuffled_ids[id_end]

    def iterate_json_line_sentences(self, sentences: list, tq: tqdm):
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
            # self.es.index(index=self.name + '.sentences',
            #               id=self.sID,
            #               body=sent)
            cur_action = {'_index': self.name + '.sentences',
                          '_id': self.randomize_id(self.sID),
                          '_source': sent}
            yield cur_action
            if self.sID % 500 == 0:
                logging.debug('Indexing sentence', self.sID, ',', self.total_num_words, 'words so far.')
                tq.set_postfix(doc=self.dID, sentences=self.sID, words=self.total_num_words)
                tq.update()
            self.numSents += 1
            self.sID += 1

    def index_line(self, tags: list, link: str, tq: tqdm):
        """
        Store the metadata of the source json_line.
        """
        if self.dID % 100 == 0:
            logging.debug('Indexing json_line', self.dID)
            tq.set_postfix(doc=self.dID, sentences=self.sID, words=self.total_num_words)
            tq.update()
        meta = {'tags': tags, 'link': link, 'n_sents': self.numSents}
        self.numSents = 0
        try:
            self.es.index(index=self.name + '.docs',
                          id=self.dID,
                          body=meta)
        except RequestError as err:
            logging.error('Metadata error: {0}'.format(err))
        self.dID += 1

    def analyze_dir(self):
        """
        Collect all filenames for subsequent indexing and calculate
        their total size. Store them as object properties.
        """
        self.filenames = []
        self.corpusSizeInBytes = 0
        for root, dirs, files in os.walk(self.data_dir):
            for file in files:

                file_name_full = os.path.join(root, file)
                file_size = os.path.getsize(file_name_full)
                self.corpusSizeInBytes += file_size
                self.filenames.append((file_name_full, file_size))
        logging.info(f"Corpus size in Bytes = {self.corpusSizeInBytes}")

    def index_dir(self):
        """
        Index all files from the Data directory, sorted by their size
        in decreasing order. Use a previously collected list of filenames
        and file sizes. Such sorting helps prevent memory errors
        when indexing large corpora, as the default behavior is to load
        the whole file is into memory, and there is more free memory
        in the beginning of the process. If MemoryError occurs, the
        iterative JSON parser is used, which works much slower.
        """
        tq = tqdm(total=0)
        if len(self.filenames) <= 0:
            logging.error('There are no files in this corpus.')
            return
        n = 0
        for file_name, file_size in sorted(self.filenames, key=lambda p: -p[1]):
            logging.info(file_name)
            n += 1
            tq.desc = f"{os.path.basename(file_name)} {n}/{len(self.filenames)}"
            tq.update()
            if 'sample_size' in self.settings and 0 < self.settings['sample_size'] < 1:
                # Only take a random sample of the source files (for test purposes)
                if random.random() > self.settings['sample_size']:
                    continue
            with open(file_name, "r", encoding='UTF-8') as j_file:  # Index json lines 1 by 1
                for j_line in j_file:
                    es_ready = self.es_wrapper.create_es_format(json.loads(j_line))
                    bulk(self.es, self.iterate_json_line_sentences(es_ready.sentences, tq=tq), chunk_size=200,
                         request_timeout=60)
                    self.index_line(tags=es_ready.tags, link=es_ready.link, tq=tq)
                    tq.set_postfix(doc=self.dID, sentences=self.sID, words=self.total_num_words)
                    tq.update()
        tq.desc = "finished"
        tq.update()

    def load_corpus(self):
        """
        Drop the current database, if any, and load the entire corpus.
        """
        t1 = time.time()
        # self.compile_translations()
        indices_deleted = self.delete_indices()
        if not indices_deleted:
            return
        self.analyze_dir()
        self.create_indices()
        self.index_dir()
        t2 = time.time()
        total_time = t2 - t1
        time_format = datetime.timedelta(seconds=total_time)
        logging.info(f'Corpus indexed in {time_format}: {self.dID} documents, {self.sID} sentences')
        print(f'Corpus indexed in {time_format} | {self.dID} documents, {self.sID} sentences')


class IndexerTest(unittest.TestCase):
    def setUp(self):
        self.index = Indexer(data_dir='test', overwrite_index=True)
        self.index.delete_indices()
        self.index.analyze_dir()
        self.index.create_indices()
        self.es_ready = self.read_test_file()

    def read_test_file(self):
        with open("../test/test.jsonl", "r", encoding='UTF-8') as j_file:
            for j_line in j_file:
                return self.index.es_wrapper.create_es_format(json.loads(j_line))

    def test_doc_mapping_type(self):
        self.assertIs(type(self.index.docMapping), dict)

    def test_doc_mapping_len(self):
        self.assertNotEqual(len(self.index.docMapping), 0)

    def test_sentence_mapping_type(self):
        self.assertIs(type(self.index.sentMapping), dict)

    def test_sentence_mapping_len(self):
        self.assertNotEqual(len(self.index.sentMapping), 0)

    def test_es_wrapper_type(self):
        self.assertIs(type(self.es_ready), RussianES)

    def test_es_wrapper_text(self):
        print(self.es_ready.text)
        self.assertEqual(self.es_ready.text, "На сайте функционирует система коррекции ошибок. Обнаружив неточность в тексте на данной странице, выделите её и нажмите Ctrl+Enter.")

    def test_es_wrapper_sentences(self):
        print(self.es_ready.sentences)
        self.assertEqual(self.es_ready.sentences, ["На сайте функционирует система коррекции ошибок.", "Обнаружив неточность в тексте на данной странице, выделите её и нажмите Ctrl+Enter."])

    def test_es_wrapper_tags(self):
        print(self.es_ready.tags)
        self.assertEqual(self.es_ready.tags, ["tiny"])

    def test_es_link(self):
        print(self.es_ready.link)
        self.assertEqual(self.es_ready.link, "http://allorostov.ru/comp/168085")


if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)
