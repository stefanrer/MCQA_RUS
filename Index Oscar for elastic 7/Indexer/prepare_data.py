import copy
import json
import gzip
import os
import re
import logging


class PrepareData:
    """
    Contains functions called when preparing the data
    for indexing in the database.
    """
    rxBadField = re.compile('[^a-zA-Z0-9_]|^(?:lex|gr|gloss_index|wf|[wm]type|ana|sent_ids|id)$')
    MULTIPLE_SHARDS_THRESHOLD = 256 * 1024 * 1024

    __slots__ = ('settings', 'docNormalizer')

    def __init__(self, settings: dict):
        """
        Load corpus-specific settings from settings.json.
        """
        self.settings = copy.deepcopy(settings)
        logging.basicConfig(filename=os.path.join("../Logs", "prepare.log"), filemode="w", level=logging.INFO)
        # self.docNormalizer = {
        #     'analysis': {
        #         'analyzer': {
        #             'lowercase_normalizer': {
        #                 'type': 'custom',
        #                 'tokenizer': 'standard',
        #                 'char_filter': [],
        #                 'filter': ['lowercase']
        #             },
        #             'lowercase_normalizer_notokenize': {
        #                 'type': 'pattern',
        #                 'pattern': '[|]',
        #                 'lowercase': True
        #             }
        #         }
        #     }
        # }

    def generate_docs_mapping(self) -> dict:
        """
        Return Elasticsearch mapping for the type "doc".
        Each element of docs index contains metadata about
        a single document.
        """
        docs_properties = self.settings['docs_properties']

        # mapping = {
        #     'mappings': {
        #         'properties': docs_properties
        #     },
        #     'settings': self.docNormalizer
        # }

        mapping = {
            'mappings': {
                'properties': docs_properties
            }
        }
        logging.debug("generated docs mapping")
        return mapping

    def generate_sentences_mapping(self, corpus_size_in_bytes: int = 0) -> dict:
        """
            Return Elasticsearch mapping for the type "sentence", based
            on searchable features described in the corpus settings.
            """
        sents_properties = self.settings['sents_properties']

        # Large corpora on machines with enough CPU cores
        # are split into shards, so that searches can run in parallel
        # on different pieces of the corpus.
        num_shards = 1
        cpu_count = os.cpu_count()
        if (corpus_size_in_bytes > PrepareData.MULTIPLE_SHARDS_THRESHOLD
                and cpu_count is not None and cpu_count > 2):
            num_shards = cpu_count - 1
        mapping = {
            'mappings': {
                'properties': sents_properties
            },
            'settings': {
                'number_of_shards': num_shards,
                'refresh_interval': '30s',
                'max_regex_length': 5000,
                'mapping': {
                    'nested_objects.limit': 50000
                }
            }
        }
        logging.debug("generated sentence mapping")
        return mapping

    def generate_mappings(self) -> dict:
        """
            Return Elasticsearch mappings for all types to be used
            in the corpus database.
            """
        m_sent = self.generate_sentences_mapping()
        m_doc = self.generate_docs_mapping()
        mappings = {
            'docs': m_doc,
            'sentences': m_sent
        }
        logging.debug("generated mapping for all types")
        return mappings

    def write_mappings(self, file_name_out: str):
        """
            Generate and write Elasticsearch mappings for all types to be used
            in the corpus database.
            """
        with open(file_name_out, 'w', encoding='utf-8') as f_out:
            f_out.write(json.dumps(self.generate_mappings(), indent=2, ensure_ascii=False))


if __name__ == '__main__':
    pd = PrepareData()
    pd.write_mappings('mappings.json')
