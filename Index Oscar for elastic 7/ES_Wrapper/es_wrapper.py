import os
import json
import logging
from dataclasses import dataclass
from ES_Wrapper.text_cleaner import TextCleaner
from ES_Wrapper.sentence_splitter import Splitter
from ES_Wrapper.tokenizer import Tokenizer


@dataclass(frozen=True)
class RussianES:
    text: str
    sentences: list
    tags: list
    link: str


class EsWrapper:
    """
    Contains methods to make JSONs ready for indexing from jsonl files
    """
    __slots__ = ('settings', 'data_dir', 'cleaner', 'tokenizer', 'splitter')

    def __init__(self, settings_path: str = '../ES_Wrapper/settings.json', files_dir: str = '../Data'):
        self.settings = self.load_settings(settings_path)
        logging.basicConfig(filename=os.path.join("../Logs", "wrapper.log"), filemode="w",
                            level=logging.INFO)
        self.data_dir = files_dir
        if not os.path.exists(self.data_dir):
            logging.error("No Data Directory Detected")
        self.cleaner = TextCleaner(self.settings)
        self.tokenizer = Tokenizer(self.settings)
        self.splitter = Splitter(self.settings)

    @staticmethod
    def load_settings(file_path: str) -> dict:
        with open(file_path, "r", encoding="utf-8-sig") as settings_file:
            return json.loads(settings_file.read())

    def read_files_dir(self):
        for (root, dirs, files) in os.walk(self.data_dir, topdown=True):
            for file in files:
                self.read_file(file_path=os.path.join(self.data_dir, file))

    def read_file(self, file_path: str):
        with open(file_path, "r", encoding='UTF-8') as j_file:
            for j_line in j_file:
                es_ready = self.create_es_format(json.loads(j_line))
                logging.debug(es_ready)

    @staticmethod
    def remove_meta_from_sentences(sentences: list) -> list:
        for index in range(len(sentences)):
            sentences[index] = sentences[index]['text']
        return sentences

    def create_es_format(self, j_dict: dict) -> RussianES:
        text = self.cleaner.clean_text(j_dict['content'])
        tokens = self.tokenizer.tokenize(text)
        sentences = self.splitter.split(tokens, text)
        sentences = self.remove_meta_from_sentences(sentences)  # Remove token info from sentences
        annotation = j_dict['metadata']['annotation']
        uri = j_dict['warc_headers']['warc-target-uri']
        return RussianES(text=text, sentences=sentences, tags=annotation, link=uri)


if __name__ == "__main__":
    es_wrapper = EsWrapper()
    es_wrapper.read_files_dir()
