# Index documents to Elasticsearch 6
## Install required python libraries (done on python 3.10)
pip install -r requirements.txt

## Launch Elastic Indexing
1. python index_document.py --overwrite = 'True' --data_folder_path = 'Path to data folder'

2. nohup python index_document.py &
    * nohup if you want to launch in background mode for example on server
    * --overwrite - bool() so don't write the flag if you don't want to overwrite existing indices
      * --data_folder_path - Folder where you store data that needs to be indexed
        * in RuCam's case it's Oscar dataset folder:
          * it consists of 1000 gzip jsonline files
          * example of 1 jsonline : 
            * {"content":"На сайте функционирует система коррекции ошибок. Обнаружив неточность в тексте на данной странице, выделите её и нажмите Ctrl+Enter.","warc_headers":{"warc-block-digest":"sha1:DXDRVOJX67IOKXP3EXOGF56EZSAKNCEK","warc-record-id":"<urn:uuid:7f05e260-22a8-4304-afd0-a8e29354e345>","content-type":"text/plain","warc-refers-to":"<urn:uuid:ef3106c6-1603-44d7-8ed6-37c449c49596>","warc-date":"2021-11-28T10:40:23Z","warc-type":"conversion","content-length":"234","warc-identified-content-language":"rus,srp","warc-target-uri":"http://allorostov.ru/comp/168085"},"metadata":{"identification":{"label":"ru","prob":0.99334705},"annotation":["tiny"],"sentence_identifications":[{"label":"ru","prob":0.99334705}]}}
          * The text, sentence, token, meta extraction is happening in EsWrapper class in es_wrapper.py file
## Info
### Index mapping (create_indices.py):
#### docs mapping:
* n_sents - number of sentences in doc
* annotation from jsonline
* link to doc from jsonline
* sentences ids in .sentences index
   ```
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
   ```
#### sentences mapping:
* prev_id - id of prev sentence in doc
* next_id - id of next sentence in doc
* doc_id - id of doc in .docs index
* n_words - number of words in sentence
* sentence - sentence text with russian analyzer (has snowball stemmer)
    ```
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
    ```
###  Sentences Extraction (es_wrapper.py)
* settings in wrapper_settings.json file
#### text_cleaner.py
* remove html
* normalize spaces and newlines
* add space between punc and next word
* convert quotes
* clean other things
#### tokenizer.py
* tokenize text
#### sentence_splitter.py
* Split the text into sentences by packing tokens into separate sentence JSON objects.
* Remove token info from sentences
#### tags = annotation
#### link = uri
#### Get class RussianES:
    text: str
    sentences: list
    tags: list
    link: str
Later used in the index_document.py script
