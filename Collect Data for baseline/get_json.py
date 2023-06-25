from elasticsearch import Elasticsearch
import spacy
from spacy_langdetect import LanguageDetector
from spacy.language import Language
from multiprocessing import Pool
import pandas as pd
import json
import os


@Language.factory("language_detector")
def get_lang_detector(nlp, name):
    return LanguageDetector()


nlp_detect = spacy.load("ru_core_news_sm") # 1
Language.factory("language_detector", func=get_lang_detector)
nlp_detect.add_pipe('language_detector', last=True)


def do_elastic_query(elastic_instance, es_query: dict, query_size: int = 3000):
    if es_query["size"] != query_size:
        es_query["size"] = query_size
    es_result = elastic_instance.search(
        index="oscar.sentences",
        body=es_query
    )
    return es_result["hits"]["hits"]


def get_elastic_query_hits(elastic_instance, es_query: dict):
    es_query["size"] = 1
    es_result = elastic_instance.search(
        index="oscar.sentences",
        body=es_query
    )
    return es_result["hits"]["total"]["value"]


def make_wildcard_query_for_object_pair(object_pair: tuple, query_size: int = 1, min_words: int = 3, max_words:int = 20):
    elastic_query = {
        "size": query_size,
        "query" : {
            "bool": {
                "must": [],
                "filter": [ 
                    { 
                        "range": { 
                            "n_words": { 
                                "gte": min_words,
                                "lte": max_words
                            }
                        }
                    }
                ]
            }
        },
        "fields": ["sentence"],
        "_source": False
    }
    common_object_list = [x for x in object_pair[0].split() if x in object_pair[1].split()]
    object1_list = [x for x in object_pair[0].split() if x not in object_pair[1].split() and x not in common_object_list]
    object2_list = [x for x in object_pair[1].split() if x not in object_pair[0].split() and x not in common_object_list]
    add_query_wildcards(common_object_list, elastic_query)
    add_query_wildcards(object1_list, elastic_query)
    add_query_wildcards(object2_list, elastic_query)
    return " ".join(common_object_list), " ".join(object1_list), " ".join(object2_list), elastic_query


def make_string_query_for_object_pair(object_pair: tuple, query_size: int = 1, min_words: int = 3, max_words:int = 20):
    elastic_query = {
        "size": query_size,
        "query": {
            "query_string": {
              "query": "",
              "default_field": "sentence"
            }
        },
        "fields": ["sentence"],
        "_source": False
    }
    common_object_list = [x for x in object_pair[0].split() if x in object_pair[1].split()]
    object1_list = [x for x in object_pair[0].split() if x not in object_pair[1].split() and x not in common_object_list]
    object2_list = [x for x in object_pair[1].split() if x not in object_pair[0].split() and x not in common_object_list]
    add_query_strings(common_object_list, elastic_query)
    add_query_strings(object1_list, elastic_query)
    add_query_strings(object2_list, elastic_query)
    return " ".join(common_object_list), " ".join(object1_list), " ".join(object2_list), elastic_query


def make_aspect_queries(es_exist_query: dict, es_instance, q_res_list: list, ob1: str, ob2: str, c_ob: str, is_string: bool = False):
    if not is_string:
        add_empty_query_wildcard(es_exist_query)
    comparison_pril = [
    'лучш', 'легч', 'хорош', 'велик', 'сильн', 'мил', 'известн', 'красив', 'чист','свеж',
    'хуж', 'слаб', 'плох', 'бедн', 'опасн', 'ужасн', 'сложн', 'противн', 'скучн', 'вредн'
    ]
    for pril in comparison_pril:
        if is_string:
            es_exist_query["query"]["query_string"]["query"] += " AND " + pril
        else:
            es_exist_query["query"]["bool"]["must"][-1]["wildcard"]["sentence"]["value"] = pril + "*"
        hits, a_num_hits = clean_hits(do_elastic_query(es_instance, es_exist_query, query_size = 3000))
        if a_num_hits > 0:
            for a_hit in hits:
                q_res_list.append([ob1, ob2, c_ob, pril, a_hit])
            #print(f"\nObject1: {ob1}\nObject2: {ob2}\nCommon object: {c_ob}\nAspect: {pril}\nHits: {a_num_hits}")


def add_query_wildcards(wildcard_object_list: list, wildcard_query: dict):
    for query_object in wildcard_object_list:
        object_append = {
            "wildcard": {
                "sentence": {
                    "value" : query_object + "*",
                    "boost": 1.0,
                    "rewrite": "constant_score"
                }
            }
        }
        wildcard_query["query"]["bool"]["must"].append(object_append)


def add_empty_query_wildcard(wildcard_query: dict):
    object_append = {
        "wildcard": {
            "sentence": {
                "value" : "",
                "boost": 1.0,
                "rewrite": "constant_score"
            }
        }
    }
    wildcard_query["query"]["bool"]["must"].append(object_append)


def add_query_strings(string_object_list: list, string_query: dict):
    if string_query["query"]["query_string"]["query"] != "":
        string_query["query"]["query_string"]["query"] += " AND "
    string_query["query"]["query_string"]["query"] += " AND ".join(string_object_list)


def f_detect_language(hit):
    return nlp_detect(hit)._.language


def f_get_sentences(hit):
    return hit["fields"]["sentence"][0]


def clean_hits(hits: dict):
    clean_hits = []
    amount_hits = 0
    
    with Pool(24) as clean_p:
        hits_list = clean_p.map(f_get_sentences, hits)
    with Pool(24) as detect_p: 
        detect = detect_p.map(f_detect_language, hits_list)
    for idx in range(len(hits_list)):
        if detect[idx]["language"] == "ru":
            clean_hits.append(hits_list[idx])
            amount_hits += 1
    return clean_hits, amount_hits


# Create Dataset directory
if not os.path.exists("Dataset"):
    os.makedirs("Dataset")

# Establish elastic connection
es = Elasticsearch("http://localhost:10008", request_timeout = 100, max_retries = 2)



# Opening JSON file
f = open("finish_dataset.json")

# returns JSON object as a dictionary
object_data = json.load(f)

# Closing file
f.close()
for key in object_data:
    if key == "Методологии разработки":
        is_query_string = True
    else:
        is_query_string = False
    print(f"Currently searching for {key} Category")
    filename = key + ".json"
    filepath = os.path.join("Dataset", filename)
    all_obj_list = object_data[key]
    possible_pairs = [(a, b) for idx, a in enumerate(all_obj_list) for b in all_obj_list[idx + 1:]]
    query_results = []
    for ob_p in possible_pairs:
        
        # Check if object1 is part of object2 or object2 is part of object1
        check_common_object_list = [x for x in ob_p[0].split() if x in ob_p[1].split()]
        check_object1_list = [x for x in ob_p[0].split() if x not in ob_p[1].split() and x not in check_common_object_list]
        check_object2_list = [x for x in ob_p[1].split() if x not in ob_p[0].split() and x not in check_common_object_list]
        if len(check_object1_list) == 0 or len(check_object2_list) == 0:
            continue
        asp = ""
        if key == "Методологии разработки":
            com_obj, obj1, obj2, query_body = make_string_query_for_object_pair(ob_p)
        else:
            com_obj, obj1, obj2, query_body = make_wildcard_query_for_object_pair(ob_p)
        num_hits = get_elastic_query_hits(elastic_instance = es, es_query = query_body)
        if num_hits < 100:
            continue
        else:
            if num_hits < 1000: # No Aspect query
                #print("----------------------------------------------------------------------------------------------------")
                #print(f"Unclean Hits: {num_hits}")
                hits, num_hits = clean_hits(do_elastic_query(es, query_body, query_size = 1000))
                #print(hits[0])
                #print(f"\nObject1: {obj1}\nObject2: {obj2}\nCommon object: {com_obj}\nAspect: {asp}\nHits: {num_hits}")
                for hit in hits:
                    query_results.append([obj1, obj2, com_obj, asp, hit])
            else: # Aspect query
                #print(f"\nObject1: {obj1}\nObject2: {obj2}\nCommon object: {com_obj}\nAspect: {asp}\nHits: {num_hits}")
                make_aspect_queries(query_body, es, query_results, obj1, obj2, com_obj, is_query_string)
    json_object = {key : query_results}
    with open(filepath, 'w', encoding='utf8') as json_file:
        json.dump(json_object, json_file, ensure_ascii=False)
    print(f"Finished searching for {key} Category")





