import pandas as pd
import pymorphy2
import json
from pymystem3 import Mystem
from tqdm import tqdm
from typing import Union
from collections import defaultdict
import re


def find_iterative(iter_text, o, iter_step=0):
    founded = list(re.finditer(o.lower(), iter_text.lower()))

    if len(founded) > iter_step:
        i = list(re.finditer(o.lower(), iter_text.lower()))[iter_step]
        iter_text = iter_text[:i.span()[0]] + ("**" + iter_text[i.span()[0]:i.span()[1]] + "**") + iter_text[
                                                                                                   i.span()[1]:]
        iter_step += 1
        return find_iterative(iter_text, o, iter_step)

    else:
        iter_step += 1
        return iter_text


def make_obj_bold(make_text, obj):
    objs = obj.split(', ')
    for o in objs:
        make_text = find_iterative(make_text, o)

    return make_text


res = defaultdict(lambda: defaultdict(list))


def extract_comp(adjs: list, founded_adjectives: list[str], text: str, obj1, obj2, adj_type, common_obj, aspect):
    for founded_adj in founded_adjectives:
        modified_text = make_obj_bold(text, obj1)
        modified_text = make_obj_bold(modified_text, obj2)
        if not pd.isnull(common_obj):
            modified_text = make_obj_bold(modified_text, common_obj)
        ltext = text.lower()
        splitted = ltext.split(founded_adj)
        # подходят только предложения, где прилагательное находится между двумя объектами
        if obj1 in splitted[0] and obj2 in splitted[0]:
            td = {'text': modified_text,
                  'adj_type': None,
                  'adj': None,
                  'common_obj': common_obj,
                  'aspect': aspect}
            res[obj1 + '|' + obj2][None].append(td)

        elif obj1 in splitted[1] and obj2 in splitted[1]:
            td = {'text': modified_text,
                  'adj_type': None,
                  'adj': None,
                  'common_obj': common_obj,
                  'aspect': aspect}
            res[obj1 + '|' + obj2][None].append(td)
        else:
            # теперь adj - это список найденных в тексте плохих/хороших прилагательных
            if re.search(r"(\bне).{,3}%s" % founded_adj, text):
                # print(founded_adj, '###', modified_text)
                if adj_type == 'pos':
                    adj_type = 'neg'
                else:
                    adj_type = 'pos'
            else:
                adj_type = adj_type

            if adjs[founded_adj] == 'COMP':
                td = {'text': modified_text,
                      'adj_type': adj_type,
                      'adj': founded_adj,
                      'common_obj': common_obj,
                      'aspect': aspect}
                res[obj1 + '|' + obj2]['COMP'].append(td)
            # это нужно если будут примеры только с одним элементом пары либо если хотим без сравнения
            elif adjs[founded_adj] == 'Supr':
                td = {'text': modified_text,
                      'adj_type': adj_type,
                      'adj': founded_adj,
                      'common_obj': common_obj,
                      'aspect': aspect}
                res[obj1 + '|' + obj2]['Supr'].append(td)
            else:
                td = {'text': modified_text,
                      'adj_type': 'neutral',
                      'adj': 'neutral',
                      'common_obj': common_obj,
                      'aspect': aspect}
                res[obj1 + '|' + obj2]['neutral'].append(td)


morph = pymorphy2.MorphAnalyzer()
m = Mystem()

good = pd.read_csv('better.txt')
bad = pd.read_csv('worse.txt')


def compile_big_goods():
    big_goods_comp = dict()
    for word in tqdm(good.word.values):
        for par in morph.parse(word)[0].lexeme:
            if 'Supr' in par.tag:
                big_goods_comp[par.word] = 'Supr'
            if 'COMP' in par.tag:
                big_goods_comp[par.word] = 'COMP'
    return big_goods_comp


def compile_big_bads():
    big_bads_comp = dict()
    for word in tqdm(bad.word.values):
        for par in morph.parse(word)[0].lexeme:
            if 'Supr' in par.tag:
                big_bads_comp[par.word] = 'Supr'
            if 'COMP' in par.tag:
                big_bads_comp[par.word] = 'COMP'
    return big_bads_comp


big_goods = compile_big_goods()
big_bads = compile_big_bads()

data_df = pd.read_csv("final_dataset.csv", sep='\t', dtype=str)
data_df['obj'] = data_df['Object1'] + ';' + data_df['Object2']


def compile_result_dict(df):
    comp_result_dict = defaultdict(dict)
    for index, row in df.iterrows():
        # concatenate the values in the 'Obj1' and 'Obj2' columns
        key = row['obj']
        # add the sentence to the list of sentences associated with the key
        dict_to_return = {"Sentence": str, "CommonObject": str, "Aspect": Union[str, None]}
        dict_to_return['Sentence'] = str(row['Sentence'])
        dict_to_return['CommonObject'] = row['CommonObject']
        try:
            dict_to_return['Aspect'] = row['Aspect']
        except KeyError:
            dict_to_return['Aspect'] = None

        if key in comp_result_dict:
            comp_result_dict[key].append(dict_to_return)
        else:
            comp_result_dict[key] = [dict_to_return]
    return comp_result_dict


result_dict = compile_result_dict(data_df)

for pair in tqdm(list(result_dict.keys())):
    obj1, obj2 = pair.split(';')
    texts = result_dict[pair]
    for text in texts:
        # analyzed_text = analyze_sent(text)
        good_words = [word for word in big_goods.keys() if word in str(text['Sentence'])]
        if good_words:
            extract_comp(big_goods, good_words, text['Sentence'], obj1, obj2, 'pos', text['CommonObject'],
                         text['Aspect'])
        bad_words = [word for word in big_bads.keys() if word in str(text['Sentence'])]
        if bad_words:
            extract_comp(big_bads, bad_words, text['Sentence'], obj1, obj2, 'neg', text['CommonObject'], text['Aspect'])
        if not bad_words and not good_words:
            modified_text = make_obj_bold(str(text['Sentence']), obj1)
            modified_text = make_obj_bold(modified_text, obj2)
            if not pd.isnull(text['CommonObject']):
                modified_text = make_obj_bold(modified_text, text['CommonObject'])
            td = {'text': modified_text,
                  'adj_type': None,
                  'adj': None,
                  'common_obj': text['CommonObject'],
                  'aspect': text['Aspect']}
            res[obj1 + '|' + obj2][None].append(td)


def default_dict_to_dict(d):
    if isinstance(d, defaultdict):
        d = {k: default_dict_to_dict(v) for k, v in d.items()}
    return d


# convert defaultdict to dict
res = default_dict_to_dict(res)

pos = 0
neg = 0
neutral = 0

for key in res.keys():
    for k in res[key].values():
        for i in k:
            if i['adj_type'] is None:
                neutral += 1
            if i['adj_type'] == 'pos':
                pos += 1
            if i['adj_type'] == 'neg':
                neg += 1

print(pos, neg, neutral)

with open('data.json', 'w') as f:
    json.dump(res, f)
