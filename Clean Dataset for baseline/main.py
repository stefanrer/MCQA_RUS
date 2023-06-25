import pandas as pd
import json
import re
import spacy
from tqdm import tqdm

nlp = spacy.load('ru_core_news_md')

web_p = re.compile(r"(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?")
numbers_p = re.compile(r"\(\d+\)")
dash_p = re.compile(r"[-]{3,}")
dates = re.compile('\d+:\d+')
numbers_sqbr = re.compile(r"\[[A-z0-9\, ]+\]")
brackets_p = re.compile(r"\{.\+?\}")
fl_p = re.compile(r"(First:.+?)?Last: \d{1,2}\/\d{1,2}\/\d{2,4} \d+")


def delete_spec(text):
    text = numbers_sqbr.sub("", text)
    text = text.replace(" < ", "")
    text = text.replace("<>", "")
    text = text.replace(" > ", "")
    text = text.replace("•  •  •", "")
    text = text.replace("[...]", "")
    text = brackets_p.sub("", text)
    return text


def clean_russian_text(text):
    # Remove URLs
    text = re.sub(r'http\S+', '', text)

    # Remove emoticons
    text = re.sub(r'[\U0001F600-\U0001F64F]', '', text)

    # Remove HTML tags
    text = re.sub(r'<.*?>', '', text)

    # Remove empty brackets
    text = re.sub(r'\(\s*\)', '', text)
    text = re.sub(r'\[\s*\]', '', text)

    # Remove brackets with punctuation inside
    text = re.sub(r'\([^\w\s]*\)', '', text)
    text = re.sub(r'\[[^\w\s]*\]', '', text)

    # Remove special symbols
    text = delete_spec(text)

    # Remove extra whitespaces
    text = re.sub(r'\s+', ' ', text).strip()

    # Format the register
    text = text.lower()
    text = text.capitalize()

    return text


def compare_sentences(sent):
    result = []
    docs = [nlp(s) for s in tqdm(sent, desc='Creating Doc objects')]
    for i, doc1 in enumerate(docs):
        for j, doc2 in enumerate(docs):
            if i != j:
                similarity = doc1.similarity(doc2)
                if similarity > 0.8:
                    docs.remove(doc2)
        result.append(sent[i])
    return result


def remove_nan_rows(df):
    df = df.dropna(subset=['Sentence'])
    return df


def dict_to_dataframe(d):
    data = []
    for key, values in d.items():
        obj1, obj2 = key.split(";")
        if len(values) == 0:
            continue
        print("\n", obj1, obj2)
        values = compare_sentences(values)
        for value in values:
            data.append([obj1, obj2, clean_russian_text(value)])
    df = pd.DataFrame(data, columns=['Obj1', 'Obj2', 'Sentence'])
    return df


def split_dataframe(df):
    half = len(df) // 2
    df1 = df.iloc[:half]
    df2 = df.iloc[half:]
    return df1, df2


with open("dataset2.json", "r", encoding="UTF-8") as f:
    sentences = remove_nan_rows(dict_to_dataframe(json.load(f)))
    sentences1, sentences2 = split_dataframe(sentences)
    sentences1.to_csv("dataset1.csv", sep='\t')
    sentences2.to_csv("dataset2.csv", sep='\t')
