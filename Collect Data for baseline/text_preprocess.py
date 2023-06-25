# import nltk
import re
import os
import pickle
import pandas as pd
import argparse

# nltk.download("punkt")

web_p = re.compile(r"(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?")
numbers_p = re.compile(r"\(\d+\)")
numbers_ru_p = re.compile(r"(\d+ ){3,}")
dash_p = re.compile(r"[-]{3,}")
dates = re.compile('\d+:\d+')
numbers_sqbr = re.compile(r"\[[A-z0-9\, ]+\]")
brackets_p = re.compile(r"\{.\+?\}")
fl_p = re.compile(r"(First:.+?)?Last: \d{1,2}\/\d{1,2}\/\d{2,4} \d+")
date_ru = re.compile(r"\d{1,2}: \d{1,2}, \d{1,2} \w{3} ")
web_page = re.compile(r".+?\d+ \| .+?\.\w+")
time_p = re.compile("\d+: \d+: (\d+)?:{0,2}")
big_date = re.compile("\d{2,4}[-\.:,/] ?\d{2}[-\.:,/] ?\d{2,4},?")
sec = re.compile("(в )?\d{2}: ?\d{2}")
ru_date = re.compile("\d+ \w+ \d{4}( г.,)?")
en_date = re.compile("\w+ \d{2}, \d{4}")


def check_dash(text):
    return len(dash_p.findall(text)) > 0


def check_num(text):
    return len(numbers_p.findall(text)) > 0


def delete_nums_ru(text):
    return numbers_ru_p.sub("", text)


def delete_big_date(text):
    text = big_date.sub("", text)
    text = sec.sub("", text)
    return text


def check_web(text):
    length = max([0] + [len(i) for i in web_p.findall(text)])
    return length > 1


def delete_web(text):
    if web_p.search(text):
        if web_p.search(text)[0].startswith("http") or web_p.search(text)[0].startswith("www"):
            return web_p.sub("", text)
        else:
            return text
    else:
        return text


def delete_web_ru(text):
    return web_page.sub("", text)


def check_dates(text):
    return len(dates.findall(text)) > 0


def delete_date_ru(text):
    text = date_ru.sub("", text)
    text = time_p.sub("", text)
    text = ru_date.sub("", text)
    return text


def delete_spec(text):
    text = numbers_sqbr.sub("", text)
    text = text.replace(" < ", "")
    text = text.replace("<>", "")
    text = text.replace(" > ", "")
    text = text.replace(" › ", "")
    text = text.replace("» ", "")
    text = text.replace("« ", "")
    text = text.replace("•  •  •", "")
    text = text.replace("[...]", "")
    text = text.replace("::", "")
    text = brackets_p.sub("", text)
    return text


def delete_first_last(text):
    return fl_p.sub("", text)


def check_capitalized(text):
    words = text.split()
    capitalized = sum([int(i.capitalize() == i) for i in words])
    if capitalized > len(words) / 2:
        return True
    else:
        return False


def clean_sent(text):
    text = delete_spec((delete_web(text)))
    text = delete_nums_ru(delete_date_ru(text))
    text = delete_web_ru(text)
    text = delete_big_date(text)
    text = en_date.sub("", text)
    text = text.replace("*** - ", "")
    text = text.strip().lstrip("- ").strip()
    return text


def get_pickle(fp):
    with (open(fp, "rb")) as openfile:
        return pickle.load(openfile)

parser = argparse.ArgumentParser(description='Clean dataset')
parser.add_argument('--file', type=str,
                    help='An optional filepath')
args = parser.parse_args()

if args.file != "":
    print(f"{os.path.splitext(os.path.basename(args.file))[0]} to CSV...")
    df = pd.DataFrame([[sent[0], sent[1], sent[2], sent[3], sent[4], clean_sent(sent[5])] for sent in
                       get_pickle(args.file)], columns=['Group', 'Object1', 'Object2', 'CommonObject', 'Aspect',
                                                        'Sentence'])
    df.to_csv(os.path.join("Csv", os.path.splitext(os.path.basename(args.file))[0] + ".csv"), sep='\t', encoding='utf-8', index=False)
    print(f"Finished...")

else:
    for root, dirs, files in os.walk('result'):
        for file in files:
            if "doc_result" in file:
                print(f"{file} to CSV...")
                filepath = os.path.join(root, file)
                df = pd.DataFrame([[sent[0], sent[1], sent[2], sent[3], sent[4], clean_sent(sent[5])] for sent in
                                   get_pickle(filepath)], columns=['Group', 'Object1', 'Object2', 'CommonObject',
                                                                   'Aspect', 'Sentence'])
                df.to_csv(os.path.join("Csv", os.path.splitext(file)[0] + ".csv"), sep='\t', encoding='utf-8', index=False)
                print(f"Finished...")
