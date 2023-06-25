import os
import pickle
import gc
from tqdm import tqdm
import argparse


def get_pickle(fp):
    with (open(fp, "rb")) as openfile:
        return pickle.load(openfile)


def compare_sentences(docs, pair):
    result = []
    skip_idx_set = set()
    if len(docs) > 10000:
        docs = docs[:10000]

    pbar = tqdm(total=len(docs))
    pbar.set_description("Processing %s" % pair)
    for idx_doc1, doc1 in enumerate(docs):
        pbar.update(1)

        if not doc1[5].vector_norm or idx_doc1 in skip_idx_set:
            continue

        skip_idx_set |= {(idx_doc2, doc2)[0] + idx_doc1 + 1 for idx_doc2, doc2 in
                         enumerate(docs[idx_doc1 + 1:]) if (
                                     doc2[5].vector_norm and idx_doc1 + 1 + idx_doc2 not in skip_idx_set and
                                     doc1[5].similarity(doc2[5]) > 0.97)}
        result.append([docs[idx_doc1][0], docs[idx_doc1][1], docs[idx_doc1][2], docs[idx_doc1][3], docs[idx_doc1][4],
                       docs[idx_doc1][5]])
    pbar.close()
    del docs
    del skip_idx_set
    gc.collect()
    return result


def clean_data(data_to_clean):
    data = []
    for ob_pair in data_to_clean:
        cleaned_data = compare_sentences(data_to_clean[ob_pair], ob_pair)
        data += cleaned_data
        del cleaned_data
        gc.collect()
    return data


def create_object_dict(data, filename):
    print(f"Creating dict for {filename}")
    doc_dict = {}
    for doc in data:
        object_pair = doc[1]+"_"+doc[2]
        if object_pair not in doc_dict:
            doc_dict[object_pair] = []
        doc_dict[object_pair].append(doc)
    return doc_dict


parser = argparse.ArgumentParser(description='split dataset')
parser.add_argument('--file', type=str, help='An optional filepath')
args = parser.parse_args()

if args.file != "":
    created_dict = create_object_dict(get_pickle(args.file), os.path.splitext(os.path.basename(args.file))[0])
    finish_data = clean_data(created_dict)
    with open(os.path.join("old_docs", os.path.splitext(os.path.basename(args.file))[0] + "_new_wek.pickle"), "wb") as rr:
        pickle.dump(finish_data, rr)
    print(len(finish_data))
    gc.collect()
