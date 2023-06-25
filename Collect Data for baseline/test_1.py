import os
import pickle
import gc
from tqdm import tqdm
import argparse


def save_progress(save_idx, save_res, save_set, save_filename):
    with open(os.path.join("result", save_filename + "_result.pickle"), "wb") as rr:
        pickle.dump(save_res, rr)
    with open(os.path.join("result", save_filename + "_start_idx.pickle"), "wb") as rs:
        pickle.dump(save_idx, rs)
    with open(os.path.join("result", save_filename + "_skip_idx_set.pickle"), "wb") as ris:
        pickle.dump(save_set, ris)
    gc.collect()


def compare_sentences(docs, filename):
    if os.path.exists(os.path.join("result", filename + "_result.pickle")):
        with open(os.path.join("result", filename + "_result.pickle"), "rb") as rr:
            result = pickle.load(rr)
        with open(os.path.join("result", filename + "_start_idx.pickle"), "rb") as rs:
            start_idx = pickle.load(rs)
        with open(os.path.join("result", filename + "_skip_idx_set.pickle"), "rb") as ris:
            skip_idx_set = pickle.load(ris)
    else:
        start_idx = 0
        result = []
        skip_idx_set = set()

    gc.collect()

    if start_idx == len(docs):
        print(filename, "Already exist")
        return result

    pbar = tqdm(total=len(docs))
    pbar.set_description("Processing %s" % filename)
    pbar.update(start_idx)
    for idx_doc1, doc1 in enumerate(docs[start_idx:]):
        pbar.update(1)
        if (start_idx + idx_doc1) % 1000 == 0:
            # print(f"Progress: {start_idx + idx_doc1} / {len(docs)}")
            save_progress(start_idx + idx_doc1, result, skip_idx_set, filename)
            gc.collect()

        if not doc1[5].vector_norm or start_idx + idx_doc1 in skip_idx_set:
            continue

        skip_idx_set |= {(idx_doc2, doc2)[0] + start_idx + idx_doc1 + 1 for idx_doc2, doc2 in
                         enumerate(docs[start_idx + idx_doc1 + 1:]) if (
                                     doc2[5].vector_norm and start_idx + idx_doc1 + 1 + idx_doc2 not in skip_idx_set and
                                     doc1[5].similarity(doc2[5]) > 0.97)}
        result.append([docs[idx_doc1][0], docs[idx_doc1][1], docs[idx_doc1][2], docs[idx_doc1][3], docs[idx_doc1][4],
                       docs[idx_doc1][5].text])
    pbar.close()
    with open(os.path.join("result", filename + "_result.pickle"), "wb") as rr:
        pickle.dump(result, rr)
    with open(os.path.join("result", filename + "_start_idx.pickle"), "wb") as rs:
        pickle.dump(len(docs), rs)
    with open(os.path.join("result", filename + "_skip_idx_set.pickle"), "wb") as ris:
        pickle.dump(skip_idx_set, ris)
    del docs
    del skip_idx_set
    gc.collect()
    return result


def clean_data(data_to_clean, filename):
    print(f"Start Cleaning {filename}...")
    data_to_clean = compare_sentences(data_to_clean, filename)
    print(f"Finished Cleaning {filename}, result size: {len(data_to_clean)}")
    del data_to_clean
    gc.collect()
    # return data_to_clean


def get_pickle(fp):
    with (open(fp, "rb")) as openfile:
        return pickle.load(openfile)


parser = argparse.ArgumentParser(description='Clean dataset')
parser.add_argument('--file', type=str,
                    help='An optional filepath')
args = parser.parse_args()

if args.file != "":
    clean_data(get_pickle(args.file), os.path.splitext(os.path.basename(args.file))[0])
    gc.collect()
else:
    for root, dirs, files in os.walk('old_docs'):
        for file in files:
            filepath = os.path.join(root, file)
            # data = clean_data(get_pickle(filepath), os.path.splitext(file)[0])
            clean_data(get_pickle(filepath), os.path.splitext(file)[0])
            gc.collect()


