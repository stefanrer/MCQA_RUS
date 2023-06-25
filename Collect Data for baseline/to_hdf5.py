import json
import os
import spacy
import h5py
import pickle
import numpy as np
import pandas as pd
import gc
import itertools


def get_pickle(fp):
    with (open(fp, "rb")) as openfile:
        return pickle.load(openfile)


dataset = 0

for root, dirs, files in os.walk('old_docs'):
    for file in files:
        filepath = os.path.join(root, file)
        data = get_pickle(filepath)
        gc.collect()
        half_length = len(data) // 2
        first_half, second_half = data[:half_length], data[half_length:]
        del data
        gc.collect()
        half_length = len(first_half) // 2
        first_fourth, second_fourth, three_fourth, four_fourth = first_half[:half_length], first_half[half_length:], second_half[:half_length], second_half[half_length:]
        del first_half
        del second_half
        gc.collect()
        df1 = pd.DataFrame(first_fourth, columns=['Group', 'Object1', 'Object2', 'CommonObject', 'Aspect', 'Embeddings'])
        del first_fourth
        gc.collect()
        df2 = pd.DataFrame(second_fourth, columns=['Group', 'Object1', 'Object2', 'CommonObject', 'Aspect', 'Embeddings'])
        del second_fourth
        gc.collect()
        df3 = pd.DataFrame(three_fourth, columns=['Group', 'Object1', 'Object2', 'CommonObject', 'Aspect', 'Embeddings'])
        del three_fourth
        gc.collect()
        df4 = pd.DataFrame(four_fourth, columns=['Group', 'Object1', 'Object2', 'CommonObject', 'Aspect', 'Embeddings'])
        del four_fourth
        gc.collect()
        df1.to_hdf(os.path.join("nlp", "embeddings.h5"), 'dataseta' + str(dataset), mode='a')
        del df1
        gc.collect()
        df2.to_hdf(os.path.join("nlp", "embeddings.h5"), 'datasetb' + str(dataset), mode='a')
        del df2
        gc.collect()
        df3.to_hdf(os.path.join("nlp", "embeddings.h5"), 'datasetc' + str(dataset), mode='a')
        del df3
        gc.collect()
        df4.to_hdf(os.path.join("nlp", "embeddings.h5"), 'datasetd' + str(dataset), mode='a')
        del df4
        gc.collect()
        print(f"Finished file: {filepath}")
        dataset += 1



