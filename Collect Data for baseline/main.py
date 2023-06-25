import json
import os
import spacy
import h5py
import pickle
import numpy as np
import pandas as pd
import gc
import itertools


framecopy = pd.HDFStore(os.path.join("nlp", "embeddings.h5"))
dataset = list(itertools.chain.from_iterable([framecopy[s].values.tolist() for s in framecopy]))
for dataset in framecopy:
    df = framecopy[dataset]
    print(df.size)
l = framecopy.values.tolist()
print(len(l))
print(len(dataset))
framecopy.close()
