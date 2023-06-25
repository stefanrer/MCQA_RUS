import pickle
import os

for root, dirs, files in os.walk('old_docs'):
    for file in files:
        filepath = os.path.join(root, file)
        print(filepath)