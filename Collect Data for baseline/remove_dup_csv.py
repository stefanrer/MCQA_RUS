import os
import pandas as pd


def clean_df(cl_df):
    return cl_df.drop_duplicates(subset=['Sentence'], keep='first')


# result_df = df.drop_duplicates(subset=['Column1', 'Column2'], keep='first')
# print(result_df)


for root, dirs, files in os.walk('Csv'):
    for file in files:
        filepath = os.path.join(root, file)
        df = clean_df(pd.read_csv(filepath, sep="\t"))
        df.to_csv(filepath, sep='\t', encoding='utf-8', index=False)
