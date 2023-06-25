import pandas
import pandas as pd
import os

df = pd.DataFrame()
for root, dirs, files in os.walk('Csv'):
    for file in files:
        filepath = os.path.join(root, file)
        new_df = pandas.read_csv(filepath, sep="\t")
        df = [df, new_df]
        df = pd.concat(df)

df = df.drop_duplicates(subset=['Sentence'], keep='first')
df.to_csv("final_dataset.csv", sep='\t', encoding='utf-8', index=False)