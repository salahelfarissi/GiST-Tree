import pandas as pd

df = pd.read_csv('tree.csv')
df.drop('col8', inplace=True, axis=1)
df.drop('col9', inplace=True, axis=1)
df.drop('col2', inplace=True, axis=1)
df.drop('col4', inplace=True, axis=1)
df.drop('col6', inplace=True, axis=1)

print(df)