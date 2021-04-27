import pandas as pd

df = pd.read_csv('tree.csv')

new = df["level"].str.split("\(", n = 1, expand = True)
df["page"]= new[0]
df["level"]= new[1]
df.drop(columns =["level"], inplace = True)

print(df)