import pandas as pd

df = pd.read_csv('tree.csv')

#new = df["level"].str.split("\(", n = 1, expand = True)
#df["page"]= new[0]
#df["level"]= new[1]
#df.drop(columns =["level"], inplace = True)

df[['page','level']] = df.level.str.split("(",expand=True)
df[['tmp','level']] = df.level.str.split(":",expand=True)
df[['level','tmp']] = df.level.str.split(")",expand=True)
df[['free(Bytes)','occupied']] = df.space.str.split("b",expand=True)
df[['tmp','occupied']] = df.occupied.str.split("(",expand=True)
df[['occupied(%)','tmp']] = df.occupied.str.split("%",expand=True)
df.drop('tmp', inplace=True, axis=1)
df.drop('space', inplace=True, axis=1)
df.drop('occupied', inplace=True, axis=1)

print(df)