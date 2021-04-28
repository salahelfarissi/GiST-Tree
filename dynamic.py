import psycopg2

conn = psycopg2.connect("""
    host=localhost
    dbname=mono
    user=elfarissi
    password='%D2a3#PsT'
    """)

cur = conn.cursor()

# This works, but it is not optimal
table_name = 'my_table'
cur.execute(
    "insert into %s values (%%s, %%s)" % table_name,
    [10, 20])

# table_name1 = 'my_table1'
# cur.execute(
#     "create table %s (id integer, code integer)" % table_name1
#     )

# i = 1
# table_name2 = 'souss.tree' + str(i)
# cur.execute(
# "create table %s (id integer, code integer)" % table_name2
# )

i = 2
table_name2 = 'souss.tree' + str(i)
cur.execute(
"create table %s (id integer, code integer)" % table_name2
)

conn.commit()
cur.close()
conn.close()