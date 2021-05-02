import psycopg2
from psycopg2 import sql

conn = psycopg2.connect("""
    host=192.168.1.107
    dbname=mono
    user=elfarissi
    password='%D2a3#PsT'
    """)

cur = conn.cursor()

for i in range(3):

    cur.execute(sql.SQL("""
        CREATE TABLE {table} (id {type});
        """).format(
        table=sql.Identifier('table_1'+str(i)),
        type=sql.SQL('integer')))

conn.commit()

cur.close()
conn.close()
