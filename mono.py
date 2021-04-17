import psycopg2 as p

conn = p.connect(
    host="localhost",
    database="mono",
    user="elfarissi",
    password="%D2a3#PsT"
)

cur = conn.cursor()

cur.execute("select p_code, p_nom from p_09")

rows = cur.fetchall()

for r in rows:
    print(f"p_code {r[0]} p_nom {r[1]}")

cur.close()
conn.close()