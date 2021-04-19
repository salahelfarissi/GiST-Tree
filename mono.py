import psycopg2

# Connect to mono database
conn = p.connect("dbname=mono user=elfarissi")

# Open a cursor to perform databse operations
cur = conn.cursor()

# 
cur.execute("select p_code, p_nom from p_09")

rows = cur.fetchall()

for r in rows:
    print(f"p_code {r[0]} p_nom {r[1]}")

cur.close()
conn.close()