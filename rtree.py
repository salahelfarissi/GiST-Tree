import psycopg2

# Connect to mono database
conn = p.connect("dbname=mono user=elfarissi")

# Open a cursor to perform databse operations
cur = conn.cursor()

# Execute a command: this lists spatial tables
cur.execute("SELECT f_table_name AS nom_table FROM geometry_columns;")

rows = cur.fetchall()

for r in rows:
    print(f"p_code {r[0]} p_nom {r[1]}")

cur.close()
conn.close()