import psycopg2

# Connect to db
conn = psycopg2.connect("""
    host=192.168.1.104
    dbname=mono
    user=elfarissi
    password='%D2a3#PsT'
    """)

# Cursor
cur = conn.cursor()

cur.execute("""
    DROP TABLE IF EXISTS cascade.com_cas;
    """)

cur.execute("""
    DROP TABLE IF EXISTS cascade.indices;
    """)

cur.execute("""
    DROP TABLE IF EXISTS cascade.r_tree_l1;
    """)

cur.execute("""
    DROP TABLE IF EXISTS cascade.r_tree_l2;
    """)

cur.execute("SELECT count(*) FROM maroc.communes;")
count = cur.fetchone()

for i in range (count[0]):
    table_name = 'cascade.tree_'+str(i)
    cur.execute("""
        DROP TABLE IF EXISTS %s;
        """ % table_name)

conn.commit()
cur.execute("END TRANSACTION;")
cur.close()
conn.close()