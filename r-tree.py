import psycopg2
import csv
# import pandas as pd
# import numpy as np 

# Connect to mono database
conn = psycopg2.connect("dbname=mono user=elfarissi password='%D2a3#PsT'")

# Open a cursor to perform databse operations
cur = conn.cursor()

cur.execute("DROP EXTENSION IF EXISTS gevel_ext;")
cur.execute("CREATE EXTENSION gevel_ext;")

# This creates a table where oid indices will be stored
cur.execute("DROP TABLE IF EXISTS indices;")
cur.execute("CREATE TABLE r_tree.indices (idx_oid serial primary key, idx_name varchar);")

# This lists OIDs of spatial indeces
## (19) WITH gt_name... this lists spatial tables
### (26) SELECT... this returns OID of spatial indices
#### (31) AND c.relname IN (... this lists all spatial indices
cur.execute("""
    INSERT INTO indices

    WITH gt_name AS (
        SELECT
            f_table_name AS t_name
        FROM geometry_columns
    )

    SELECT
        CAST(c.oid AS INTEGER),
        c.relname
    FROM pg_class c, pg_index i
    WHERE c.oid = i.indexrelid
    
    AND c.relname IN (
        SELECT
            relname
        FROM pg_class, pg_index
        WHERE pg_class.oid = pg_index.indexrelid
        AND pg_class.oid IN (
            SELECT
                indexrelid
            FROM pg_index, pg_class
            WHERE pg_class.relname IN (
                SELECT t_name
                FROM gt_name)
            AND pg_class.oid = pg_index.indrelid
            AND indisunique != 't'
            AND indisprimary != 't' ))
""")

# Obtain data as Python objects
cur.execute("SELECT * FROM indices;")
rows = cur.fetchall()

print("\nList of spatial indices\n")

for r in rows:
    print(f"Index: {r[1]}")
    print(f"↳ OID: {r[0]}")

oid = int(input("\nWhich spatial index do you want to visualize?\nOID → "))

cur.execute("""
    SELECT 
        CASE 
            WHEN type = 'MULTIPOLYGON' THEN 'POLYGON'
            ELSE type
        END AS type
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM indices
	    JOIN pg_indexes
        ON idx_name = indexname
	    WHERE idx_oid::integer = (%s));
    """,
    (oid,))
g_type = cur.fetchone()

cur.execute("""
    SELECT 
        srid
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM indices
	    JOIN pg_indexes
        ON idx_name = indexname
	    WHERE idx_oid::integer = (%s));
    """,
    (oid,))
g_srid = cur.fetchone()

print("\nStatistics\n")
cur.execute("SELECT gist_stat((%s));", (oid,))
stats = cur.fetchone()
print(stats[0])

l = list(stats)
l = l[0].splitlines()

for e in range(len(l)):
    l[e] = " ".join(l[e].split())

def extractDigits(lst):
    res = []
    for el in lst:
        sub = el.split(', ')
        res.append(sub)
      
    return(res)

l = extractDigits(l)
l = [sub.split(': ') for subl in l for sub in subl]

print(f"Nombre de niveaux → {l[0][1]}\n")
num_level = int(input("Niveau à visualiser \n↳ "))

cur.execute("select gist_tree((%s), 2);", (oid, ))
tree = cur.fetchone()

t = list(tree)

t = t[0].splitlines()

for e in range(len(t)):
    t[e] = " ".join(t[e].split())

t = extractDigits(t)

t = [sub.split(' ') for subl in t for sub in subl]

with open("tree.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(t)

cur.execute("CREATE TABLE IF NOT EXISTS r_tree.r_tree (geom geometry((%s)));", (g_type[0], ))
cur.execute("TRUNCATE TABLE r_tree RESTART IDENTITY;")

cur.execute("""
    INSERT INTO r_tree 
    SELECT replace(a::text, '2DF', '')::box2d::geometry(POLYGON, (%s))
    FROM (SELECT * FROM gist_print((%s)) as t(level int, valid bool, a box2df) WHERE level = (%s)) AS subq
    """,
    (g_srid, oid, num_level, ))

conn.commit()

cur.execute("END TRANSACTION;")
cur.execute("VACUUM ANALYZE r_tree;")
cur.execute("NOTIFY qgis, 'refresh qgis';")

cur.close()
conn.close()