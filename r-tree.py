import psycopg2

# Connect to mono database
conn = psycopg2.connect("dbname=mono user=elfarissi password='%D2a3#PsT'")

# Open a cursor to perform databse operations
cur = conn.cursor()

cur.execute("CREATE EXTENSION IF NOT EXISTS gevel_ext;")

# This creates a table where oid indices will be stored
cur.execute("CREATE TABLE IF NOT EXISTS gist_indices (idx_name varchar, idx_oid varchar);")
cur.execute("TRUNCATE gist_indices RESTART IDENTITY;")

# This lists OIDs of spatial indeces
## (19) WITH gt_name... this lists spatial tables
### (26) SELECT... this returns OID of spatial indices
#### (31) AND c.relname IN (... this lists all spatial indices
cur.execute("""
    INSERT INTO gist_indices

    WITH gt_name AS (
        SELECT
            f_table_name AS t_name
        FROM geometry_columns
    )

    SELECT
        c.relname,
        CAST(c.oid AS INTEGER)
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
cur.execute("SELECT * FROM gist_indices;")
rows = cur.fetchall()

print("\nList of spatial indices\n")

for r in rows:
    print(f"Index: {r[0]}")
    print(f"↳ OID: {r[1]}")

oid = int(input("\nWhich spatial index do you want to visualize?\nOID → "))

cur.execute("""
    SELECT 
        CASE 
            WHEN type = 'MULTIPOLYGON' THEN 'POLYGON'
            ELSE type
        END AS type
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM gist_indices
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
	    SELECT tablename FROM gist_indices
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

cur.execute("DROP TABLE IF EXISTS r_tree;")
cur.execute("CREATE TABLE r_tree (geom geometry((%s)));", (g_type[0], ))

##cur.execute("DROP TABLE IF EXISTS r_tree;")

cur.execute("""
    INSERT INTO r_tree 
    SELECT replace(a::text, '2DF', '')::box2d::geometry(POLYGON, (%s))
    FROM (SELECT * FROM gist_print((%s)) as t(level int, valid bool, a box2df) WHERE level=1) AS subq
    """,
    (g_srid, oid,))

conn.commit()

cur.execute("END TRANSACTION;")
cur.execute("VACUUM ANALYZE r_tree;")
cur.execute("NOTIFY qgis, 'refresh qgis';")

cur.close()
conn.close()
