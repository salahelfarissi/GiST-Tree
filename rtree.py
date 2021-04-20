import psycopg2

# Connect to mono database
conn = psycopg2.connect("dbname=mono user=elfarissi password='%D2a3#PsT'")

# Open a cursor to perform databse operations
cur = conn.cursor()

# This creates a new table
cur.execute("DROP TABLE IF EXISTS gist_indices;")
cur.execute("CREATE TABLE gist_indices (idx_name varchar, idx_oid varchar);")

# This lists OIDs of spatial indeces
cur.execute(" INSERT INTO gist_indices WITH nom_table AS ( \
        SELECT f_table_name AS nom_table \
        FROM geometry_columns \
    ) \
SELECT \
    relname, \
    CAST(c.oid AS INTEGER) FROM pg_class c, pg_index i  \
WHERE c.oid = i.indexrelid and c.relname IN ( \
    WITH nom_table AS ( \
        SELECT f_table_name AS nom_table \
        FROM geometry_columns \
        ) \
    SELECT \
        relname \
    FROM pg_class, pg_index \
    WHERE pg_class.oid = pg_index.indexrelid \
    AND pg_class.oid IN ( \
        SELECT indexrelid FROM pg_index, pg_class \
        WHERE pg_class.relname IN (SELECT nom_table FROM nom_table) \
        AND pg_class.oid=pg_index.indrelid \
        AND indisunique != 't' \
        AND indisprimary != 't' ) \
);")

# Obtain data as Python objects
cur.execute("SELECT * FROM gist_indices;")

rows = cur.fetchall()

for r in rows:
    print(r)

cur.execute("DROP TABLE IF EXISTS r_tree;")

cur.execute("""
    SELECT replace(a::text, '2DF', '')::box2d::geometry as geom
    INTO r_tree
    FROM (SELECT * FROM gist_print((%s)) as t(level int, valid bool, a box2df) WHERE level =1) AS subq
    """,
    ('29334',))

conn.commit()

cur.close()
conn.close()