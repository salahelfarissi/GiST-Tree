import psycopg2

# Connect to mono database
conn = psycopg2.connect("dbname=mono user=elfarissi password='%D2a3#PsT'")

# Open a cursor to perform databse operations
cur = conn.cursor()

# This creates a new table
cur.execute("DROP TABLE IF EXISTS gist_indices;")
cur.execute("CREATE TABLE gist_indices (idx_name varchar, idx_oid varchar);")

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

print("\nList of GiST indices :\n")

for r in rows:
    print(f"Index: {r[0]}, Identifier: {r[1]}")

oid = input("\nWhich geometry index you want to visualize?\n→ ")

cur.execute("DROP TABLE IF EXISTS r_tree;")

cur.execute("""
    SELECT replace(a::text, '2DF', '')::box2d::geometry as geom
    INTO r_tree
    FROM (SELECT * FROM gist_print((%s)) as t(level int, valid bool, a box2df) WHERE level=1) AS subq
    """,
    (oid,))

conn.commit()

cur.close()
conn.close()