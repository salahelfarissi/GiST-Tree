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

print("\nList of GiST indices :\n")

for r in rows:
    print(f"Index: {r[0]}, Identifier: {r[1]}")

oid = int(input("\nWhich geometry index you want to visualize?\n→ "))

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

cur.execute("CREATE TABLE IF NOT EXISTS r_tree (geom geometry((%s), (%s)));", (g_type[0], g_srid[0],))
cur.execute("TRUNCATE r_tree RESTART IDENTITY;")

##cur.execute("DROP TABLE IF EXISTS r_tree;")

cur.execute("""
    INSERT INTO r_tree 
    SELECT replace(a::text, '2DF', '')::box2d::geometry
    FROM (SELECT * FROM gist_print((%s)) as t(level int, valid bool, a box2df) WHERE level=1) AS subq
    """,
    (oid,))

conn.commit()

cur.execute("END TRANSACTION;")
cur.execute("VACUUM ANALYZE r_tree;")
cur.execute("NOTIFY qgis, 'refresh qgis';")

cur.close()
conn.close()