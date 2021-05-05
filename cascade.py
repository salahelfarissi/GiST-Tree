import psycopg2
from psycopg2 import sql

# * Connect to an existing database
host = 'localhost'
dbname = 'mono'
password = '%D2a3#PsT'
conn = psycopg2.connect(f"""
    host={host}
    dbname={dbname}
    password={password}
    """)

# * Open a cursor to perform database operations
cur = conn.cursor()

# * Cascade schema will hold R-Tree bbox generated through iteration
schema = 'cascade'
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

# * I will insert geometries from communes table into com_cas table through iteration
new_communes_table = 'cascade.com_cas'
cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {new_communes_table} (
        c_code varchar(32),
        geom geometry(MultiPolygon, 4326));""")

cur.execute(f"TRUNCATE TABLE {new_communes_table};")

new_index = 'com_cas_geom_idx'
cur.execute(f"""
    CREATE INDEX IF NOT EXISTS {new_index}
        ON {new_communes_table} USING gist
        (geom);""")

# TODO: use f-strings in subsequent queries
cur.execute("""
    CREATE TABLE IF NOT EXISTS cascade.indices (
    idx_oid serial primary key,
    idx_name varchar);
    """)

cur.execute("TRUNCATE TABLE cascade.indices;")

cur.execute("""INSERT INTO cascade.indices
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
            AND indisprimary != 't' ))""")

cur.execute("""
    SELECT * FROM cascade.indices
    WHERE idx_name = 'com_cas_geom_idx';""")
index = cur.fetchone()
index = list(index)
index = index[0]

cur.execute("""
    SELECT
        CASE
            WHEN type = 'MULTIPOLYGON' THEN 'POLYGON'
            ELSE type
        END AS type
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM cascade.indices
	    JOIN pg_indexes
        ON idx_name = indexname
	    WHERE idx_oid::integer = %s);
    """,
            [index])
g_type = cur.fetchone()

cur.execute("""SELECT
        srid
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM cascade.indices
	    JOIN pg_indexes
        ON idx_name = indexname
	    WHERE idx_oid::integer = %s);
    """,
            [index])
g_srid = cur.fetchone()

cur.execute("SELECT count(*) FROM communes;")
count = cur.fetchone()

for i in range(count[0]):
    cur.execute("""
        INSERT INTO cascade.com_cas
        select
            c.c_code,
            c.geom
        from maroc.communes c
        order by c.geom <-> (
            select geom from maroc.communes
            where c_nom = 'Lagouira')
        limit 1
        offset %(int)s;
        """,
                {'int': i})

    cur.execute("SELECT gist_stat(%s);", [index])
    stats = cur.fetchone()

    print(stats[0])

    def extractDigits(lst):
        res = []
        for el in lst:
            sub = el.split(', ')
            res.append(sub)

        return(res)

    def expandB(lst):
        tmp = list(lst)
        tmp = tmp[0].splitlines()
        for e in range(len(tmp)):
            tmp[e] = " ".join(tmp[e].split())
        tmp = extractDigits(tmp)

        return(tmp)

    stats = expandB(stats)

    stats = [sub.split(': ') for subl in stats for sub in subl]

    cur.execute(sql.SQL("""
        DROP TABLE IF EXISTS cascade.{table};
            """).format(
        table=sql.Identifier('tree_'+str(i))))

    cur.execute(sql.SQL("""
        CREATE TABLE cascade.{table} (geom geometry (%s, %s));""").format(
        table=sql.Identifier('tree_'+str(i))),
        [g_type[0], g_srid[0]])

    cur.execute(sql.SQL("""
        INSERT INTO cascade.{table}
        SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
        FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = 1) AS subq""").format(
        table=sql.Identifier('tree_'+str(i))),
        [g_srid[0], index])

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cascade.r_tree (
            geom geometry(%s, %s));
        """,
                [g_type[0], g_srid[0]])

    cur.execute("TRUNCATE TABLE cascade.r_tree")

    cur.execute("""
            INSERT INTO cascade.r_tree
            SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
            FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = 1) AS subq""",
                [g_srid[0], index])

    conn.commit()

    cur.execute("END TRANSACTION;")

    cur.execute(sql.SQL("""
        VACUUM ANALYZE cascade.{table};""").format(
        table=sql.Identifier('tree_'+str(i))))

    cur.execute("VACUUM ANALYZE cascade.r_tree;")

    cur.execute("NOTIFY qgis, 'refresh qgis';")

conn.commit()
cur.execute("END TRANSACTION;")
cur.execute("VACUUM ANALYZE cascade.com_cas;")
cur.execute("NOTIFY qgis, 'refresh qgis';")
cur.close()
conn.close()
