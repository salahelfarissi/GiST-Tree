import psycopg2
from psycopg2 import sql

# * Connect to an existing database
login = ['localhost', 'mono', '%D2a3#PsT']

conn = psycopg2.connect(f"""
    host={login[0]}
    dbname={login[1]}
    password={login[2]}
    """)

# * Open a cursor to perform database operations
cur = conn.cursor()

# * Cascade schema will hold R-Tree bbox generated through iteration
schema = 'cascade_c_09'
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

# * I will insert geometries from communes table into com_cas table through iteration
new_c_09_table = schema+'.c_09_cas'

cur.execute(f"""
    CREATE TABLE {new_c_09_table} (
        c_code varchar(32),
        geom geometry(MultiPolygon, 26192));""")

cur.execute(f"TRUNCATE TABLE {new_c_09_table};")

new_index = 'c_09_cas_geom_idx'
cur.execute(f"""
    CREATE INDEX IF NOT EXISTS {new_index}
        ON {new_c_09_table} USING gist
        (geom);""")

# TODO: use f-strings in subsequent queries
# TODO: use a combination of tab and newline when interacting with the user
indices_table = 'cascade.indices'
cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {indices_table} (
    idx_oid serial primary key,
    idx_name varchar);
    """)

cur.execute(f"TRUNCATE TABLE {indices_table};")

cur.execute(f"""INSERT INTO {indices_table}
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

cur.execute(f"""
    SELECT * FROM {indices_table}
    WHERE idx_name = 'com_cas_geom_idx';""")
index = cur.fetchone()
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

c_09_table = 'souss.c_09'

cur.execute(f"""
    CREATE OR REPLACE FUNCTION num_geom() RETURNS INTEGER as $$
        select count(*) from {c_09_table};
    $$ LANGUAGE SQL;
    """)


cur.execute("SELECT num_geom();")
count = cur.fetchone()

for i in range(count[0]):
    cur.execute(f"""
        INSERT INTO {new_c_09_table}
        select
            c.c_code,
            c.geom
        from souss.c_09 c
        order by c.geom <-> (
            select geom from souss.c_09
            where c_nom = 'Allougoum')
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
        DROP TABLE IF EXISTS cascade_c_09.{table};
            """).format(
        table=sql.Identifier('tree_'+str(i))))

    cur.execute(sql.SQL("""
        CREATE TABLE cascade_c_09.{table} (geom geometry (%s, %s));""").format(
        table=sql.Identifier('tree_'+str(i))),
        [g_type[0], g_srid[0]])

    cur.execute(sql.SQL("""
        INSERT INTO cascade.{table}
        SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
        FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = 1) AS subq""").format(
        table=sql.Identifier('tree_'+str(i))),
        [g_srid[0], index])

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cascade_c_09.r_tree (
            geom geometry(%s, %s));
        """,
                [g_type[0], g_srid[0]])

    cur.execute("TRUNCATE TABLE cascade_c_09.r_tree")

    cur.execute(f"""
            INSERT INTO cascade_c_09.r_tree
            SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
            FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = 1) AS subq""",
                [g_srid[0], index])

    conn.commit()

    cur.execute("END TRANSACTION;")

    cur.execute(sql.SQL("""
        VACUUM ANALYZE cascade_c_09.{table};""").format(
        table=sql.Identifier('tree_'+str(i))))

    cur.execute("VACUUM ANALYZE cascade_c_09.r_tree;")

    cur.execute("NOTIFY qgis, 'refresh qgis';")

conn.commit()
cur.execute("END TRANSACTION;")
cur.execute("VACUUM ANALYZE cascade_c_09.c_09_cas;")
cur.execute("NOTIFY qgis, 'refresh qgis';")
cur.close()
conn.close()
