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
# TODO: use a combination of tab and newline when interacting with the user
# ? sort & sort(reverse=True) for a query result in psycopg2.
# ? [].sort() method sorts a list permanently otherwise use the sorted([]) method.
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

# * index of the cascade table
cur.execute(f"""
    SELECT * FROM {indices_table}
    WHERE idx_name = 'com_cas_geom_idx';""")
index = cur.fetchone()

cur.execute(f"""
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
	    WHERE idx_oid::integer = {index[0]});
    """)
g_type = cur.fetchone()

cur.execute(f"""SELECT
        srid
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM cascade.indices
	    JOIN pg_indexes
        ON idx_name = indexname
	    WHERE idx_oid::integer = {index[0]});
    """)
g_srid = cur.fetchone()

# * existing communes table
communes_table = 'maroc.communes'

cur.execute(f"""
    CREATE OR REPLACE FUNCTION num_geom() RETURNS INTEGER as $$
        select count(*) from {communes_table};
    $$ LANGUAGE SQL;
    """)


cur.execute("SELECT num_geom();")
count = cur.fetchone()


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


for i in range(1, count[0]+1):
    cur.execute(f"""
        INSERT INTO {new_communes_table}
        select
            c.c_code,
            c.geom
        from maroc.communes c
        order by c.geom <-> (
            select geom from maroc.communes
            where c_nom = 'Lagouira')
        limit 1
        offset {i-1};
        """)

    cur.execute(f"SELECT gist_stat({index[0]});")
    stats = cur.fetchone()

    print(stats[0])

    stats = expandB(stats)

    stats = [sub.split(': ') for subl in stats for sub in subl]

    level = int(stats[0][1])

    level = [value for value in range(1, level+1)]

    for l in level:

        if len(level) == 1:

            relation = {
                'schema': 'level_'+str(l+1),
                'table': 'tree_l'+str(l+1)+'_'+str(i)
            }

            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {relation['schema']};")

            cur.execute(sql.SQL("""
                DROP TABLE IF EXISTS {schema}.{table};
                    """).format(
                schema=sql.Identifier(relation['schema']),
                table=sql.Identifier(relation['table'])))

            cur.execute(sql.SQL("""
                CREATE TABLE {schema}.{table} (geom geometry (%s, %s));""").format(
                schema=sql.Identifier(relation['schema']),
                table=sql.Identifier(relation['table'])),
                [g_type[0], g_srid[0]])

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.{table}
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(relation['schema']),
                table=sql.Identifier(relation['table'])),
                [g_srid[0], index[0], l])

            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.r_tree_l2 (
                    geom geometry(%s, %s));
                """).format(
                schema=sql.Identifier(relation['schema'])
            ),
                [g_type[0], g_srid[0]])

            cur.execute(sql.SQL("""
                TRUNCATE TABLE {schema}.r_tree_l2""").format(
                schema=sql.Identifier(relation['schema'])))

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.r_tree_l2
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(relation['schema'])),
                [g_srid[0], index[0], l])

            conn.commit()

            cur.execute("END TRANSACTION;")

            cur.execute(sql.SQL("""
                VACUUM ANALYZE {schema}.r_tree_l2;""").format(
                schema=sql.Identifier(relation['schema'])))

            cur.execute("NOTIFY qgis, 'refresh qgis';")

        elif l == 1:

            relation = {
                'schema': 'level_'+str(l+1),
                'table': 'tree_l'+str(l+1)+'_'+str(i)
            }

            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {relation['schema']};")

            cur.execute(sql.SQL("""
                DROP TABLE IF EXISTS {schema}.{table};
                    """).format(
                schema=sql.Identifier(relation['schema']),
                table=sql.Identifier(relation['table'])))

            cur.execute(sql.SQL("""
                CREATE TABLE {schema}.{table} (geom geometry (%s, %s));""").format(
                schema=sql.Identifier(relation['schema']),
                table=sql.Identifier(relation['table'])),
                [g_type[0], g_srid[0]])

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.{table}
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(relation['schema']),
                table=sql.Identifier(relation['table'])),
                [g_srid[0], index[0], l])

            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.r_tree_l2 (
                    geom geometry(%s, %s));
                """).format(
                schema=sql.Identifier(relation['schema'])
            ),
                [g_type[0], g_srid[0]])

            cur.execute(sql.SQL("""
                TRUNCATE TABLE {schema}.r_tree_l2""").format(
                schema=sql.Identifier(relation['schema'])))

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.r_tree_l2
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(relation['schema'])),
                [g_srid[0], index[0], l])

            conn.commit()

            cur.execute("END TRANSACTION;")

            cur.execute(sql.SQL("""
                VACUUM ANALYZE {schema}.r_tree_l2;""").format(
                schema=sql.Identifier(relation['schema'])))

            cur.execute("NOTIFY qgis, 'refresh qgis';")

        else:

            schema = 'level_'+str(l)
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

            table = 'tree_l'+str(l)+'_'+str(i)
            cur.execute(sql.SQL("""
                DROP TABLE IF EXISTS {schema}.{table};
                    """).format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table)),
                [schema])

            cur.execute(sql.SQL("""
                CREATE TABLE {schema}.{table} (geom geometry (%s, %s));""").format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table)),
                [g_type[0], g_srid[0]])

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.{table}
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table)),
                [g_srid[0], index[0], l])

            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.r_tree_l1 (
                    geom geometry(%s, %s));
                """).format(
                schema=sql.Identifier(schema)
            ),
                [g_type[0], g_srid[0]])

            cur.execute(sql.SQL("""
                TRUNCATE TABLE {schema}.r_tree_l1""").format(
                schema=sql.Identifier(schema)))

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.r_tree_l1
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(schema)),
                [g_srid[0], index[0], l])

            conn.commit()

            cur.execute("END TRANSACTION;")

            cur.execute(sql.SQL("""
                VACUUM ANALYZE {schema}.r_tree_l1;""").format(
                schema=sql.Identifier(schema)))

            cur.execute("NOTIFY qgis, 'refresh qgis';")

conn.commit()

cur.execute("NOTIFY qgis, 'refresh qgis';")

cur.close()
conn.close()
