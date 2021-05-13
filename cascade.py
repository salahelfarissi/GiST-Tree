import psycopg2
from psycopg2 import sql

psql = {
    'host': 'localhost',
    'dbname': 'mono',
    'password': '%D2a3#PsT'
}

conn = psycopg2.connect(f"""
    host={psql['host']}
    dbname={psql['dbname']}
    password={psql['password']}
    """)

cur = conn.cursor()

table = {
    'schema': 'maroc',
    'table': 'communes'
}

new_table = {
    'schema': 'cascade',
    'table': 'communes',
    'index': 'new_communes_geom_idx'
}

indices = {
    'schema': new_table['schema'],
    'table': 'indices'
}


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


cur.execute(sql.SQL("""
    CREATE SCHEMA IF NOT EXISTS {schema};
    """).format(
    schema=sql.Identifier(new_table['schema'])))

cur.execute(sql.SQL("""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        c_code varchar(32),
        geom geometry(MultiPolygon, 4326));
        """).format(
            schema=sql.Identifier(new_table['schema']),
            table=sql.Identifier(new_table['table'])))

cur.execute(sql.SQL("""
    TRUNCATE TABLE {schema}.{table};
    """).format(
    schema=sql.Identifier(new_table['schema']),
    table=sql.Identifier(new_table['table'])))

cur.execute(sql.SQL("""
    CREATE INDEX IF NOT EXISTS {index}
        ON {schema}.{table} USING gist (geom);
        """).format(
            index=sql.Identifier(new_table['index']),
            schema=sql.Identifier(new_table['schema']),
            table=sql.Identifier(new_table['table'])))

cur.execute(sql.SQL("""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        idx_oid serial primary key,
        idx_name varchar);
        """).format(
            schema=sql.Identifier(indices['schema']),
            table=sql.Identifier(indices['table'])))

cur.execute(sql.SQL("""TRUNCATE TABLE {schema}.{table};
""").format(
    schema=sql.Identifier(indices['schema']),
    table=sql.Identifier(indices['table'])))

cur.execute(sql.SQL("""
    INSERT INTO {schema}.{table}
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
            AND indisprimary != 't' ));
            """).format(
    schema=sql.Identifier(indices['schema']),
    table=sql.Identifier(indices['table'])))

cur.execute(sql.SQL("""
    SELECT idx_oid FROM {schema}.{table}
    WHERE idx_name = %s;
    """).format(
    schema=sql.Identifier(indices['schema']),
    table=sql.Identifier(indices['table'])),
    [new_table['index']])

new_table['idx_oid'] = cur.fetchone()[0]

cur.execute(sql.SQL("""
    SELECT
        CASE
            WHEN type = 'MULTIPOLYGON' THEN 'POLYGON'
            ELSE type
        END AS type
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM {schema}.{table}
	    JOIN pg_indexes
        ON idx_name = indexname
	    WHERE idx_oid::integer = %s);
    """).format(
    schema=sql.Identifier(indices['schema']),
    table=sql.Identifier(indices['table'])),
    [new_table['idx_oid']])

new_table['type'] = cur.fetchone()[0]

cur.execute(sql.SQL("""
    SELECT
        srid
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM {schema}.{table}
	    JOIN pg_indexes
        ON idx_name = indexname
	    WHERE idx_oid::integer = %s);
    """).format(
    schema=sql.Identifier(indices['schema']),
    table=sql.Identifier(indices['table'])),
    [new_table['idx_oid']])

new_table['srid'] = cur.fetchone()[0]

cur.execute(sql.SQL("""
    SELECT COUNT(*) FROM {schema}.{table};
    """).format(
    schema=sql.Identifier(table['schema']),
    table=sql.Identifier(table['table'])))

table['tuples'] = cur.fetchone()[0]

for i in range(1, table['tuples']+1):
    cur.execute(sql.SQL("""
        INSERT INTO {schema}.{table}
        SELECT
            c.c_code,
            c.geom
        FROM {new_schema}.{new_table} c
        ORDER by c.geom <-> (
            SELECT geom FROM {new_schema}.{new_table}
            WHERE c_nom = 'Lagouira')
        LIMIT 1
        OFFSET %s;
        """).format(
        schema=sql.Identifier(new_table['schema']),
        table=sql.Identifier(new_table['table']),
        new_schema=sql.Identifier(table['schema']),
        new_table=sql.Identifier(table['table'])),
        [i-1])

    cur.execute(f"SELECT gist_stat({new_table['idx_oid']});")
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
                [new_table['type'], new_table['srid']])

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.{table}
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(relation['schema']),
                table=sql.Identifier(relation['table'])),
                [new_table['srid'], new_table['idx_oid'], l])

            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.r_tree_l2 (
                    geom geometry(%s, %s));
                """).format(
                schema=sql.Identifier(relation['schema'])
            ),
                [new_table['type'], new_table['srid']])

            cur.execute(sql.SQL("""
                TRUNCATE TABLE {schema}.r_tree_l2""").format(
                schema=sql.Identifier(relation['schema'])))

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.r_tree_l2
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(relation['schema'])),
                [new_table['srid'], new_table['idx_oid'], l])

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
                [new_table['type'], new_table['srid']])

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.{table}
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(relation['schema']),
                table=sql.Identifier(relation['table'])),
                [new_table['srid'], new_table['idx_oid'], l])

            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.r_tree_l2 (
                    geom geometry(%s, %s));
                """).format(
                schema=sql.Identifier(relation['schema'])
            ),
                [new_table['type'], new_table['srid']])

            cur.execute(sql.SQL("""
                TRUNCATE TABLE {schema}.r_tree_l2""").format(
                schema=sql.Identifier(relation['schema'])))

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.r_tree_l2
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(relation['schema'])),
                [new_table['srid'], new_table['idx_oid'], l])

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
                [new_table['type'], new_table['srid']])

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.{table}
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(schema),
                table=sql.Identifier(table)),
                [new_table['srid'], new_table['idx_oid'], l])

            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {schema}.r_tree_l1 (
                    geom geometry(%s, %s));
                """).format(
                schema=sql.Identifier(schema)
            ),
                [new_table['type'], new_table['srid']])

            cur.execute(sql.SQL("""
                TRUNCATE TABLE {schema}.r_tree_l1""").format(
                schema=sql.Identifier(schema)))

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.r_tree_l1
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(schema)),
                [new_table['srid'], new_table['idx_oid'], l])

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
