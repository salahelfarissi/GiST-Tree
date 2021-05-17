from psycopg2 import sql, connect
from functions import *

# TODO: use prompt +="\n..."#
# ? python ---.py
# ? change ip address

# * Define variables
psql = {
    'host': '192.168.1.105',
    'dbname': 'mono',
    'password': '%D2a3#PsT'
}

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

# * Connect to PostgeSQL
conn = connect(f"""
    host={psql['host']}
    dbname={psql['dbname']}
    password={psql['password']}
    """)

cur = conn.cursor()

# * Create a new table
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

# * Retrieve the index identifier of the new table
new_table['idx_oid'] = index(
    indices['schema'], indices['table'], new_table['index'])

# * Retrieve type and srid
# * type is used by Python
new_table['type'] = g_type(
    indices['schema'], indices['table'], new_table['idx_oid'])

new_table['srid'] = g_srid(
    indices['schema'], indices['table'], new_table['idx_oid'])

# * Number of rows (geometries)
table['tuples'] = count(
    table['schema'], table['table'])


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

    if int(stats[3][1]) in list(range(100, table['tuples'] + 1, 100)):
        prompt = f'You have inserted {i} tuples.'
        prompt += '\nPress Enter to continue.'

        _prompt = input(prompt)

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

            relation = {
                'schema': 'level_'+str(l),
                'table': 'tree_l'+str(l)+'_'+str(i)
            }

            cur.execute(
                f"CREATE SCHEMA IF NOT EXISTS {relation['schema']};")

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
                CREATE TABLE IF NOT EXISTS {schema}.r_tree_l1 (
                    geom geometry(%s, %s));
                """).format(
                schema=sql.Identifier(relation['schema'])
            ),
                [new_table['type'], new_table['srid']])

            cur.execute(sql.SQL("""
                TRUNCATE TABLE {schema}.r_tree_l1""").format(
                schema=sql.Identifier(relation['schema'])))

            cur.execute(sql.SQL("""
                INSERT INTO {schema}.r_tree_l1
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq""").format(
                schema=sql.Identifier(relation['schema'])),
                [new_table['srid'], new_table['idx_oid'], l])

            conn.commit()

            cur.execute("END TRANSACTION;")

            cur.execute(sql.SQL("""
                VACUUM ANALYZE {schema}.r_tree_l1;""").format(
                schema=sql.Identifier(relation['schema'])))

            cur.execute("NOTIFY qgis, 'refresh qgis';")

conn.commit()

cur.execute("NOTIFY qgis, 'refresh qgis';")

cur.close()
conn.close()

# (C) Copyright 2021 by Abdane & El Farissi and
# Pr Hajji Hicham. All Rights Reserved.
