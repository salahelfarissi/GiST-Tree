from psycopg2 import sql, connect

psql = {
    'host': '192.168.1.105',
    'dbname': 'mono',
    'user': 'elfarissi'
}

conn = connect(f"""
    host={psql['host']}
    dbname={psql['dbname']}
    password={psql['password']}
    """)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS communes_bbox (
        c_code varchar(32),
        geom geometry(MultiPolygon, 4326));
        """)

cur.execute("""
    TRUNCATE TABLE communes_bbox;
    """)

cur.execute("""
    CREATE INDEX IF NOT EXISTS communes_bbox_geom_idx
        ON communes_bbox USING gist (geom);
        """)

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
    gist_stat = cur.fetchone()

    print(gist_stat[0])

    # ? gist_stat is a string
    # ? expandB fct creates a nested list object that correponds to attributes and their values
    gist_stat = expandB(gist_stat)

    while 1:
        if int(gist_stat[3][1]) == table['tuples']:
            break
        else:
            for e in range(100, table['tuples'] + 1, 100):
                if int(gist_stat[3][1]) == e:
                    prompt = f'You have inserted {int(gist_stat[3][1])} tuples.'
                    prompt += '\nPress Enter to continue.'
                    input(prompt)
        break

    level = int(gist_stat[0][1])

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

            continue

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

            continue

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

# (C) Copyright 2021 by Abdane & El Farissi
# Pr Hajji Hicham. All Rights Reserved.
