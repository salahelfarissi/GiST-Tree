from psycopg2 import sql, connect

psql = {
    'host': '192.168.1.105',
    'dbname': 'mono',
    'password': '%D2a3#PsT'
}

conn = connect(f"""
host={psql['host']}
dbname={psql['dbname']}
password={psql['password']}
""")

cur = conn.cursor()


def tree(level, tuple):
    relation = {
        'schema': 'level_'+str(level+1),
        'table': 'tree_l'+str(level+1)+'_'+str(tuple)
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
    tmp = [sub.split(': ') for subl in tmp for sub in subl]

    return(tmp)


def index(arg1, arg2, arg3):
    # * GiST indices of spatial tables
    cur.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            idx_oid serial primary key,
            idx_name varchar);
            """).format(
                schema=sql.Identifier(arg1),
                table=sql.Identifier(arg2)))

    cur.execute(sql.SQL("""
        TRUNCATE TABLE {schema}.{table};
        """).format(
        schema=sql.Identifier(arg1),
        table=sql.Identifier(arg2)))

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
        schema=sql.Identifier(arg1),
        table=sql.Identifier(arg2)))

    cur.execute(sql.SQL("""
        SELECT idx_oid FROM {schema}.{table}
        WHERE idx_name = %s;
        """).format(
        schema=sql.Identifier(arg1),
        table=sql.Identifier(arg2)),
        [arg3])

    return cur.fetchone()[0]


def g_type(arg1, arg2, arg3):
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
        schema=sql.Identifier(arg1),
        table=sql.Identifier(arg2)),
        [arg3])

    return cur.fetchone()[0]


def g_srid(arg1, arg2, arg3):
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
        schema=sql.Identifier(arg1),
        table=sql.Identifier(arg2)),
        [arg3])

    return cur.fetchone()[0]


def count(arg1, arg2):
    cur.execute(sql.SQL("""
        SELECT COUNT(*) FROM {schema}.{table};
        """).format(
        schema=sql.Identifier(arg1),
        table=sql.Identifier(arg2)))

    return cur.fetchone()[0]
