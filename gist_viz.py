from psycopg2 import sql, connect

conn = connect(f"""
    host='192.168.1.104'
    dbname='mono'
    user='elfarissi'
    """)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS communes_knn (
        c_code varchar(32),
        geom geometry(MultiPolygon, 4326));
        """)

cur.execute("""
    TRUNCATE TABLE communes_knn;
    """)

cur.execute("""
    CREATE INDEX IF NOT EXISTS communes_knn_geom_idx
        ON communes_knn USING gist (geom);
        """)

cur.execute("""
    CREATE TABLE IF NOT EXISTS indices (
        idx_oid serial primary key,
        idx_name varchar);
        """)

cur.execute("""
    TRUNCATE TABLE indices;
    """)

cur.execute("""
    INSERT INTO indices
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
            """)

cur.execute("""
    SELECT idx_oid FROM indices
    WHERE idx_name = 'communes_knn_geom_idx';
    """)

idx_oid = cur.fetchone()[0]


cur.execute("""
    SELECT
        CASE
            WHEN type = 'MULTIPOLYGON' THEN 'POLYGON'
            ELSE type
        END AS type
    FROM geometry_columns
    WHERE f_table_name IN (
        SELECT tablename FROM indices
        JOIN pg_indexes
        ON idx_name = indexname
        WHERE idx_oid::integer = %s);
    """,
            [idx_oid])

g_type = cur.fetchone()[0]


cur.execute("""
    SELECT
        srid
    FROM geometry_columns
    WHERE f_table_name IN (
        SELECT tablename FROM indices
        JOIN pg_indexes
        ON idx_name = indexname
        WHERE idx_oid::integer = %s);
    """,
            [idx_oid])

g_srid = cur.fetchone()[0]


cur.execute("""
    SELECT COUNT(*) FROM communes;
    """)

tuples = cur.fetchone()[0]

# TODO: convert string to int for numeric values


def string_to_list(st=()):
    lst = list(st)
    lst = lst[0].splitlines()
    lst = [" ".join(lst[e].split()) for e in range(len(lst))]
    lst = [[el] for el in lst]
    lst = [sub.split(': ') for subl in lst for sub in subl]

    return(lst)


for i in range(1, tuples + 1):

    cur.execute("""
        INSERT INTO communes_knn
        SELECT
            c.c_code,
            c.geom
        FROM communes c
        ORDER by c.geom <-> (
            SELECT geom FROM communes
            WHERE c_nom = 'Lagouira')
        LIMIT 1
        OFFSET %s;
        """,
                [i - 1])

    cur.execute(f"SELECT gist_stat({idx_oid});")
    stat = cur.fetchone()

    print(stat[0])

    stat = string_to_list(stat)

    key = [i[0] for i in stat]
    value = [i[1] for i in stat]
    for i in range(6):
        value[i] = int(value[i])

    # ? Number of levels
    level = value[0]

    level = [value for value in range(1, level + 1)]

    for l in level:

        if len(level) == 1 or l == 1:

            cur.execute("""
                CREATE TABLE IF NOT EXISTS r_tree_l2 (
                    geom geometry(%s, %s));
                """,
                        [g_type, g_srid])

            cur.execute("""
                TRUNCATE TABLE r_tree_l1""")

            cur.execute("""
                TRUNCATE TABLE r_tree_l2""")

            cur.execute("""
                INSERT INTO r_tree_l2
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq
                """,
                        [g_srid, idx_oid, l])

            conn.commit()

            cur.execute("END TRANSACTION;")

            cur.execute("""
                VACUUM ANALYZE r_tree_l1;""")

            cur.execute("""
                VACUUM ANALYZE r_tree_l2;""")

            cur.execute("NOTIFY qgis, 'refresh qgis';")

            continue

        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS r_tree_l1 (
                    geom geometry(%s, %s));
                """,
                        [g_type, g_srid])

            cur.execute("""
                TRUNCATE TABLE r_tree_l1""")

            cur.execute("""
                INSERT INTO r_tree_l1
                SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
                FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq
                """,
                        [g_srid, idx_oid, l])

            conn.commit()

            cur.execute("END TRANSACTION;")

            cur.execute("""
                VACUUM ANALYZE r_tree_l1;""")

            cur.execute("NOTIFY qgis, 'refresh qgis';")

cur.close()
conn.close()
