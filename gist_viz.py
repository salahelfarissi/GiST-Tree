# gist_viz.py
""" Visualize gist index """
""" Script accepts a command line argument """

from psycopg2 import connect, sql
from func import *
import sys
conn = connect("""
    host=localhost
    dbname=nyc
    user=postgres
    """)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS neighborhoods_knn (
        c_code varchar(32),
        geom geometry(MultiPolygon, 26918));
        """)

cur.execute("""
    TRUNCATE TABLE neighborhoods_knn;
    """)

cur.execute("""
    DROP INDEX IF EXISTS neighborhoods_knn_geom_idx;
    """,
            [])


cur.execute("""
    CREATE INDEX neighborhoods_knn_geom_idx
        ON neighborhoods_knn USING gist (geom);
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
    WHERE idx_name = 'neighborhoods_knn_geom_idx';
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
    SELECT COUNT(*) FROM nyc_neighborhoods;
    """)
num_geometries = cur.fetchone()[0]

num_injections = int(sys.argv[1])

if num_injections > num_geometries:
    num_iterations = num_geometries + 1
else:
    num_iterations = num_injections + 1


def bbox(table, l):

    cur.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            geom geometry(%s));
        """).format(
        sql.Identifier(table)
    ),
        [g_type])

    cur.execute(sql.SQL("""
        TRUNCATE TABLE {} RESTART IDENTITY""").format(
        sql.Identifier(table)
    ))

    cur.execute(sql.SQL("""
        INSERT INTO {}
        SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon)
        FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq
        """).format(
        sql.Identifier(table)
    ),
        [idx_oid, l])

    conn.commit()

    cur.execute("NOTIFY qgis;")


for i in range(1, num_iterations):

    cur.execute("""
        INSERT INTO neighborhoods_knn
        SELECT
            n.gid,
            n.geom
        FROM nyc_neighborhoods n
        ORDER by n.geom <-> (
            SELECT geom FROM nyc_neighborhoods
            WHERE name = 'Tottensville')
        LIMIT 1
        OFFSET %s;
        """,
                [i - 1])

    cur.execute("""
        END;
        """)

    cur.execute("""
        VACUUM ANALYZE neighborhoods_knn;
        """)

    cur.execute(f"SELECT gist_stat({idx_oid});")
    stat = cur.fetchone()

    stat = unpack(stat)

    for key, value in stat.items():
        print(f'{key:<16} : {value:,}')
    print()

    level = stat['Levels']

    level = [value for value in range(1, level + 1)]

    if len(level) == 1:
        bbox('r_tree_l2', 1)

    else:
        for l in level:
            if l == 1:
                bbox('r_tree_l1', 1)
            else:
                bbox('r_tree_l2', 2)

cur.close()
conn.close()
