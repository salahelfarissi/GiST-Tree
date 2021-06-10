# gist_viz.py
"""Display r-tree bboxes"""
from psycopg2 import connect, sql
from func import *
import pandas as pd

conn = connect("""
    host=localhost
    dbname=nyc
    user=postgres
    """)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS streets_knn (
        geom geometry(MultiLineString, 26918));
        """)

cur.execute("""
    TRUNCATE TABLE streets_knn;
    """)

cur.execute("""
    CREATE INDEX IF NOT EXISTS streets_knn_geom_idx
        ON streets_knn USING gist (geom);
        """)

cur.execute("""
    CREATE TABLE IF NOT EXISTS indices (
        idx_oid serial primary key,
        idx_name varchar);
        """)

cur.execute("""
    TRUNCATE TABLE indices RESTART IDENTITY;
    """)

cur.execute("""
    INSERT INTO indices
    WITH gt_name AS (
        SELECT
            f_table_name AS t_name
        FROM geometry_columns
    )
    SELECT
        CAST(c.oid AS INTEGER) as "OID",
        c.relname as "INDEX"
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
    SELECT * FROM indices;
    """)

indices = pd.Series(dict(cur.fetchall()))
idx_oid = int(indices[indices == 'streets_knn_geom_idx'].index[0])

cur.execute(""" 
    SELECT  
        type 
    FROM geometry_columns 
    WHERE f_table_name IN ( 
	    SELECT tablename FROM indices 
	    JOIN pg_indexes 
        ON idx_name = indexname 
	    WHERE idx_oid::integer = %s); 
    """,
            [idx_oid])
g_type = cur.fetchone()

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
g_srid = cur.fetchone()

cur.execute("""
    SELECT COUNT(*) FROM nyc_streets;
    """)
num_geometries = cur.fetchone()[0]


def bbox(table, l):

    cur.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            geom geometry);
        """).format(
        sql.Identifier(table)
    ))

    cur.execute(sql.SQL("""
        TRUNCATE TABLE {}""").format(
        sql.Identifier(table)
    ))

    cur.execute(sql.SQL("""
        INSERT INTO {}
        SELECT st_setsrid(replace(a::text, '2DF', '')::box2d::geometry, %s)
        FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq
        """).format(
        sql.Identifier(table)
    ),
        [g_srid[0], idx_oid, l])

    cur.execute("NOTIFY qgis;")


for i in range(1, num_geometries + 1):

    cur.execute("""
        INSERT INTO streets_knn
        SELECT
            n.geom
        FROM nyc_streets n
        ORDER by n.geom <-> (
            SELECT geom FROM nyc_streets
            WHERE name = 'Tottensville')
        LIMIT 1
        OFFSET %s;
        """,
                [i - 1])

    cur.execute("""
        END;
        """)

    cur.execute("""
        VACUUM ANALYZE streets_knn;
        """)

    if i == 1:
        stat = dict()
    cur.execute(f"SELECT gist_stat({idx_oid});")

    for key, value in unpack(cur.fetchone()).items():
        if key in stat:
            stat[key].append(value)
        else:
            stat[key] = [value]

    for key, value in stat.items():
        print(f'{key:<16} : {value[i - 1]:,}')
    print()

    level = stat['Levels'][i - 1]

    level = [value for value in range(1, level + 1)]

    if len(level) == 1:
        bbox('r_tree_l2', 1)

    else:
        for l in level:
            if l == 1:
                bbox('r_tree_l1', 1)
            else:
                bbox('r_tree_l2', 2)

conn.commit()

cur.close()
conn.close()
