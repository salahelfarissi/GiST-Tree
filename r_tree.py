# r_tree.py
"""Display r-tree bboxes"""
from psycopg2 import connect
from func import *  # user defined functions
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# Use connect class to establish connection to PostgreSQL
conn = connect("""
    host=192.168.1.100
    dbname=mono
    user='elfarissi'
    """)

cur = conn.cursor()

cur.execute("""
    CREATE SCHEMA IF NOT EXISTS gist;
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
    SELECT * FROM indices;
    """)

indices = cur.fetchall()

w, vw = max_len(indices)  # field width

# Display a two column table with index and oid
print(f'\n{"Index":>{w}}{"OID":>{vw}}', '-'*30, sep='\n')

for oid, name in indices:
    print(f'{name:>{w}}{oid:>{vw}}')

# Ask the user which index to visualize
try:
    idx_oid = int(input("""
        \nWhich GiST index do you want to visualize?\nOID → """))
except ValueError:
    idx_oid = int(input("""
        \nYou must enter an integer value!\nOID → """))

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

cur.execute(f"SELECT gist_stat({idx_oid});")
stat = cur.fetchone()

stat = unpack(stat)

print(f"\nNumber of levels → {stat['Levels']}\n")
level = int(input("Level to visualize \n↳ "))

cur.execute("""
    CREATE TABLE IF NOT EXISTS gist.r_tree (
        id serial primary key,
        area_km2 numeric,
        geom geometry(%s));
    """,
            [g_type[0]])

cur.execute("TRUNCATE TABLE gist.r_tree RESTART IDENTITY;")

cur.execute("""
    INSERT INTO gist.r_tree (geom)
    SELECT replace(a::text, '2DF', '')::box2d::geometry(POLYGON, %s)
    FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq
    """,
            [g_srid[0], idx_oid, level])

cur.execute("""
    UPDATE gist.r_tree
    SET area_km2 = round((st_area(geom)/1000)::numeric, 2);
    """)

""" Graphing total size in bytes of index entities """
bytes = np.array([_ for _ in stat.values()][6:])
values = np.array([_ for _ in stat.keys()][6:])

title = 'R-Tree'
sns.set_style('whitegrid')  # white backround with gray grid lines
axes = sns.barplot(values, bytes, palette='pastel')  # create bars
axes.set_title(title)  # set graph title
axes.set(xlabel='Memory', ylabel='Size')  # label the axes

# scale y-axis by 10% to make room for text above bars
axes.set_ylim(top=max(bytes) * 1.10)

# display frequency & percentage above each patch (bar)
for bar, byte in zip(axes.patches, bytes):
    text_x = bar.get_x() + bar.get_width() / 2.0
    text_y = bar.get_height()
    text = f'{byte:,} bytes\n{byte / sum(bytes):.2%}'
    axes.text(text_x, text_y, text,
              fontsize=11, ha='center', va='bottom')

plt.show()  # display graph

conn.commit()

cur.execute("END TRANSACTION;")
cur.execute("VACUUM ANALYZE gist.r_tree;")
cur.execute("NOTIFY qgis, 'refresh qgis';")

cur.close()
conn.close()
