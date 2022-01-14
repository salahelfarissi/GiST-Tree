# r_tree.py
"""Display r-tree bboxes"""
from psycopg2 import connect
from func import *  # user defined functions
import pandas as pd

# Use connect class to establish connection to PostgreSQL
conn = connect(
    """
    host=localhost
    dbname=nyc
    user=postgres
    """
)

cur = conn.cursor()

# You find a sql function to execute beforehand in queries folder
cur.execute(
    """
    SELECT * FROM indices();
            """
)

indices = cur.fetchall()

# Display a two column table with index and oid
w1, w2 = field_width(indices)

print(f'\n{"Index":>{w1}}{"OID":>{w2}}', "-" * (w1 + w2), sep="\n")

for oid, name in indices:
    print(f"{name:>{w1}}{oid:>{w2}}")

# Ask the user which index to visualize
try:
    idx_oid = int(
        input(
            """
        \nWhich GiST index do you want to visualize?\nOID → """
        )
    )
except ValueError:
    idx_oid = int(
        input(
            """
        \nYou must enter an integer value!\nOID → """
        )
    )

# g_srid() function must be created beforehand (queries folder)
cur.execute("SELECT g_srid(%s);", [idx_oid])
g_srid = cur.fetchone()

cur.execute(f"SELECT gist_stat({idx_oid});")
stat = pd.Series(unpack(cur.fetchone()))

print(f"\nTree has a depth of {stat.Levels}.\n")
level = int(input("Which level do you want to visualize?\nLevel → "))

print("\n¯\_(ツ)_/¯\n")

cur.execute(
    """
    DROP TABLE IF EXISTS r_tree;
    """
)

cur.execute(
    """
    CREATE TABLE r_tree (
        id serial primary key,
        geom geometry(POLYGON, %s));
    """,
    [g_srid[0]],
)

cur.execute(
    """
    INSERT INTO r_tree (geom)
    SELECT st_setsrid(replace(a::text, '2DF', '')::box2d::geometry, %s)
    FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq
    """,
    [g_srid[0], idx_oid, level],
)

conn.commit()

cur.execute("END TRANSACTION;")
cur.execute("VACUUM ANALYZE r_tree;")
cur.execute("NOTIFY qgis;")

cur.close()
conn.close()
