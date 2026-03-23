"""Display r-tree bboxes"""

import os

import pandas as pd
from psycopg import connect

from gist_tree.func import field_width, unpack


def main():
    # Use connect class to establish connection to Postgres
    conn = connect(
        host=os.environ.get("PGHOST", "localhost"),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
        port=os.environ.get("PGPORT", "5432"),
    )
    cur = conn.cursor()

    # You find a sql function to execute beforehand in queries folder
    cur.execute(
        """
        SELECT * FROM public.indices(); """
    )

    indices = cur.fetchall()

    # Display a two column table with index and oid
    w1, w2 = field_width(indices)

    print(f"\n{'Index':>{w1}}{'OID':>{w2}}", "-" * (w1 + w2), sep="\n")

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

    # Look up index name — pramsey/gevel takes index name (text), not OID
    idx_name = next(name for oid, name in indices if oid == idx_oid)

    # g_srid() function must be created beforehand (queries folder)
    cur.execute("SELECT g_srid(%s);", [idx_oid])
    g_srid = cur.fetchone()

    cur.execute("SELECT gist_stat(%s);", [idx_name])
    stat = pd.Series(unpack(cur.fetchone()))

    print(f"\nTree has a depth of {stat.Levels}.\n")
    level = int(input("Which level do you want to visualize?\nLevel → "))

    print("\n¯\\_(ツ)_/¯\n")

    cur.execute(
        """
        DROP TABLE IF EXISTS r_tree;
        """
    )

    cur.execute(
        f"""
        CREATE TABLE r_tree (
            id serial primary key,
            geom geometry(POLYGON, {g_srid[0]}));
        """
    )

    cur.execute(
        """
        INSERT INTO r_tree (geom)
        SELECT st_setsrid(replace(a::text, '2DF', '')::box2d::geometry, %s)
        FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq
        """,
        [g_srid[0], idx_name, level],
    )

    conn.commit()

    conn.autocommit = True
    cur.execute("VACUUM ANALYZE r_tree;")
    cur.execute("NOTIFY qgis;")

    cur.close()
    conn.close()
