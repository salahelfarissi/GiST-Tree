# gist_viz.py
"""Display r-tree bboxes"""
from psycopg2 import connect, sql
from func import *

CHECKPOINT = 500  # ask to continue every N inserts

conn = connect(
    """
    host=localhost
    port=5433
    dbname=nyc
    user=postgres
    password=postgres
    """
)

cur = conn.cursor()

cur.execute(
    """
    CREATE TABLE IF NOT EXISTS streets_knn (
        geom geometry(MultiLineString, 3857));
        """
)

cur.execute(
    """
    TRUNCATE TABLE streets_knn;
    """
)

cur.execute(
    """
    CREATE INDEX IF NOT EXISTS streets_knn_geom_idx
        ON streets_knn USING gist (geom);
        """
)

# indices() function must be created beforehand
cur.execute(
    """
    SELECT oid, index FROM indices() WHERE index = 'streets_knn_geom_idx';
    """
)

row = cur.fetchone()
knn_idx_oid = row[0]
knn_idx_name = row[1]

cur.execute(
    """
    SELECT COUNT(*) FROM nyc_streets;
    """
)
num_geometries = cur.fetchone()[0]


def bbox(table, scope):

    cur.execute(
        sql.SQL(
            """
        CREATE TABLE IF NOT EXISTS {} (
            geom geometry);
        """
        ).format(sql.Identifier(table))
    )

    cur.execute(
        sql.SQL(
            """
        TRUNCATE TABLE {}"""
        ).format(sql.Identifier(table))
    )

    cur.execute(
        sql.SQL(
            """
        INSERT INTO {}
        SELECT st_setsrid(replace(a::text, '2DF', '')::box2d::geometry, 3857)
        FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq
        """
        ).format(sql.Identifier(table)),
        [knn_idx_name, scope],
    )

    cur.execute("NOTIFY qgis;")


stat = dict()

for i in range(1, num_geometries + 1):

    cur.execute(
        """
        INSERT INTO streets_knn
        SELECT
            n.geom
        FROM nyc_streets n
        ORDER by n.geom <-> (
            SELECT geom FROM nyc_streets
            WHERE id = 12623)
        LIMIT 1
        OFFSET %s;
        """,
        [i - 1],
    )

    cur.execute(
        """
        END;
        """
    )

    cur.execute(
        """
        VACUUM ANALYZE streets_knn;
        """
    )

    cur.execute("SELECT gist_stat(%s);", [knn_idx_name])

    for key, value in unpack(cur.fetchone()).items():
        if key in stat:
            stat[key].append(value)
        else:
            stat[key] = [value]

    for key, value in stat.items():
        print(f"{key:<16} : {value[i - 1]:,}")
    print()

    level = stat["Levels"][i - 1]

    level = [value for value in range(1, level + 1)]

    if len(level) == 1:
        bbox("r_tree_l2", 1)

    else:
        for j in level:
            if j == 1:
                bbox("r_tree_l1", 1)
            else:
                bbox("r_tree_l2", 2)

    if i % CHECKPOINT == 0:
        answer = input(f"\n[{i}/{num_geometries}] Continue? (y/n) → ").strip().lower()
        if answer != "y":
            print("Stopping.")
            break

conn.commit()

cur.close()
conn.close()
