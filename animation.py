from psycopg2 import connect
from func import unpack
import sys

from matplotlib import animation
import matplotlib.pyplot as plt
import random as sns

conn = connect("""
    host='192.168.1.100'
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
num_geometries = cur.fetchone()[0]

number_of_frames = int(sys.argv[1])
injections_per_frame = int(sys.argv[2])

if injections_per_frame > num_geometries:
    num_iterations = num_geometries + 1
else:
    num_iterations = injections_per_frame + 1


def update(frame_number, num_iterations, faces, frequencies):

    for i in range(1, num_iterations):

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

        stat = unpack(stat)

        for key, value in stat.items():
            print(f'{key:<16}: {value:,}')
        print()

    plt.cla()
    axes = sns.barplot(faces, frequencies, palette='bright')
    axes.set_title(f'Memory Size for {sum(frequencies)} Tuples')
    axes.set(xlabel='Value', ylabel='Frequency')
    axes.set_ylim(top=max(frequencies) * 1.10)

    for bar, frequency in zip(axes.patches, frequencies):
        text_x = bar.get_x() + bar.get_width() / 2.0
        text_y = bar.get_height()
        text = f'{frequency:,}\n{frequency / sum(frequencies):.3%}'
        axes.text(text_x, text_y, text, ha='center', va='bottom')


sns.set_style('whitegrid')
figure = plt.figure('R-Tree')
values = list(range(1, 4))
frequencies = [0] * 3

bbox_animation = animation.FuncAnimation(
    figure, update, repeat=False, frames=number_of_frames, interval=33,
    fargs=(injections_per_frame, values, frequencies))

cur.close()
conn.close()
