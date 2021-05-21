from psycopg2 import sql, connect
from functions import *

# * Define variables
psql = {
    'host': '192.168.1.105',
    'dbname': 'mono',
    'password': '%D2a3#PsT'
}

# * Connect to PostgeSQL
conn = connect(f"""
    host={psql['host']}
    dbname={psql['dbname']}
    password={psql['password']}
    """)

cur = conn.cursor()


cur.execute("""
    CREATE OR REPLACE FUNCTION new_table () RETURNS void AS $$ 
        plpy.execute("CREATE SCHEMA IF NOT EXISTS gist_viz;") 
        plpy.execute("CREATE TABLE IF NOT EXISTS gist_viz.communes(c_code varchar(32), geom geometry(MultiPolygon, 4326));") 
        plpy.execute("TRUNCATE TABLE gist_viz.communes;") 
        plpy.execute("CREATE INDEX IF NOT EXISTS new_communes_geom_idx ON gist_viz.communes USING gist(geom);")
    $$ LANGUAGE plpython3u; 
    """)

cur.execute("""
    CREATE OR REPLACE FUNCTION gist_index () RETURNS void AS $$ 
        plpy.execute("CREATE TABLE IF NOT EXISTS gist_viz.indices (idx_oid serial primary key, idx_name varchar);") 
        plpy.execute("TRUNCATE TABLE gist_viz.indices;") 
        plpy.execute("INSERT INTO gist_viz.indices \
            WITH gt_name AS ( \
                SELECT \
                    f_table_name AS t_name \
                FROM geometry_columns \
            ) \
            SELECT \
                CAST(c.oid AS INTEGER), \
                c.relname \
            FROM pg_class c, pg_index i \
            WHERE c.oid = i.indexrelid \
            AND c.relname IN ( \
                SELECT \
                    relname \
                FROM pg_class, pg_index \
                WHERE pg_class.oid = pg_index.indexrelid \
                AND pg_class.oid IN ( \
                    SELECT \
                        indexrelid \
                    FROM pg_index, pg_class \
                    WHERE pg_class.relname IN ( \
                        SELECT t_name \
                        FROM gt_name) \
                    AND pg_class.oid = pg_index.indrelid \
                    AND indisunique != 't' \
                    AND indisprimary != 't' ));")
    $$ LANGUAGE plpython3u;""")

conn.commit()

cur.close()
conn.close()
