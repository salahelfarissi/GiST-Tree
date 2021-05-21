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
    CREATE FUNCTION public.new_table () RETURNS void AS $$ 
        plpy.execute("CREATE SCHEMA IF NOT EXISTS cascade;") 
        plpy.execute("CREATE TABLE IF NOT EXISTS cascade.communes(c_code varchar(32), geom geometry(MultiPolygon, 4326));") 
        plpy.execute("TRUNCATE TABLE cascade.communes;") 
        plpy.execute("CREATE INDEX IF NOT EXISTS new_communes_geom_idx ON cascade.communes USING gist(geom);")
    $$ LANGUAGE plpython3u; 
    """)

conn.commit()

cur.close()
conn.close()
