import psycopg2

# * Connect to an existing database
login = ['192.168.1.101', 'mono', '%D2a3#PsT']

conn = psycopg2.connect(f"""
    host={login[0]}
    dbname={login[1]}
    password={login[2]}
    """)

# * Open a cursor to perform database operations
cur = conn.cursor()

# * Cascade schema will hold R-Tree bbox generated through iteration
schema = 'regions'
cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

for i in range(1, 13):
    if i < 10:
        region_view = 'c_0'+str(i)
        region_code = '0'+str(i)+'.%'
        cur.execute(f"""
            CREATE OR REPLACE VIEW {schema}.{region_view} AS
            SELECT c_code, c_type, c_nom, st_transform(geom, 26192) as geom FROM maroc.communes WHERE c_code like '{region_code}';
            """)
    else:
        region_view = 'c_'+str(i)
        region_code = str(i)+'.%'
        cur.execute(f"""
            CREATE OR REPLACE VIEW {schema}.{region_view} AS
            SELECT c_code, c_type, c_nom, st_transform(geom, 26192) as geom FROM maroc.communes WHERE c_code like '{region_code}';
            """)

conn.commit()
cur.close()
conn.close()
