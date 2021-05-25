import psycopg2

conn = psycopg2.connect("""
    host=192.168.1.104
    dbname=mono
    password='%D2a3#PsT'
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

print('\nList of GiST indices')

for i in indices:
    print(f'Index: {i[1]}', f'OID: {i[0]}', sep=' ▮ ')

idx_oid = int(input("""
    \nWhich GiST index do you want to visualize?\nOID → """))

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


def tuple_to_dict(st=()):
    lst = list(st)[0].splitlines()
    lst = [" ".join(lst[e].split()) for e in range(len(lst))]
    lst = [[el] for el in lst]
    lst = [sub.split(': ') for subl in lst for sub in subl]

    key = [i[0] for i in lst]
    value = [i[1] for i in lst]

    for i in range(len(value)):
        value[i] = value[i].replace(' bytes', '')
        value[i] = int(value[i])

    dct = {key[i]: value[i] for i in range(len(key))}

    return(dct)


print("\nStatistics")

cur.execute(f"SELECT gist_stat({idx_oid});")
stat = cur.fetchone()

print(stat[0])

stat = tuple_to_dict(stat)

print(f"Number of levels → {stat['Number of levels']}\n")
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


conn.commit()

cur.execute("END TRANSACTION;")
cur.execute("VACUUM ANALYZE gist.r_tree;")
cur.execute("NOTIFY qgis, 'refresh qgis';")

cur.close()
conn.close()
