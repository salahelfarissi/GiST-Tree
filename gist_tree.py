import psycopg2

# Connect to db
conn = psycopg2.connect("""
    host=192.168.1.104
    dbname=mono
    user=elfarissi
    password='%D2a3#PsT'
    """)

# Cursor
cur = conn.cursor()

# com_cas (communes cascade) is the table where features will be inserted
cur.execute("""DROP TABLE IF EXISTS cascade.com_cas;
    """)

cur.execute("""CREATE TABLE cascade.com_cas (
        c_code varchar(32),
        geom geometry(MultiPolygon, 4326));
    """)

# Create index using GiST
cur.execute("""CREATE INDEX com_cas_geom_idx
	ON cascade.com_cas USING gist
	(geom);
    """)

# r_tree will be updated as soon as new features get added
cur.execute("""DROP TABLE IF EXISTS cascade.r_tree;
    """)

# indices stores identifiers
cur.execute("DROP TABLE IF EXISTS cascade.indices;")
cur.execute("""CREATE TABLE cascade.indices (
    idx_oid serial primary key, 
    idx_name varchar);
    """)

cur.execute("""INSERT INTO cascade.indices

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
            AND indisprimary != 't' ))
""")

# Obtain OID of com_cas index
cur.execute("SELECT * FROM cascade.indices WHERE idx_name = 'com_cas_geom_idx';")
rows = cur.fetchone()
rows = list(rows)
oid = rows[0]

# geometry type of com_cas table
cur.execute("""SELECT 
        CASE 
            WHEN type = 'MULTIPOLYGON' THEN 'POLYGON'
            ELSE type
        END AS type
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM cascade.indices
	    JOIN pg_indexes
        ON idx_name = indexname
	    WHERE idx_oid::integer = %s);
    """,
    [oid])
g_type = cur.fetchone()
print(g_type)
# srid of com_cas table
cur.execute("""SELECT 
        srid
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM cascade.indices
	    JOIN pg_indexes
        ON idx_name = indexname
	    WHERE idx_oid::integer = %s);
    """,
    [oid])
g_srid = cur.fetchone()

<<<<<<< HEAD
# cur.execute("""
#     INSERT INTO cascade.com_cas
#     SELECT c_code, geom
#     FROM communes
#     WHERE c_code = '12.066.01.03.'
#     """)

num_level = 1

cur.execute("""
    DROP TABLE IF EXISTS cascade.r_tree;
    """)

cur.execute("SELECT count(*) FROM communes;")
=======
cur.execute("SELECT count(*) FROM maroc.communes;")
>>>>>>> if_statement
count = cur.fetchone()

for i in range (count[0]):
    cur.execute("""
        INSERT INTO com_cas
        select 
            c.c_code, 
            c.geom
        from communes c
        order by c.geom <-> (
            select geom from communes
            where c_nom = 'Lagouira')
        limit 1
        offset %s;
        """,
        [i])
    
    cur.execute("SELECT gist_stat(%s);", [oid])
    stats = cur.fetchone()

    print(stats[0])

    def extractDigits(lst):
        res = []
        for el in lst:
            sub = el.split(', ')
            res.append(sub)
        
        return(res)

    def expandB(lst):
        # converting tuple to list [...]
        tmp = list(lst)
        # l is a list that contains one element
        # we splited the string on new line marks (\n)
        tmp = tmp[0].splitlines()
        # l is now a list with 9 elements (len(l) = 9)
        # this loop removes duplicate spaces in each element
        for e in range(len(tmp)):
            tmp[e] = " ".join(tmp[e].split())
        # this function puts each element in its own list
        # the result is a list of lists
        tmp = extractDigits(tmp)

        return(tmp)

    l = expandB(stats)

    l = [sub.split(': ') for subl in l for sub in subl]

    table_name = 'cascade.tree_'+str(i)
    cur.execute("""DROP TABLE IF EXISTS %s;
        """ % table_name)
    cur.execute("""CREATE TABLE %s (geom geometry(%%s, %%s))
        """ %table_name,
        [g_type[0], g_srid[0]])

    cur.execute("""INSERT INTO %s 
    SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %%s)
    FROM (SELECT * FROM gist_print(%%s) as t(level int, valid bool, a box2df) WHERE level = 1) AS subq
    """ % table_name,
    [g_srid[0], oid])

    cur.execute("DROP TABLE IF EXISTS cascade.r_tree;")
    cur.execute("""CREATE TABLE cascade.r_tree (
            geom geometry(%s, %s));
        """,
        [g_type[0], g_srid[0]])

    cur.execute("""
    INSERT INTO cascade.r_tree 
    SELECT replace(a::text, '2DF', '')::box2d::geometry(Polygon, %s)
    FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = 1) AS subq
    """,
    [g_srid[0], oid])

    # commiting our changes to the database
    conn.commit()

    # ending transaction to be able to run VACUUM ANALYZE afterwards
    cur.execute("END TRANSACTION;")
    # VACUUM command serves for updating statistics stored in postgres db
    # that relates to nour r_tree when we rerun the python script for other relations
    cur.execute("VACUUM ANALYZE %s;" % table_name)
    cur.execute("VACUUM ANALYZE cascade.r_tree;")
    # we notify qgis of the updates to display changes on the fly
    cur.execute("NOTIFY qgis, 'refresh qgis';")

conn.commit()
cur.execute("END TRANSACTION;")
cur.execute("VACUUM ANALYZE cascade.com_cas;")
cur.execute("NOTIFY qgis, 'refresh qgis';")
cur.close()
conn.close()