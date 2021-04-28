import psycopg2
import pandas as pd
import csv

# Connect to db
conn = psycopg2.connect("""
    host=192.168.1.105
    dbname=mono
    user=elfarissi
    password='%D2a3#PsT'
    """)
# Cursor
cur = conn.cursor()

# Create com_cas table
cur.execute("""
    CREATE TABLE IF NOT EXISTS cascade.com_cas (
        c_code varchar(32),
        geom geometry(MultiPolygon, 4326));
    """)

# Create index
cur.execute("""
    CREATE INDEX IF NOT EXISTS com_cas_geom_idx
	ON cascade.com_cas USING gist
	(geom);
    """)

# This creates a table where oid indices will be stored
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
oid = rows[0]

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
    [oid])
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
    [oid])
g_srid = cur.fetchone()

# cur.execute("""
#     INSERT INTO cascade.com_cas
#     SELECT c_code, geom
#     FROM communes
#     WHERE c_code = '12.066.01.03.'
#     """)

cur.execute("SELECT count(*) FROM communes;")
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
            where c_nom = 'LAGOUIRA')
        limit 1
        offset %s;
        """,
        [i])
    
    # gist_stat() comes with the gevel extension
    # gist_stat() shows some statistics about the GiST tree
    print("\nStatistics\n")
    cur.execute("SELECT gist_stat(%s);", [oid])
    stats = cur.fetchone()

    # stats var is a tuple with one element (..., )
    print(stats[0])

    # this function creates sublists
    def extractDigits(lst):
        res = []
        for el in lst:
            sub = el.split(', ')
            res.append(sub)
        
        return(res)

    # this function allows for gist_stat() and gist_tree() output to be used in other line of codes
    # mainly changing data structures for easy access 
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

    # this splits the sublists to retrieve the values afterwards
    l = [sub.split(': ') for subl in l for sub in subl]

    # this asks the user about the level of the tree to visualize
    num_level = oid

    # gist_tree() comes with the gevel extension
    # gist_tree() shows tree construction
    cur.execute("SELECT gist_tree(%s);", [oid])
    tree = cur.fetchone()

    t = expandB(tree)

    t = [sub.split(' ') for subl in t for sub in subl]

    # with open("tree.csv", "w", newline="") as f:
    #     writer = csv.writer(f)
    #     # we added a header row, standard names are meant to be droped afterwards (coli...)
    #     f.write('level,col2,blk,col4,tuple,col6,space,col8,col9\n')
    #     writer.writerows(t)

    # # we started using pandas data structures to clean data
    # df = pd.read_csv('tree.csv')
    # # the columns droped are for keys that we replaced by issuing a header row
    # df.drop('col2', inplace=True, axis=1)
    # df.drop('col4', inplace=True, axis=1)
    # df.drop('col6', inplace=True, axis=1)
    # df.drop('col8', inplace=True, axis=1)
    # df.drop('col9', inplace=True, axis=1)

    # # the following code splits columns to retrieve specific values
    # # this process was mandatory since the gist_tree() output was txt consisting of a single string
    # df[['page','level']] = df.level.str.split("(",expand=True)
    # df[['tmp','level']] = df.level.str.split(":",expand=True)
    # df[['level','tmp']] = df.level.str.split(")",expand=True)
    # df[['free(Bytes)','occupied']] = df.space.str.split("b",expand=True)
    # df[['tmp','occupied']] = df.occupied.str.split("(",expand=True)
    # df[['occupied(%)','tmp']] = df.occupied.str.split("%",expand=True)
    # df.drop('tmp', inplace=True, axis=1)
    # df.drop('space', inplace=True, axis=1)
    # df.drop('occupied', inplace=True, axis=1)

    # # this changes the order of columns in tree.csv file
    # df = df[["page", "level", "blk", "tuple", "free(Bytes)", "occupied(%)"]]

    # # renaming columns to maintain clarity
    # df.rename(columns = {'page':'node', 'level':'level', 'blk':'block', 'tuple':'num_tuples', 'free(Bytes)':'free_space(bytes)', 'occupied(%)':'occupied_space(%)'}, inplace = True)

    # # writing all changes to the original file
    # df.to_csv('tree.csv', index=False)

    # # creating a table that will hold the tree.csv content in the database
    # cur.execute("""
    # CREATE TABLE IF NOT EXISTS cascade.tree (
    #     node serial PRIMARY KEY,
    #     level integer,
    #     block integer,
    #     num_tuples integer,
    #     "free_space(bytes)" double precision,
    #     "occupied_space(%)" double precision);
    # """
    # )

    # cur.execute("TRUNCATE TABLE cascade.tree RESTART IDENTITY;")

    # # copying data from tree.csv which is on our disk to database "mono"
    # with open('tree.csv', 'r') as f:
    #     next(f) # Skip the header row.
    #     cur.copy_from(f, 'cascade.tree', sep=',')

    cur.execute("""
        CREATE TABLE IF NOT EXISTS cascade.gist_tree (
            geom geometry(%s));
        """,
        [g_type[0]])

cur.execute("TRUNCATE TABLE gist_tree RESTART IDENTITY;")

cur.execute("""
    INSERT INTO cascade.gist_tree 
    SELECT replace(a::text, '2DF', '')::box2d::geometry(POLYGON, %s)
    FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq
    """,
    [g_srid, oid, num_level])

# commiting our changes to the database
conn.commit()

# ending transaction to be able to run VACUUM ANALYZE afterwards
cur.execute("END TRANSACTION;")
# VACUUM command serves for updating statistics stored in postgres db
# that relates to nour r_tree when we rerun the python script for other relations
cur.execute("VACUUM ANALYZE cascade.gist_tree;")
# we notify qgis of the updates to display changes on the fly
cur.execute("NOTIFY qgis, 'refresh qgis';")

cur.close()
conn.close()