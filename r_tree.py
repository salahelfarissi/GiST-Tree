import psycopg2
import pandas as pd
import csv

# Connect to mono database
# I am using WSL2, so the host needs to be adjusted
conn = psycopg2.connect("""
    host=192.168.1.107
    dbname=mono
    user=elfarissi
    password='%D2a3#PsT'
    """)

# Openning a cursor to perform databse operations
cur = conn.cursor()

# Adding spatial functions to postgres
cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

# gevel_ext allows GiST index viz
cur.execute("DROP EXTENSION IF EXISTS gevel_ext;")
cur.execute("CREATE EXTENSION gevel_ext;")

# This creates a table where oid indices will be stored
cur.execute("DROP TABLE IF EXISTS indices;")
cur.execute("""CREATE TABLE r_tree.indices (
    idx_oid serial primary key, 
    idx_name varchar);
    """)

# This lists OIDs of spatial indices
# (39) WITH gt_name... this lists spatial tables
# (42) geometry_columns is a view that comes with postgis, it holds spatial relations attributes (geometry type, srid, etc)
# (45) SELECT... this returns OID (Object Identifier) of spatial indices
# (51) AND c.relname IN (... this lists all spatial indices
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
            AND indisprimary != 't' ))
""")

# Obtain data as Python objects
cur.execute("SELECT * FROM indices;")
rows = cur.fetchall()

print("\nList of GiST indices\n")

# print indices with their object identifiers
for r in rows:
    print(f"Index: {r[1]}")
    print(f"↳ OID: {r[0]}")

# allowing the user to choose which index to visualize
oid = int(input("\nWhich spatial index do you want to visualize?\nOID → "))

# retrieving the geometry type of the table that is associated with index that the user chose
# Multi to Poly is a constraint due to the geomerty type of the bounding boxes of the index
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

# retrieving the srid of the table that is associated with index that the user chose
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
print(f"Nombre de niveaux → {l[0][1]}\n")
num_level = int(input("Niveau à visualiser \n↳ "))

# gist_tree() comes with the gevel extension
# gist_tree() shows tree construction
cur.execute("SELECT gist_tree(%s, 2);", [oid])
tree = cur.fetchone()

t = expandB(tree)

t = [sub.split(' ') for subl in t for sub in subl]

with open("tree.csv", "w", newline="") as f:
    writer = csv.writer(f)
    # we added a header row, standard names are meant to be droped afterwards (coli...)
    f.write('level,col2,blk,col4,tuple,col6,space,col8,col9\n')
    writer.writerows(t)

# we started using pandas data structures to clean data
df = pd.read_csv('tree.csv')
# the columns droped are for keys that we replaced by issuing a header row
df.drop('col2', inplace=True, axis=1)
df.drop('col4', inplace=True, axis=1)
df.drop('col6', inplace=True, axis=1)
df.drop('col8', inplace=True, axis=1)
df.drop('col9', inplace=True, axis=1)

# the following code splits columns to retrieve specific values
# this process was mandatory since the gist_tree() output was txt consisting of a single string
df[['page', 'level']] = df.level.str.split("(", expand=True)
df[['tmp', 'level']] = df.level.str.split(":", expand=True)
df[['level', 'tmp']] = df.level.str.split(")", expand=True)
df[['free(Bytes)', 'occupied']] = df.space.str.split("b", expand=True)
df[['tmp', 'occupied']] = df.occupied.str.split("(", expand=True)
df[['occupied(%)', 'tmp']] = df.occupied.str.split("%", expand=True)
df.drop('tmp', inplace=True, axis=1)
df.drop('space', inplace=True, axis=1)
df.drop('occupied', inplace=True, axis=1)

# this changes the order of columns in tree.csv file
df = df[["page", "level", "blk", "tuple", "free(Bytes)", "occupied(%)"]]

# renaming columns to maintain clarity
df.rename(columns={'page': 'node', 'level': 'level', 'blk': 'block', 'tuple': 'num_tuples',
          'free(Bytes)': 'free_space(bytes)', 'occupied(%)': 'occupied_space(%)'}, inplace=True)

# writing all changes to the original file
df.to_csv('tree.csv', index=False)

# creating a table that will hold the tree.csv content in the database
cur.execute("""
CREATE TABLE IF NOT EXISTS r_tree.tree (
    node serial PRIMARY KEY,
    level integer,
    block integer,
    num_tuples integer,
    "free_space(bytes)" double precision,
    "occupied_space(%)" double precision);
"""
            )

cur.execute("TRUNCATE TABLE tree RESTART IDENTITY;")

# copying data from tree.csv which is on our disk to database "mono"
with open('tree.csv', 'r') as f:
    next(f)  # Skip the header row.
    cur.copy_from(f, 'tree', sep=',')

# creating the table that will hold the bounding boxes of the GiST tree
cur.execute("""
    CREATE TABLE IF NOT EXISTS r_tree.r_tree (
        geom geometry(%s));
    """,
            [g_type[0]])
cur.execute("TRUNCATE TABLE r_tree RESTART IDENTITY;")

cur.execute("""
    INSERT INTO r_tree 
    SELECT replace(a::text, '2DF', '')::box2d::geometry(POLYGON, %s)
    FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq
    """,
            [g_srid[0], oid, num_level])

# commiting our changes to the database
conn.commit()

# ending transaction to be able to run VACUUM ANALYZE afterwards
cur.execute("END TRANSACTION;")
# VACUUM command serves for updating statistics stored in postgres db
# that relates to nour r_tree when we rerun the python script for other relations
cur.execute("VACUUM ANALYZE r_tree;")
# we notify qgis of the updates to display changes on the fly
cur.execute("NOTIFY qgis, 'refresh qgis';")

cur.close()
conn.close()
