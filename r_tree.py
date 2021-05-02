import psycopg2

# * Connect to an existing database
# ! Host ip changes for virtual machines
conn = psycopg2.connect("""
    host=192.168.1.107
    dbname=mono
    password='%D2a3#PsT'
    """)

# * Open a cursor to perform database operations
cur = conn.cursor()

# ? Output postgis functions in a seperate schema called postgis
cur.execute("CREATE SCHEMA IF NOT EXISTS postgis;")

cur.execute("""
    CREATE EXTENSION IF NOT EXISTS postgis
    SCHEMA postgis;""")

# ? Output gevel_ext functions in a seperate schema called gevel
# * gist_stat(INDEXNAME) - show some statistics about GiST tree
# * gist_tree(INDEXNAME,MAXLEVEL) - show GiST tree up to MAXLEVEL
# * gist_tree(INDEXNAME) - show full GiST tree
# * gist_print(INDEXNAME) - prints objects stored in GiST tree
cur.execute("CREATE SCHEMA IF NOT EXISTS gevel;")

cur.execute("""
    CREATE EXTENSION IF NOT EXISTS gevel_ext
    SCHEMA gevel;""")

# ? Output stored GiST indices
cur.execute("TRUNCATE TABLE r_tree.indices;")
cur.execute("""
    CREATE TABLE IF NOT EXISTS r_tree.indices (
        idx_oid serial primary key, 
        idx_name varchar);""")
cur.execute("""
    INSERT INTO r_tree.indices
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
            AND indisprimary != 't' ))""")
cur.execute("SELECT * FROM indices;")

indices = cur.fetchall()

print("\nList of GiST indices\n")

for i in indices:
    print(f"Index: {i[1]}")
    print(f"↳ OID: {i[0]}")

index = int(input("\nWhich GiST index do you want to visualize?\nOID → "))

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
            [index])
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
            [index])
g_srid = cur.fetchone()

print("\nStatistics\n")
cur.execute("SELECT gist_stat(%s);", [index])
stats = cur.fetchone()

print(stats[0])


def extractDigits(lst):
    res = []
    for el in lst:
        sub = el.split(', ')
        res.append(sub)

    return(res)


def expandB(lst):
    tmp = list(lst)
    tmp = tmp[0].splitlines()
    for e in range(len(tmp)):
        tmp[e] = " ".join(tmp[e].split())
    tmp = extractDigits(tmp)

    return(tmp)


stats = expandB(stats)
l = [sub.split(': ') for subl in stats for sub in subl]
print(stats)
print(f"Number of levels → {stats[0][1]}\n")
level = int(input("Level to visualize \n↳ "))

cur.execute("SELECT gist_tree(%s, 2);", [index])
tree = cur.fetchone()

t = expandB(tree)
t = [sub.split(' ') for subl in t for sub in subl]

with open("tree.csv", "w", newline="") as f:
    writer = csv.writer(f)
    f.write('level,col2,blk,col4,tuple,col6,space,col8,col9\n')
    writer.writerows(t)

df = pd.read_csv('tree.csv')

df.drop('col2', inplace=True, axis=1)
df.drop('col4', inplace=True, axis=1)
df.drop('col6', inplace=True, axis=1)
df.drop('col8', inplace=True, axis=1)
df.drop('col9', inplace=True, axis=1)

df[['page', 'level']] = df.level.str.split("(", expand=True)
df[['tmp', 'level']] = df.level.str.split(":", expand=True)
df[['level', 'tmp']] = df.level.str.split(")", expand=True)
df[['free(Bytes)', 'occupied']] = df.space.str.split("b", expand=True)
df[['tmp', 'occupied']] = df.occupied.str.split("(", expand=True)
df[['occupied(%)', 'tmp']] = df.occupied.str.split("%", expand=True)
df.drop('tmp', inplace=True, axis=1)
df.drop('space', inplace=True, axis=1)
df.drop('occupied', inplace=True, axis=1)

df = df[["page", "level", "blk", "tuple", "free(Bytes)", "occupied(%)"]]

df.rename(columns={'page': 'node', 'level': 'level', 'blk': 'block', 'tuple': 'num_tuples',
          'free(Bytes)': 'free_space(bytes)', 'occupied(%)': 'occupied_space(%)'}, inplace=True)

df.to_csv('tree.csv', index=False)

cur.execute("""
    CREATE TABLE IF NOT EXISTS r_tree.tree (
        node serial PRIMARY KEY,
        level integer,
        block integer,
        num_tuples integer,
        "free_space(bytes)" double precision,
        "occupied_space(%)" double precision);""")

cur.execute("TRUNCATE TABLE r_tree.tree RESTART IDENTITY;")

with open('tree.csv', 'r') as f:
    next(f)
    cur.copy_from(f, 'tree', sep=',')

cur.execute("""
    CREATE TABLE IF NOT EXISTS r_tree.r_tree (
        geom geometry(%s));
    """,
            [g_type[0]])
cur.execute("TRUNCATE TABLE r_tree.r_tree RESTART IDENTITY;")

cur.execute("""
    INSERT INTO r_tree.r_tree 
    SELECT replace(a::text, '2DF', '')::box2d::geometry(POLYGON, %s)
    FROM (SELECT * FROM gist_print(%s) as t(level int, valid bool, a box2df) WHERE level = %s) AS subq
    """,
            [g_srid[0], oid, num_level])

conn.commit()

cur.execute("END TRANSACTION;")
cur.execute("VACUUM ANALYZE r_tree.r_tree;")
cur.execute("NOTIFY qgis, 'refresh qgis';")

cur.close()
conn.close()
