import psycopg2

# Connect to db from wsl2
conn = psycopg2.connect("""host=192.168.1.104
    dbname=mono
    user=elfarissi
    password='%D2a3#PsT'
    """)

# Cursor
cur = conn.cursor()

# com_cas (communes cascade) is the table where features will be inserted
cur.execute("""DROP TABLE IF EXISTS cascade.com_cas;
    """)


# r_tree will be updated as soon as new features get added
cur.execute("""DROP TABLE IF EXISTS cascade.r_tree;
    """)

# indices stores identifiers
cur.execute("DROP TABLE IF EXISTS cascade.indices;")

cur.execute("SELECT count(*) FROM maroc.communes;")
count = cur.fetchone()

for i in range (count[0]):

    table_name = 'cascade.tree_l1_'+str(i)
    cur.execute("""DROP TABLE IF EXISTS %s;
        """ % table_name)

    cur.execute("DROP TABLE IF EXISTS cascade.r_tree;")

    # commiting our changes to the database
    conn.commit()

    # ending transaction to be able to run VACUUM ANALYZE afterwards
    cur.execute("END TRANSACTION;")
    # VACUUM command serves for updating statistics stored in postgres db
    # that relates to nour r_tree when we rerun the python script for other relations
    #cur.execute("VACUUM ANALYZE %s;" % table_name)
    # cur.execute("VACUUM ANALYZE cascade.r_tree;")
    # we notify qgis of the updates to display changes on the fly
    cur.execute("NOTIFY qgis, 'refresh qgis';")

conn.commit()
cur.execute("END TRANSACTION;")
# cur.execute("VACUUM ANALYZE cascade.com_cas;")
cur.execute("NOTIFY qgis, 'refresh qgis';")
cur.close()
conn.close()