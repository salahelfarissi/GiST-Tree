-- FUNCTION: public.index_table()

-- DROP FUNCTION public.index_table();

CREATE OR REPLACE FUNCTION public.index_table(
	)
    RETURNS integer
    LANGUAGE 'plpython3u'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
plpy.execute("""
        CREATE TABLE IF NOT EXISTS indices (
            idx_oid serial primary key,
            idx_name varchar);
            """)

    plpy.execute("""
        TRUNCATE TABLE indices;
        """)
    
    plpy.execute("""
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
                AND indisprimary != 't' ))""")

    rv = plpy.execute("""
        SELECT idx_oid FROM indices
        WHERE idx_name = 'communes_geom_idx';""")

    foo = rv[0]["idx_oid"]

    return foo
$BODY$;

ALTER FUNCTION public.index_table()
    OWNER TO elfarissi;