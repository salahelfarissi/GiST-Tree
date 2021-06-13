CREATE OR REPLACE FUNCTION indices() RETURNS TABLE(oid integer, index varchar)
    AS $$ WITH
	gt_name AS (
        SELECT
            f_table_name AS t_name
        FROM geometry_columns
    )
    SELECT
        CAST(c.oid AS INTEGER) as "oid",
        c.relname as "index"
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
	$$
    LANGUAGE SQL;

CREATE OR REPLACE FUNCTION g_srid(integer) RETURNS integer
    AS $$ SELECT 
        srid
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM indices()
	    JOIN pg_indexes
        ON index = indexname
	    WHERE oid::integer = $1);
    $$
    LANGUAGE SQL;

CREATE OR REPLACE FUNCTION g_type(integer) RETURNS varchar
    AS $$ SELECT 
        type
    FROM geometry_columns
    WHERE f_table_name IN (
	    SELECT tablename FROM indices()
	    JOIN pg_indexes
        ON index = indexname
	    WHERE oid::integer = $1);
    $$
    LANGUAGE SQL;