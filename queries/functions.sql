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