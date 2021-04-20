-- List tables using INFORMATION_SCHEMA
SELECT f_table_name AS nom_table FROM geometry_columns;

-- List spatial indices
WITH nom_table AS (
        SELECT f_table_name AS nom_table
        FROM geometry_columns
    )
SELECT
    relname
FROM pg_class, pg_index
WHERE pg_class.oid = pg_index.indexrelid
AND pg_class.oid IN (
    SELECT indexrelid FROM pg_index, pg_class
    WHERE pg_class.relname IN (SELECT nom_table FROM nom_table)
    AND pg_class.oid=pg_index.indrelid
    AND indisunique != 't'
    AND indisprimary != 't' );

-- Retrieve OIDs of a spatial indices
WITH nom_table AS (
        SELECT f_table_name AS nom_table
        FROM geometry_columns
    )
SELECT
    relname,
    CAST(c.oid AS INTEGER) FROM pg_class c, pg_index i  
WHERE c.oid = i.indexrelid and c.relname IN (
    WITH nom_table AS (
        SELECT f_table_name AS nom_table
        FROM geometry_columns
        )
    SELECT
        relname
    FROM pg_class, pg_index
    WHERE pg_class.oid = pg_index.indexrelid
    AND pg_class.oid IN (
        SELECT indexrelid FROM pg_index, pg_class
        WHERE pg_class.relname IN (SELECT nom_table FROM nom_table)
        AND pg_class.oid=pg_index.indrelid
        AND indisunique != 't'
        AND indisprimary != 't' )
);

-- Viz
SELECT replace(a::text, '2DF', '')::box2d::geometry as geom
FROM (SELECT * FROM gist_print('29415') as t(level int, valid bool, a box2df) WHERE level =1) AS subq;