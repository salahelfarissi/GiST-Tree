-- List tables using INFORMATION_SCHEMA
SELECT
    table_name
FROM information_schema.tables
WHERE
    table_type = 'BASE TABLE'
    AND table_schema
    NOT IN ('pg_catalog', 'information_schema');

-- Retrieve OID of a spatial indix
SELECT CAST(c.oid AS INTEGER) FROM pg_class c, pg_index i  
WHERE c.oid = i.indexrelid and c.relname = 'c_09_geom_idx' LIMIT 1;

-- Table rtree has no srid
CREATE TABLE rtree (geom geometry);
INSERT INTO rtree
SELECT replace(a::text, '2DF', '')::box2d::geometry as geom
FROM (SELECT * FROM gist_print('29333') as t(level int, valid bool, a box2df) WHERE level =1) AS subq;

-- Polygon to linestring
select st_astext(st_exteriorring(geom)) from rtree_v2;