SELECT CAST(c.oid AS INTEGER) FROM pg_class c, pg_index i  
WHERE c.oid = i.indexrelid and c.relname = 'c_09_geom_idx' LIMIT 1;

CREATE TABLE rtree (geom geometry);
INSERT INTO rtree
SELECT replace(a::text, '2DF', '')::box2d::geometry as geom
FROM (SELECT * FROM gist_print('29333') as t(level int, valid bool, a box2df) WHERE level =1) AS subq;