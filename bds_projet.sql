-- postgis DATABASE template creation 
CREATE DATABAS postgis;
CREATE SCHEMA postgis;
CREATE EXTENSION postgis SCHEMA postgis;
UPDATE pg_DATABASE SET datistemplate = TRUE WHERE datname = 'postgis';

-- bds project DATABASE
CREATE DATABAS mono template postgis;

-- in order to use postgis extensions, we added the schema postgis to search path
-- as defined in the template creation
ALTER DATABAS mono SET search_path='$user', public, postgis;

-- this query helped me in identifying the srid of datum WGS84
SELECT srid, proj4text FROM spatial_ref_sys WHERE srtext like '%WGS_1984%';

-- these commands are executed outside postgres shell
shp2pgsql -s 4326 -g geom -I .\regions.shp regions | psql -U elfarissi -d mono
shp2pgsql -s 4326 -g geom -I .\provinces.shp provinces | psql -U elfarissi -d mono
shp2pgsql -s 4326 -g geom -I .\communes.shp communes | psql -U elfarissi -d mono

-- update statistics
vacuum analyze regions, provinces, communes;

-- some data cleaning
delete FROM regions WHERE code_regio is null;

ALTER TABLE regions RENAME COLUMN "code_regio" TO r_code;
ALTER TABLE regions RENAME COLUMN "nom_region" TO r_nom;

ALTER TABLE regions
DROP COLUMN gid,
DROP COLUMN objectid,
DROP COLUMN population,
DROP COLUMN menages,
DROP COLUMN etrangers,
DROP COLUMN marocains,
DROP COLUMN ruleid,
DROP COLUMN shape__are,
DROP COLUMN shape__len;

-- reducing number of bits that is used by some datatypes
ALTER table regions ALTER column "r_code" type varchar(3);
--
update communes set "nom_commun" = upper(nom_commun);

-- identifying region of interest
-- first region, then provinces, then communes
-- basing our queries on a normalized identifier the begins with the id of the region
SELECT * INTO c_09
FROM communes WHERE c_code like '09.%';
SELECT * INTO p_09
FROM provinces WHERE p_code like '09.%';
SELECT * INTO r_09
FROM regions WHERE r_code = '09.';

-- Spatial index creation
-- 09. is the identifier of the region souss-massa
CREATE INDEX r_09_geom_idx ON r_09 USING gist(geom);
CREATE INDEX p_09_geom_idx ON p_09 USING gist(geom);
CREATE INDEX c_09_geom_idx ON c_09 USING gist(geom);
--
update p_09 set menages_18 = case
when p_nom = 'AGADIR IDA OU TANAN' then 163283
when p_nom = 'CHTOUKA AIT BAHA' then 99852
when p_nom = 'INEZGANE AIT MELLOUL' then 142549
when p_nom = 'TAROUDANNT' then 180895
when p_nom = 'TATA' then 22675
when p_nom = 'TIZNIT' then 52671
end;

-- PK creation
ALTER TABLE r_09 ADD COLUMN id serial primary key;
ALTER TABLE p_09 ADD COLUMN id serial primary key;
ALTER TABLE c_09 ADD COLUMN id serial primary key;

-- Q1
SELECT p_nom, menages_18 FROM p_09
order by menages_18 desc
limit 1;

   p_nom    | menages_18
------------+------------
 TAROUDANNT |     180895
--
ALTER TABLE p_09
ALTER COLUMN geom TYPE geometry(MULTIPOLYGON, 26192) USING ST_Transform(ST_SetSRID(geom,4326),26192);
--
ALTER TABLE p_09
ADD COLUMN menages_04 integer;
--
update p_09 set menages_04 = case
when p_nom = 'AGADIR IDA OU TANAN' then 103395
when p_nom = 'CHTOUKA AIT BAHA' then 61419
when p_nom = 'INEZGANE AIT MELLOUL' then 87786
when p_nom = 'TAROUDANNT' then 138054
when p_nom = 'TATA' then 20349
when p_nom = 'TIZNIT' then 45188
end;
-- Q2
SELECT sum(menages_04) menages_04, sum(menages_14) menages_14 FROM p_09;

 menages_04 | menages_14
------------+------------
     456191 |     601511

-- Q3
SELECT
	SubStr(c.c_code,1,7) AS p_id,
	p.p_nom,
 	count(*) AS cr_nbre,
	ST_Union(c.geom) AS geom
FROM c_09 c
JOIN p_09 p
ON SubStr(c_code,1,7) = p_code
WHERE c.c_type = 'R'
GROUP BY p_id, p.p_nom
order by cr_nbre desc
limit 1;

  p_id   |   p_nom    | cr_nbre
---------+------------+---------
 09.541. | TAROUDANNT |      81  

 -- Q4
 SELECT
	SubStr(p.p_code,1,3) AS r_id,
	r.r_nom,
 	count(*) AS p_nbre,
	ST_Equals(ST_Union(p.geom), r.geom),
	ST_Union(p.geom) AS geom
FROM p_09 p
JOIN r_09 r
ON SubStr(p_code,1,3) = r_code
GROUP BY r_id, r.r_nom, r.geom
order by p_nbre desc;

r_id |    r_nom    | p_nbre | st_equals
------+-------------+--------+-----------
 09.  | SOUSS-MASSA |      6 | t

 -- Other method

 -- Make the region table
CREATE TABLE r_geoms AS
SELECT
  ST_Union(geom) AS geom,
  SubStr(p_code,1,3) AS r_id
FROM p_09
GROUP BY r_id;

-- Index the r_id
CREATE INDEX r_geoms_r_id_idx
  ON r_geoms (r_id);

-- Test equality of geoms
SELECT st_equals(r.geom, g.geom)
FROM r_09 r, r_geoms g
WHERE r.r_code = g.r_id;

 st_equals
-----------
 t

 -- Q5
 SELECT * FROM (
	SELECT
	SubStr(c.c_code,1,7) AS p_id,
	p.p_nom,
 	count(*) AS c_nbre,
	round((st_area(p.geom)/1000000)::numeric, 2) AS area_km2,
	ST_Union(c.geom) AS geom
FROM c_09 c
JOIN p_09 p
ON SubStr(c_code,1,7) = p_code
GROUP BY p_id, p.p_nom, area_km2
ORDER BY c_nbre DESC
	) AS subq
	ORDER BY area_km2 DESC
	LIMIT 1;

  p_id   | p_nom | c_nbre | area_km2
---------+-------+--------+----------
 09.551. | TATA  |     20 | 26481.79