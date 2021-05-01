pg_dump -h 192.168.1.105 -n souss -n r_tree -f mono.backup -F d mono

-- I created a postgis database to be use as a template

CREATE DATABASE postgis;
CREATE SCHEMA postgis;
CREATE EXTENSION postgis SCHEMA postgis;
UPDATE pg_DATABASE SET datistemplate = TRUE WHERE datname = 'postgis';

CREATE SCHEMA gevel;
CREATE EXTENSION gevel_ext SCHEMA gevel;
-- in order to use postgis & gevel_ext, we added the schema postgis to search path
-- as defined in the template creation
alter database mono set search_path = "$user", public, postgis, souss, r_tree, maroc, cascade, gevel;

-- I named the database 'mono' as an alias to monographie
CREATE DATABASE mono template postgis;

GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]
communes.prj

-- this query helped me in identifying the srid of datum WGS84
SELECT srid, proj4text FROM spatial_ref_sys WHERE srtext like '%WGS_1984%';

  srid  |  proj4text
--------+-------------------------------------
   4326 | +proj=longlat +datum=WGS84 +no_defs

-- these commands are executed outside postgres shell
shp2pgsql -s 4326 -g geom -I .\regions.shp regions | psql -U elfarissi -d mono
shp2pgsql -s 4326 -g geom -I .\provinces.shp provinces | psql -U elfarissi -d mono
shp2pgsql -s 4326 -g geom -I .\communes.shp communes | psql -U elfarissi -d mono

-- UPDATE statistics
vacuum analyze regions, provinces, communes;

-- some data cleaning
DELETE FROM regions WHERE code_regio IS NULL;

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
SELECT MAX(LENGTH(r_code)) FROM regions;
ALTER table regions ALTER column "r_code" type varchar(3);
--
UPDATE communes SET "nom_commun" = upper(nom_commun);

-- identifying region of interest
-- first region, then provinces, then communes
-- basing our queries on a normalized identifier the begins with the id of the region
SELECT * INTO c_09
FROM communes WHERE c_code like '09.%';
SELECT * INTO p_09
FROM provinces WHERE p_code like '09.%';
SELECT * INTO r_09
FROM regions WHERE r_code = '09.';

09.541.01.01. = 09. 541. 01.01.

09.     = Région "Souss-Massa"
541.    = Province "TAROUDANNT"
01.01.  = Commune "AIT IAAZA"


-- Spatial index creation
-- 09. is the identifier of the region souss-massa
CREATE INDEX r_09_geom_idx ON r_09 USING gist(geom);
CREATE INDEX p_09_geom_idx ON p_09 USING gist(geom);
CREATE INDEX c_09_geom_idx ON c_09 USING gist(geom);
--
ALTER TABLE p_09 ADD COLUMN menages_04 integer;
ALTER TABLE p_09 ADD COLUMN menages_14 integer;
ALTER TABLE p_09 ADD COLUMN menages_18 integer;
--
UPDATE p_09 SET menages_04 = case
WHEN p_nom = 'AGADIR IDA OU TANAN' THEN 103395
WHEN p_nom = 'CHTOUKA AIT BAHA' THEN 61419
WHEN p_nom = 'INEZGANE AIT MELLOUL' THEN 87786
WHEN p_nom = 'TAROUDANNT' THEN 138054
WHEN p_nom = 'TATA' THEN 20349
WHEN p_nom = 'TIZNIT' THEN 45188
END;
--
UPDATE p_09 SET menages_14 = case
WHEN p_nom = 'AGADIR IDA OU TANAN' THEN 143752
WHEN p_nom = 'CHTOUKA AIT BAHA' THEN 88732
WHEN p_nom = 'INEZGANE AIT MELLOUL' THEN 124340
WHEN p_nom = 'TAROUDANNT' THEN 171186
WHEN p_nom = 'TATA' THEN 22359
WHEN p_nom = 'TIZNIT' THEN 51142
END;
--
UPDATE p_09 SET menages_18 = case
WHEN p_nom = 'AGADIR IDA OU TANAN' THEN 163283
WHEN p_nom = 'CHTOUKA AIT BAHA' THEN 99852
WHEN p_nom = 'INEZGANE AIT MELLOUL' THEN 142549
WHEN p_nom = 'TAROUDANNT' THEN 180895
WHEN p_nom = 'TATA' THEN 22675
WHEN p_nom = 'TIZNIT' THEN 52671
END;
--
ALTER TABLE p_09
ALTER COLUMN geom TYPE geometry(MULTIPOLYGON, 26192) USING ST_Transform(ST_SetSRID(geom,4326),26192);
--
\copy (select id, c_code, c_type, c_nom from c_09 LIMIT 5) to c_09.csv csv header;
--
id,c_code,c_type,c_nom
1,09.541.01.01.,M,AIT IAAZA
2,09.581.01.07.,M,TIZNIT
3,09.541.07.03.,R,AHL TIFNOUTE
4,09.541.04.59.,R,TIGOUGA
5,09.541.03.15.,R,OUALQADI
--
\copy (select id, p_code, p_nom, menages_04, menages_14, menages_18 from p_09 LIMIT 5) to p_09.csv csv header
--
id,p_code,p_nom,menages_04,menages_14,menages_18
1,09.001.,AGADIR IDA OU TANAN,103395,143752,163283
2,09.163.,CHTOUKA AIT BAHA,61419,88732,99852
4,09.551.,TATA,20349,22359,22675
5,09.581.,TIZNIT,45188,51142,52671
3,09.273.,INEZGANE AIT MELLOUL,87786,124340,142549
--
\copy (select id, r_code, r_nom, pop_m_u, pop_m_r, pop_f_u, pop_f_r from r_09 LIMIT 5) to r_09.csv csv header; 
--
id,r_code,r_nom,pop_m_u,pop_m_r,pop_f_u,pop_f_r
1,09.,SOUSS-MASSA,0.5,0.47,0.5,0.53
--
ALTER TABLE c_09 ADD PRIMARY KEY (c_code);
ALTER TABLE c_09 ADD COLUMN p_code varchar(7);
UPDATE c_09 set p_code = substr(c_code, 1, 7); 

ALTER TABLE c_09 ADD CONSTRAINT p_fk FOREIGN KEY (p_code)
REFERENCES p_09 (p_code) ON DELETE CASCADE;

ALTER TABLE p_09 ADD PRIMARY KEY (p_code);
ALTER TABLE p_09 ADD COLUMN r_code varchar(3);
UPDATE p_09 set r_code = substr(p_code, 1, 3);

ALTER TABLE r_09 ADD PRIMARY KEY (r_code);

ALTER TABLE p_09 ADD CONSTRAINT r_fk FOREIGN KEY (r_code)
REFERENCES r_09 (r_code) ON DELETE CASCADE;

-- the query returns the geometries that have a hole in them
/* In order to run ST_NumInteriorRings() we need to convert the MultiPolygon geometries of c_09
into simple polygons, so we extract the first polygon from each collection using ST_GeometryN().*/
select c_code, st_numgeometries(geom), geom
from c_09 
where st_numinteriorrings(st_geometryn(geom, 1)) > 0;

-- Data quality
SELECT a.c_code, b.c_code, 
st_geomfromtext(st_astext(st_intersection(a.geom, b.geom)), 26192) AS geom 
FROM c_09 a, c_09 b  
WHERE ST_Intersects(a.geom, b.geom)  
AND ST_Relate(a.geom, b.geom, '2********')  
AND a.c_code != b.c_code;

-- Q1 : Dans votre région, quelle province contient le nombre le plus important de ménages ?
SELECT p_nom, menages_18 FROM p_09
ORDER BY menages_18 DESC
LIMIT 1;

   p_nom    | menages_18
------------+------------
 TAROUDANNT |     180895

-- Q2 : Dans votre région, quelle est l’évolution de l’effectif des ménages entre 2004 et 2014 ?
SELECT sum(menages_04) menages_04, sum(menages_14) menages_14 FROM p_09;

 menages_04 | menages_14
------------+------------
     456191 |     601511

-- Q3 : Dans votre région, quelle est la province qui contient le plus grand nombre de communes rurales ?
SELECT
	p.p_code,
	p.p_nom,
 	count(*) AS cr_nbre
FROM c_09 c
JOIN p_09 p
ON c.p_code = p.p_code
WHERE c.c_type = 'R'
GROUP BY 1, 2
ORDER BY cr_nbre DESC
LIMIT 1;

  p_id   |   p_nom    | cr_nbre
---------+------------+---------
 09.541. | TAROUDANNT |      81  

 /* Q4 : Dans votre région, Calculer l’agrégation des géométries des provinces appartenant à votre région
 et comparez la avec la géométrie de la région issues de fichier des régions. Que remarquez-vous ? */
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
ORDER BY p_nbre DESC;

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

 -- Q5 : Dans votre région, Quelle est la province qui a à la fois la plus grande géométrie et le plus grand nombre de communes ?
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

 -- Q6 : Dans votre région, Quelle est la répartition de la population selon le genre et le milieu de résidence ?
 select
	r_nom,
	pop_m_u,
	pop_m_r,
	pop_f_u,
	pop_f_r
from r_09;

    r_nom    | pop_m_u | pop_m_r | pop_f_u | pop_f_r
-------------+---------+---------+---------+---------
 SOUSS-MASSA |     0.5 |    0.47 |     0.5 |    0.53