create database postgis;
-- \c postgis
create SCHEMA postgis;
CREATE EXTENSION postgis SCHEMA postgis;
-- setting postgis db as template
UPDATE pg_database SET datistemplate = TRUE WHERE datname = 'postgis';
-- 
create database mono template postgis;
ALTER DATABASE mono SET search_path='$user', public, postgis;
--
select srid, proj4text from spatial_ref_sys where srtext like '%WGS_1984%';
--
shp2pgsql -s 4326 -g geom -I .\regions.shp regions | psql -U elfarissi -d mono
shp2pgsql -s 4326 -g geom -I .\provinces.shp provinces | psql -U elfarissi -d mono
shp2pgsql -s 4326 -g geom -I .\communes.shp communes | psql -U elfarissi -d mono
-- update statistics
vacuum analyze regions, provinces, communes;
--
delete from regions where code_regio is null;
--
ALTER TABLE regions
RENAME COLUMN code_regio TO r_code;
--
ALTER TABLE regions
RENAME COLUMN nom_region TO r_nom;
--
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
--
alter table regions
alter column r_code type varchar(3);
--
ALTER TABLE regions ADD COLUMN id serial primary key;
--
update communes
set nom_commun = upper(nom_commun);
--
select * 
into c_09
from communes where c_code like '09.%';
--
select * 
into p_09
from provinces where p_code like '09.%';
--
select * 
into r_09
from regions
where r_code = '09.';
--
CREATE INDEX r_09_geom_idx
ON r_09
USING gist(geom);
--
update p_09 set menages = case
when p_nom = 'AGADIR IDA OU TANAN' then 163283
when p_nom = 'CHTOUKA AIT BAHA' then 99852
when p_nom = 'INEZGANE AIT MELLOUL' then 142549
when p_nom = 'TAROUDANNT' then 180895
when p_nom = 'TATA' then 22675
when p_nom = 'TIZNIT' then 52671
end;
-- Q1
select p_nom, menages from p_09
order by menages desc
limit 1;
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