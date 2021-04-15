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