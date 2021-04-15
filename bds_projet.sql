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
ALTER TABLE regions
RENAME COLUMN code_regio TO r_code;
--
ALTER TABLE regions
RENAME COLUMN nom_region TO r_nom;