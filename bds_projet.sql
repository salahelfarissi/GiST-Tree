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
shp2pgsql -s 26191 -g geom -I .\communes.shp communes | psql -U elfarissi -d mono
vacuum analyse communes;
