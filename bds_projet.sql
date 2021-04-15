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
--
vacuum analyse communes;
--
select commune from communes
where commune like'Agadir%' or commune like 'Taroudannt%' or commune like 'Ait Baha%' 
or commune like 'Tata %' or commune like 'Tiznit%';
--
drop table communes;
--
select srid, proj4text from spatial_ref_sys where srtext like '%WGS_1984%';
--
shp2pgsql -s 4326 -g geom -I .\provinces.shp provinces | psql -U elfarissi -d mono
-- souss_massa table created
select nom_provin as "name", st_transform(geom, 26192) as geom 
into souss_massa
from provinces
where nom_provin in ('AGADIR IDA OU TANAN', 'INEZGANE AIT MELLOUL', 'TAROUDANNT', 'CHTOUKA AIT BAHA', 'TATA', 'TIZNIT');
--
select st_srid(geom) from souss_massa limit 1;
--
select max(length(name)) from souss_massa;
--
alter table souss_massa
alter column "name" type varchar(20);
--
update souss_massa
set "name" = 'AGADIR IDA OU TANANE'
where "name" = 'AGADIR IDA OU TANAN';