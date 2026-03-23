set dotenv-load := true
set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
  @just --list

build image="gist-tree-db":
  docker build -t {{image}} .

run image="gist-tree-db" container="gist-tree-db" password="postgres" port="5432":
  docker run --name {{container}} -e POSTGRES_PASSWORD={{password}} -p {{port}}:5432 -d {{image}}

start container="gist-tree-db":
  docker start {{container}}

stop container="gist-tree-db":
  docker stop {{container}}

rm container="gist-tree-db":
  -docker rm -f {{container}}

logs container="gist-tree-db":
  docker logs -f {{container}}

psql container="gist-tree-db" user="postgres" db="postgres":
  docker exec -it {{container}} psql -U {{user}} -d {{db}}

wait-ready container="gist-tree-db" user="postgres" db="postgres":
  docker exec {{container}} bash -lc 'for i in $(seq 1 60); do pg_isready -U {{user}} -d {{db}} && exit 0; sleep 1; done; exit 1'

load-data host="localhost" user="postgres" db="postgres" password="postgres" port="5432":
  ogr2ogr -f "PostgreSQL" PG:"host={{host}} user={{user}} dbname={{db}} password={{password}} port={{port}}" \
      data/nyc_streets.gpkg -nln nyc_streets -lco GEOMETRY_NAME=geom -lco FID=gid \
      -nlt MULTILINESTRING -dim XY \
      -emptyStrAsNull -makevalid

smoke image="gist-tree-db" container="gist-tree-db" user="postgres" db="postgres" password="postgres" port="5432":
  just build {{image}}
  -docker rm -f {{container}}
  docker run --name {{container}} -e POSTGRES_PASSWORD={{password}} -p {{port}}:5432 -d {{image}}
  just wait-ready {{container}} {{user}} {{db}}
  docker exec {{container}} psql -U {{user}} -d {{db}} -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('postgis','gevel') ORDER BY extname;"

# Connect to local database with pgcli
pgcli:
    PGPASSWORD=$POSTGRES_PASSWORD pgcli -h localhost -p $POSTGRES_HOST_PORT -U $POSTGRES_USER -d $POSTGRES_DB
