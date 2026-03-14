#!/bin/bash
set -eo pipefail

echo "Loading NYC shapefiles..."

for shp in /data/*.shp; do
    table=$(basename "$shp" .shp)
    echo "  -> $table"
    shp2pgsql -s 26918:3857 -c -I "$shp" "public.$table" | psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"
done

echo "Done loading NYC data."
