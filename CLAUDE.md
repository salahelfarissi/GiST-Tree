# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a GiST (Generalized Search Tree) index visualization tool for PostgreSQL spatial indexes. It connects to a local PostgreSQL database containing NYC geodata and renders the R-tree bounding boxes of GiST indexes at different tree levels. Visualization output goes to QGIS via PostgreSQL `NOTIFY` triggers.

## Prerequisites

- **Docker** and **Docker Compose**
- **Python dependencies**: `psycopg2-binary`, `pandas`
- **QGIS** for rendering bounding box output

```bash
pip install psycopg2-binary pandas
```

## Database Setup (Docker)

The included `gevel_ext.dll` is Windows-only. The `Dockerfile` clones the original gevel C source from `git://sigaev.ru/gevel` and compiles it during the image build.

**1. Start the database:**
```bash
docker compose up -d --build
```

This builds a `postgis/postgis:13-3.1` image with gevel (from `https://github.com/pramsey/gevel`) compiled and installed, creates the `nyc` database, installs PostGIS and the gevel extension (`gist_stat`, `gist_tree`, `gist_print`), and runs `queries/functions.sql` to create the `indices()`, `g_srid()`, and `g_type()` helper functions.

**2. Load NYC geodata** into the running container using `shp2pgsql` or `ogr2ogr` (not included in this repo).

**3. Update the Python connection string** in `r_tree.py` and `gist_viz.py` to add the password:
```python
conn = connect("""
    host=localhost
    dbname=nyc
    user=postgres
    password=postgres
    """)
```

## Running the Scripts

**Interactive R-tree visualizer** — prompts for index OID and level, writes bboxes to `r_tree` table, notifies QGIS:
```bash
python r_tree.py
```

**Animated GiST growth visualizer** — inserts streets one-by-one via KNN, tracks index stats after each insert, writes bboxes to `r_tree_l1`/`r_tree_l2` tables:
```bash
python gist_viz.py
```

## Architecture

- **`func.py`** — shared utilities: `unpack()` parses `gist_stat()` text output into a dict; `field_width()` computes column widths for terminal table display.
- **`r_tree.py`** — interactive script: lists all spatial indexes with OIDs, prompts user to pick one and a level, extracts bounding boxes via `gist_print()`, writes to `r_tree` table, sends `NOTIFY qgis` to refresh QGIS.
- **`gist_viz.py`** — animation script: incrementally inserts NYC streets into `streets_knn` using KNN ordering, calls `VACUUM ANALYZE` and `gist_stat()` after each insert, writes current bboxes to `r_tree_l1`/`r_tree_l2` tables.
- **`queries/functions.sql`** — SQL functions: `indices()` lists all spatial GiST indexes with OIDs; `g_srid(oid)` returns SRID; `g_type(oid)` returns geometry type.
- **`gevel_ext/`** — original Windows-only build artifacts (unused in Docker). The Docker setup compiles gevel from source instead.
- **`Dockerfile` / `docker-compose.yml`** — builds a PostGIS 13 image with gevel compiled from `git://sigaev.ru/gevel`.

## Key Data Flow

1. `gist_print(index_oid)` → returns `(level, valid, box2df)` records
2. `box2df` values are cast to `box2d` geometry with the index's SRID via `postgis.st_setsrid`
3. Result geometries are written to staging tables (`r_tree`, `r_tree_l1`, `r_tree_l2`)
4. `NOTIFY qgis` signals QGIS to refresh its layer view of those tables

## Database Connection

Both scripts connect to `host=localhost dbname=nyc user=postgres` (no password). Modify the `connect()` call at the top of each script to change connection parameters.
