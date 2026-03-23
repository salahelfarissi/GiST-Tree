# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GiST-Tree is a visualization tool for PostgreSQL GiST (Generalized Search Tree) indexes, specifically R-tree indexes on spatial data. It uses the Gevel extension for index diagnostics and Python + QGIS for visualization.

**Stack:** PostgreSQL 13 + PostGIS 3 + Gevel extension, Dockerized. Python (psycopg2, pandas) for querying and visualization. QGIS for rendering spatial bounding boxes.

## Build & Run Commands

All commands use `just` (a task runner). The database runs in Docker on port **5433** (mapped to container's 5432).

```bash
just build              # Build Docker image (gist-tree-db)
just run                # Run container (port 5433)
just wait-ready         # Wait for PostgreSQL to accept connections
just enable-extensions  # Install PostGIS and Gevel extensions
just smoke              # Full smoke test: build → run → wait → enable → verify
just psql               # Interactive psql session in container
just logs               # Tail container logs
just stop / just start  # Container lifecycle
just rm                 # Remove container
```

## Architecture

- **Dockerfile** — Multi-stage build: compiles Gevel from source (stage 1), then layers it onto PostGIS runtime (stage 2)
- **queries/functions.sql** — SQL helper functions: `indices()` lists GiST indexes, `g_srid()`/`g_type()` get spatial metadata
- **src/gist_tree/func.py** — Python utilities: `unpack()` parses `gist_stat()` output, `field_width()` for text table formatting
- **src/gist_tree/r_tree.py** — Interactive script: prompts user to select a GiST index and level, creates `r_tree` table with bounding box geometries, signals QGIS via NOTIFY. Entry point: `uv run r-tree`
- **src/gist_tree/gist_viz.py** — Animated visualization: incrementally builds KNN index on NYC streets data, captures per-step statistics, generates level-specific bounding box tables. Entry point: `uv run gist-viz`
- **data/** — NYC streets shapefile (sample dataset)

## Python Environment

Uses Python 3.13 via uv (`.venv/`). Dependencies: `psycopg`, `pandas`. Install/sync with `uv sync`.
