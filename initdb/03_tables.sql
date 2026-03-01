-- Visualization tables for gist_viz.py
-- Pre-create these so QGIS layers can be added before running the animation.

CREATE TABLE IF NOT EXISTS streets_knn (
    geom geometry(MultiLineString, 3857)
);

CREATE INDEX IF NOT EXISTS streets_knn_geom_idx
    ON streets_knn USING gist (geom);

CREATE TABLE IF NOT EXISTS r_tree_l1 (
    geom geometry(Polygon, 3857)
);

CREATE TABLE IF NOT EXISTS r_tree_l2 (
    geom geometry(Polygon, 3857)
);
