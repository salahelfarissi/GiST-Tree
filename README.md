# R-Tree

## Gevel

[Gevel](http://www.sai.msu.su/~megera/wiki/Gevel "Gevel contrib module") contrib module provides several functions useful for analyzing GiST indexes.

### Functions

- **gist_stat**(*index_name*) : show some statistics about GiST tree
- **gist_tree**(*index_name*, *max_level*) : show GiST tree up to max_level
- **gist_print**(*index_name*) : prints objects stored in GiST tree, works only if objects in index have textual representation

### Code

```sql
SELECT gist_stat({idx_oid});
```

## Psycopg

[Psycopg](https://www.psycopg.org/docs/index.html "Psycopg – PostgreSQL database adapter for Python") is a PostgreSQL database adapter for the Python programming language.

```bash
pip install psycopg2-binary
```
