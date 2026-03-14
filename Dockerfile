FROM postgis/postgis:13-3.1

# Install build tools and git
RUN apt-get update && apt-get install -y \
    postgresql-server-dev-13 \
    gcc \
    make \
    git \
    clang-11 \
    postgis \
    && rm -rf /var/lib/apt/lists/*

# Clone and build pramsey/gevel (provides gist_stat, gist_tree, gist_print)
# The Makefile's `all` target tries to copy expected/gevel.out.13 (missing for PG13).
# Creating an empty placeholder lets the build proceed; the file is only used for regression tests.
RUN git clone https://github.com/pramsey/gevel.git /tmp/gevel \
    && cd /tmp/gevel \
    && touch expected/gevel.out.13 \
    && make USE_PGXS=1 install \
    && cp gevel.control /usr/share/postgresql/13/extension/ \
    && sed '/^BEGIN;/d;/^END;/d;/^COMMIT;/d' gevel.sql > /usr/share/postgresql/13/extension/gevel--1.1.sql \
    && rm -rf /tmp/gevel

# Init scripts run in alphabetical order on first startup
COPY init.sql /docker-entrypoint-initdb.d/01_init.sql
COPY load_data.sh /docker-entrypoint-initdb.d/03_load_data.sh
RUN chmod +x /docker-entrypoint-initdb.d/03_load_data.sh
