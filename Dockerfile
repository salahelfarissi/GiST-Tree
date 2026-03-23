# syntax=docker/dockerfile:1

ARG POSTGRES_TAG=13
ARG PG_MAJOR=13

FROM postgres:${POSTGRES_TAG} AS gevel-builder

ARG PG_MAJOR
ARG GEVEL_REPO=https://github.com/pramsey/gevel.git
ARG GEVEL_REF=fa5cda2810029683850df2c4cba2f3804b081d7a

WORKDIR /tmp/gevel-src

RUN set -eux; \
    apt-get update; \
    apt-get install --no-install-recommends -y \
        build-essential \
        ca-certificates \
        git \
        "postgresql-server-dev-${PG_MAJOR}"; \
    rm -rf /var/lib/apt/lists/*; \
    git clone "${GEVEL_REPO}" .; \
    git checkout "${GEVEL_REF}"

RUN set -eux; \
    make gevel.so; \
    cp /tmp/gevel-src/gevel.so /tmp/gevel.so; \
    cp /tmp/gevel-src/gevel.control /tmp/gevel.control; \
    awk '!/^[[:space:]]*(BEGIN|END);[[:space:]]*$/' /tmp/gevel-src/gevel.sql > /tmp/gevel.sql

FROM postgres:${POSTGRES_TAG}

ARG PG_MAJOR

RUN set -eux; \
    apt-get update; \
    apt-get install --no-install-recommends -y \
        "postgresql-${PG_MAJOR}-postgis-3" \
        "postgresql-${PG_MAJOR}-postgis-3-scripts"; \
    rm -rf /var/lib/apt/lists/*

COPY --from=gevel-builder /tmp/gevel.so /tmp/gevel.so
COPY --from=gevel-builder /tmp/gevel.control /tmp/gevel.control
COPY --from=gevel-builder /tmp/gevel.sql /tmp/gevel.sql

RUN set -eux; \
    gevel_version="$(awk -F"'" '/^default_version/ {print $2}' /tmp/gevel.control)"; \
    cp /tmp/gevel.so "$(pg_config --pkglibdir)/gevel.so"; \
    cp /tmp/gevel.control "$(pg_config --sharedir)/extension/gevel.control"; \
    cp /tmp/gevel.sql "$(pg_config --sharedir)/extension/gevel--${gevel_version}.sql"; \
    rm -f /tmp/gevel.so /tmp/gevel.control /tmp/gevel.sql

COPY initdb/ /docker-entrypoint-initdb.d/
