FROM postgres:11.5

ENV WALG_VERSION=v0.2.14

USER root

COPY walg.json /var/lib/postgresql/.walg.json

RUN set -eux \
    # Install deps
    && apt-get update \
    && apt-get install -y \
      ca-certificates curl \
    # Install wal-g
    && curl -sSL "https://github.com/wal-g/wal-g/releases/download/${WALG_VERSION}/wal-g.linux-amd64.tar.gz" -o /tmp/wal-g.linux-amd64.tar.gz \
    && tar -xf /tmp/wal-g.linux-amd64.tar.gz \
    && mv wal-g /usr/bin \
    # Cleanup
    && rm /tmp/wal-g.linux-amd64.tar.gz \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER postgres
