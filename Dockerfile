# Build exporter
FROM centos:7 AS exporter-builder

WORKDIR /usr/src/
ADD https://www.python.org/ftp/python/3.7.5/Python-3.7.5.tgz  /usr/src/
RUN yum -y install curl make gcc openssl-devel bzip2-devel libffi-devel
RUN tar xzf Python-3.7.5.tgz && \
    rm -fr Python-3.7.5.tgz && \
    cd Python-3.7.5 && \
    ./configure --prefix=/usr --enable-optimizations --enable-shared && \
    make install -j 8 && \
    cd .. && \
    rm -fr Python-3.7.5 && \
    ldconfig && \
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python3 get-pip.py

COPY requirements.txt /usr/src/
RUN pip3 install -r requirements.txt
ADD exporter.py /usr/src/
RUN pyinstaller --onefile exporter.py && \
    mv dist/exporter wal-g-prometheus-exporter
ENTRYPOINT ["/usr/src/wal-g-prometheus-exporter"]

# Build Walg-g
FROM golang:1.13 as wal-g-builder
RUN go get github.com/wal-g/wal-g@[v0.2.14] || true && \
    apt update && \
    DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y cmake liblzo2-dev
WORKDIR /go/src/github.com/wal-g/wal-g
RUN make install deps pg_build

# Build final image
FROM busybox:glibc

COPY --from=exporter-builder /usr/src/wal-g-prometheus-exporter /usr/bin/
COPY --from=wal-g-builder /go/src/github.com/wal-g/wal-g/main/pg/wal-g /usr/bin/
