# Build exporter
FROM centos:8 AS exporter-builder

WORKDIR /usr/src/
ADD https://www.python.org/ftp/python/3.9.9/Python-3.9.9.tar.xz /usr/src/

RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*
RUN yum update -y
RUN yum -y install curl make gcc openssl-devel bzip2-devel libffi-devel postgresql-devel

RUN tar xvf Python-3.9.9.tar.xz && \
    rm -fr Python-3.9.9.tar.xz && \
    cd Python-3.9.9 && \
    ./configure --prefix=/usr --enable-optimizations --enable-shared && \
    make install -j 8 && \
    cd .. && \
    rm -fr Python-3.9.9 && \
    ldconfig && \
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python3 get-pip.py
COPY requirements.txt /usr/src/
RUN pip3 install -r requirements.txt
ADD exporter.py /usr/src/
RUN pyinstaller --onefile exporter.py && \
    mv dist/exporter wal-g-prometheus-exporter

# Build final image
FROM debian:10-slim

COPY --from=exporter-builder /usr/src/wal-g-prometheus-exporter /usr/bin/
ADD https://github.com/wal-g/wal-g/releases/download/v0.2.14/wal-g.linux-amd64.tar.gz /usr/bin/
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    apt-get upgrade -y -q && \
    apt-get dist-upgrade -y -q && \
    apt-get -y -q autoclean && \
    apt-get -y -q autoremove
RUN cd /usr/bin/ && \
    tar -zxvf wal-g.linux-amd64.tar.gz && \
    rm wal-g.linux-amd64.tar.gz
ENTRYPOINT ["/usr/bin/wal-g-prometheus-exporter"]
