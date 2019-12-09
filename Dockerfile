FROM centos:7


WORKDIR /usr/src/
RUN yum -y install make gcc openssl-devel bzip2-devel libffi-devel
ADD https://www.python.org/ftp/python/3.7.5/Python-3.7.5.tgz  /usr/src/
RUN tar xzf Python-3.7.5.tgz && \
    rm -fr Python-3.7.5.tgz && \
    cd Python-3.7.5 && \
    ./configure --prefix=/usr --enable-optimizations --enable-shared && \
    make install -j 8 && \
    cd .. && \
    rm -fr Python-3.7.5


RUN yum -y install curl && \
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python3 get-pip.py

#RUN yum -y install python3 python3-pip

COPY requirements.txt /usr/src/
RUN pip3 install -r requirements.txt
ADD exporter.py /usr/src/
RUN pyinstaller --onefile exporter.py && \
    mv dist/exporter wal-g-prometheus-exporter
