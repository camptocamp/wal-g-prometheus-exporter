clean:
	rm -fr build/ dist/ __pycache__

build:
	docker build -t wal-g-prometheus-exporter .
	docker run --name wal-g-prometheus-exporter wal-g-prometheus-exporter
	docker cp wal-g-prometheus-exporter:/usr/src/wal-g-prometheus-exporter ./wal-g-prometheus-exporter
	docker rm wal-g-prometheus-exporter
