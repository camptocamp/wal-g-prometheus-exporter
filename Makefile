clean:
	rm -fr build/ dist/ __pycache__

build:
	pyinstaller exporter.py
	mv dist/exporter/exporter wal-prometheus-exporter
