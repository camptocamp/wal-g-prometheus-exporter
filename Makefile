clean:
	rm -fr build/ dist/ __pycache__

build:
	pyinstaller --onefile exporter.py
	mv dist/exporter wal-g-prometheus-exporter
