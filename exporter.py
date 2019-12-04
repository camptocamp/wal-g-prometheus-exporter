import time
import os
import signal
from prometheus_client import start_http_server
from prometheus_client import Gauge
import pyinotify

# Metrics exposed
# ---------------

# Declare metrics
basebackup_gauge = Gauge('walg_basebackup',
                         'Remote Basebackups')
last_xlog_gauge = Gauge('walg_last_xlog_upload',
                        'Last upload of incremental backup', )
last_basebackup_gauge = Gauge('walg_last_basebackup_upload',
                              'Last upload of full backup')
oldest_basebackup_gauge = Gauge('walg_oldest_basebackup_upload',
                                'oldest full backup')
# g.set(4.2)   # Set to a given value


# Base backup update
# ------------------


def update_basebackup(*unused):
    """
        When this script receive a SIGHUP signal, it will call backup-list
        and update metrics about basebackups
    """
    print('Updating basebackups metrics...')


signal.signal(signal.SIGHUP, update_basebackup)
print('My PID is:', os.getpid())

# Wal backup update
# -----------------

class Handler(pyinotify.ProcessEvent):
    def __init__(self, last_xlog_gauge):
        self.last_xlog_gauge = last_xlog_gauge

    def process_default(self, event):
        archive_dir = '/tmp/archive_status'
        last_upload = 0
        for f in os.listdir(archive_dir):
            mtime = os.stat(os.path.join(archive_dir, f)).st_mtime
            if mtime > last_upload:
                last_upload = mtime
        print("Last upload : %s", last_upload)
        self.last_xlog_gauge.set(last_upload)


# Start inotify on the archive_status directory
wal_watcher = pyinotify.WatchManager()

#notifier = pyinotify.Notifier(wal_watcher,
#                              default_proc_fun=Handler(last_xlog_gauge))
notifier = pyinotify.Notifier(wal_watcher)
event_mask = (pyinotify.IN_CREATE | pyinotify.IN_DELETE
              | pyinotify.IN_MODIFY | pyinotify.IN_MOVED_FROM
              | pyinotify.IN_MOVED_TO)
wal_watcher.add_watch('/tmp/archive_status', event_mask)


def on_loop(*arg):
    print("Hello")


if __name__ == '__main__':
    # Start up the server to expose the metrics.
    start_http_server(8000)
    # Watch for events in archive_status
    notifier.loop(callback=on_loop)
