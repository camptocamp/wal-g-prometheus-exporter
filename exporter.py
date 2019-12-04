import time
import os
import signal
import subprocess
import json
import datetime
from prometheus_client import start_http_server
from prometheus_client import Gauge
import pyinotify

# Metrics exposed
# ---------------

# Declare metrics
basebackup_count_gauge = Gauge('walg_basebackup_count',
                         'Remote Basebackups count')
basebackup_gauge = Gauge('walg_basebackup',
                         'Remote Basebackups',
                         ['start_wal_segment'])
last_xlog_gauge = Gauge('walg_last_xlog_upload',
                        'Last upload of incremental backup', )
last_basebackup_gauge = Gauge('walg_last_basebackup_upload',
                              'Last upload of full backup')
oldest_basebackup_gauge = Gauge('walg_oldest_basebackup',
                                'oldest full backup')
xlog_ready_gauge = Gauge('walg_missing_remote_wal_segment_at_end',
                         'Xlog ready for upload')
xlog_done_gauge = Gauge('walg_total_remote_wal_count',
                         'Xlog uploaded')
exception_gauge = Gauge('walg_exception',
                        'Wal-g exception: 1 for xlog error and'
                        ' 2 for basebackup error')
# TODO:
# * walg_xlogs_since_basebackup
# * walg_valid_basebackup_count
# * walg_useless_remote_wal_segment
# * walg_oldest_valid_basebackup
# * walg_continious_wal
# g.set(4.2)   # Set to a given value

basebackup_exception = 0
xlog_exception = 0
# Base backup update
# ------------------


def format_date(bb):
    bb['time'] = datetime.datetime.strptime(bb['time'],
                                         bb['date_fmt'])
    bb['start_time'] = datetime.datetime.strptime(bb['start_time'],
                                               bb['date_fmt'])
    bb['finish_time'] = datetime.datetime.strptime(bb['finish_time'],
                                                bb['date_fmt'])
    return bb


def update_basebackup(*unused):
    """
        When this script receive a SIGHUP signal, it will call backup-list
        and update metrics about basebackups
    """
    print('Updating basebackups metrics...')
    try:
        res = subprocess.run(["wal-g", "backup-list", "--detail", "--json"],
                             capture_output=True, check=True)
        bbs = list(map(format_date, json.loads(res.stdout)))
        bbs.sort(key=lambda bb: bb['time'])
        print(bbs)
        for bb in bbs:
            (basebackup_gauge.labels(bb['wal_file_name'])
             .set(bb['time'].timestamp()))
        if len(bbs) > 0:
            oldest_basebackup_gauge.set(bbs[0]['time'].timestamp())
            last_basebackup_gauge.set(bbs[len(bbs) - 1]['time'].timestamp())
        basebackup_exception = 0
    except subprocess.CalledProcessError as e:
        print(e)
        basebackup_exception = 2
    basebackup_count_gauge.set(len(bbs))
    exception_gauge.set(basebackup_exception + xlog_exception)


signal.signal(signal.SIGHUP, update_basebackup)
print('My PID is:', os.getpid())

# Wal backup update
# -----------------

def update_wal(*unused):
    archive_dir = '/tmp/archive_status'
    last_upload = 0
    xlog_ready = 0
    xlog_done = 0
    try:
        for f in os.listdir(archive_dir):
            # Search for last xlog done
            if f[-5:len(f)] == '.done':
                xlog_done = xlog_done + 1
                mtime = os.stat(os.path.join(archive_dir, f)).st_mtime
                if mtime > last_upload:
                    last_upload = mtime
            # search for xlog waiting for upload
            elif f[-6:len(f)] == '.ready':
                xlog_ready = xlog_ready + 1
        xlog_exception = 0
    except FileNotFoundError:
        print("Oups....")
        xlog_exception = 1
    last_xlog_gauge.set(last_upload)
    xlog_ready_gauge.set(xlog_ready)
    xlog_done_gauge.set(xlog_done)
    exception_gauge.set(basebackup_exception + xlog_exception)


# Start inotify on the archive_status directory
wal_watcher = pyinotify.WatchManager()

#notifier = pyinotify.Notifier(wal_watcher,
#                              default_proc_fun=Handler(last_xlog_gauge))
notifier = pyinotify.Notifier(wal_watcher)
event_mask = (pyinotify.IN_CREATE | pyinotify.IN_DELETE
              | pyinotify.IN_MODIFY | pyinotify.IN_MOVED_FROM
              | pyinotify.IN_MOVED_TO)
wal_watcher.add_watch('/tmp/archive_status', event_mask)


if __name__ == '__main__':
    # Start up the server to expose the metrics.
    start_http_server(8000)
    # Watch for events in archive_status
    notifier.loop(callback=update_wal)
