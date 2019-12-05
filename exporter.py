import os
import signal
import subprocess
import json
import datetime
from prometheus_client import start_http_server
from prometheus_client import Gauge
import pyinotify

# Configuration
# -------------

archive_dir = '/tmp/archive_status'


# Metrics exposed
# ---------------

# Declare metrics
basebackup_count_gauge = Gauge('walg_basebackup_count',
                               'Remote Basebackups count')
basebackup_gauge = Gauge('walg_basebackup',
                         'Remote Basebackups',
                         ['start_wal_segment', 'start_lsn'])
oldest_valid_basebackup_gauge = Gauge('walg_oldest_valid_basebackup',
                                      'Oldest valid backup (without gap)')
last_upload_gauge = Gauge('walg_last_upload',
                          'Last upload of incremental or full backup',
                          ['type'])
oldest_basebackup_gauge = Gauge('walg_oldest_basebackup',
                                'oldest full backup')
xlog_missing_gauge = Gauge('walg_missing_remote_wal_segment',
                         'Xlog missing (gap)')
xlog_ready_gauge = Gauge('walg_missing_remote_wal_segment_at_end',
                         'Xlog ready for upload')
xlog_done_gauge = Gauge('walg_total_remote_wal_count',
                        'Xlog uploaded')
exception_gauge = Gauge('walg_exception',
                        'Wal-g exception: 1 for xlog error and'
                        ' 2 for basebackup error')
xlog_since_last_bb_gauge = Gauge('walg_xlogs_since_basebackup',
                                 'Xlog uploaded since last base backup')
continious_wal_gauge = Gauge('walg_continious_wal',
                             'sequence of xlog without gap')
valid_basebackup_count_gauge = Gauge('walg_valid_basebackup_count',
                                     'Basebackup without gap')
useless_remote_wal_segment_gauge = Gauge('walg_useless_remote_wal_segment',
                                         'Remote useless wal segments')
# TODO:
# * walg_last_basebackup_duration

basebackup_exception = 0
xlog_exception = 0
bbs = []
xlogs_ready = set()
xlogs_done = set()
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


def get_previous_wal(wal):
    timeline = wal[0:8]
    segment_low = int(wal[16:24], 16) - 1
    segment_high = int(wal[8:16], 16) + (segment_low // 0x100)
    segment_low = segment_low % 0x100
    return '%s%08X%08X' % (timeline, segment_high, segment_low)


def get_next_wal(wal):
    timeline = wal[0:8]
    segment_low = int(wal[16:24], 16) + 1
    segment_high = int(wal[8:16], 16) + (segment_low // 0x100)
    segment_low = segment_low % 0x100
    return '%s%08X%08X' % (timeline, segment_high, segment_low)

def is_before(a, b):
    timeline_a = a[0:8]
    timeline_b = b[0:8]
    if timeline_a != timeline_b:
        return False
    a_int = int(a[8:16], 16) * 0x100 + int(a[16:24], 16)
    b_int = int(b[8:16], 16) * 0x100 + int(b[16:24], 16)
    return a_int < b_int


def update_basebackup(*unused):
    """
        When this script receive a SIGHUP signal, it will call backup-list
        and update metrics about basebackups
    """
    # Todo refresh remote xlogs because backup-push migh have remove some xlog
    global basebackup_exception, xlog_exception
    global xlogs_done, xlogs_ready, bbs
    print('Updating basebackups metrics...')
    try:
        res = subprocess.run(["wal-g", "backup-list", "--detail", "--json"],
                             capture_output=True, check=True)
        local_bbs = list(map(format_date, json.loads(res.stdout)))
        local_bbs.sort(key=lambda bb: bb['time'])
        bbs = local_bbs
        print(bbs)
        for bb in bbs:
            (basebackup_gauge.labels(bb['wal_file_name'], bb['start_lsn'])
             .set(bb['time'].timestamp()))
        if len(bbs) > 0:
            oldest_basebackup_gauge.set(bbs[0]['time'].timestamp())
            (last_upload_gauge.labels('basebackup')
             .set(bbs[len(bbs) - 1]['time'].timestamp()))
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
    global basebackup_exception, xlog_exception
    global bbs, xlogs_done, xlogs_ready

    last_upload = 0
    xlog_since_last_bb = 0
    # Read the archive_status directory to find new done or ready xlog
    try:
        for f in os.listdir(archive_dir):
            # Search for last xlog done
            if f[-5:len(f)] == '.done':
                xlogs_done.add(f[0:-5])
                mtime = os.stat(os.path.join(archive_dir, f)).st_mtime
                if mtime > last_upload:
                    last_upload = mtime
            # search for xlog waiting for upload
            elif f[-6:len(f)] == '.ready':
                xlogs_ready.add(f[0:-6])
        xlog_exception = 0
    except FileNotFoundError:
        print("Oups....")
        xlog_exception = 1

    # compute metrics
    compute_complex_metrics()
    if len(bbs) > 0:
        last_bb_position = bbs[len(bbs) - 1]['wal_file_name']
        for xlog in xlogs_done:
            print("Compare %s %s", (last_bb_position, xlog))
            if is_before(last_bb_position, xlog):
                xlog_since_last_bb = xlog_since_last_bb + 1
    xlog_since_last_bb_gauge.set(xlog_since_last_bb)
    last_upload_gauge.labels('xlog').set(last_upload)
    xlog_ready_gauge.set(len(xlogs_ready))
    xlog_done_gauge.set(len(xlogs_done))
    exception_gauge.set(basebackup_exception + xlog_exception)


def compute_complex_metrics():
    """
    Scan xlog to find gaps in sequence
    """
    global bb, xlogs_ready, xlogs_done
    current_xlog = None
    missing_wal = 0
    continious_wal = 0
    oldest_valid_basebackup = None
    valid_basebackup_count = 0
    useless_remote_wal = 0

    for bb in bbs:
        print("Check bb %s %s", (bb['backup_name'], bb['wal_file_name']))
        if oldest_valid_basebackup is None:
            oldest_valid_basebackup = bb['time']
        valid_basebackup_count = valid_basebackup_count + 1

        if current_xlog is None:
            current_xlog = bb["wal_file_name"]
            continue

        while current_xlog != bb["wal_file_name"]:

            if current_xlog in xlogs_done:
                continious_wal = continious_wal + 1
            else:
                missing_wal = missing_wal + 1
                continious_wal = 0
                oldest_valid_basebackup = None
                valid_basebackup_count = 0
            current_xlog = get_next_wal(current_xlog)

    # Now we need to test wal segment to the current master position - 1
    master_position = max(xlogs_done)
    if current_xlog is not None:
        while is_before(current_xlog, master_position):
            if current_xlog in xlogs_done:
                continious_wal = continious_wal + 1
            else:
                # Don't care if it's the last WAL segment, it might be currently uploading
                # Don't reset stats if last WAL segments are missing
                remote_xlog_after_current = [xlog for xlog in xlogs_done if is_before(current_xlog, xlog)]
                if len(remote_xlog_after_current) != 0:
                    missing_wal = missing_wal + 1
                    continious_wal = 0
                    oldest_valid_basebackup = None
                    valid_basebackup_count = 0
            current_xlog = get_next_wal(current_xlog)

    # Search for useless wal segments
    if len(bbs) > 0:
        first_wal_needed = bbs[0]['wal_file_name']
        for wal in xlogs_done:
            if is_before(wal, first_wal_needed):
                useless_remote_wal = useless_remote_wal + 1

    if oldest_valid_basebackup is not None:
        oldest_valid_basebackup_gauge.set(oldest_valid_basebackup.timestamp())

    xlog_missing_gauge.set(missing_wal)
    continious_wal_gauge.set(continious_wal)
    valid_basebackup_count_gauge.set(valid_basebackup_count)
    if len(bbs) > 0:
        useless_remote_wal_segment_gauge.set(useless_remote_wal)


if __name__ == '__main__':
    # TODO : Scan remote xlog and fill xlog_done
    # Check for remote basebackup (this will also check remote xlogs)
    update_basebackup()

    # Start inotify on the archive_status directory
    wal_watcher = pyinotify.WatchManager()

    #notifier = pyinotify.Notifier(wal_watcher,
    #                              default_proc_fun=Handler(last_xlog_gauge))
    notifier = pyinotify.Notifier(wal_watcher)
    event_mask = (pyinotify.IN_CREATE | pyinotify.IN_DELETE
                  | pyinotify.IN_MODIFY | pyinotify.IN_MOVED_FROM
                  | pyinotify.IN_MOVED_TO)
    wal_watcher.add_watch('/tmp/archive_status', event_mask)


    # Start up the server to expose the metrics.
    start_http_server(8000)
    # Watch for events in archive_status
    notifier.loop(callback=update_wal)
