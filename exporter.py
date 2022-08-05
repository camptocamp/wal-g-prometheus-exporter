import os
import os.path
import subprocess
import json
import datetime
import re
import argparse
import logging
import time
import threading

from logging import warning, info, debug, error  # noqa: F401
from prometheus_client import start_http_server
from prometheus_client import Gauge
import psycopg2
from psycopg2.extras import DictCursor

# Configuration
# -------------

parser = argparse.ArgumentParser()
parser.add_argument("--debug", help="enable debug log", action="store_true")
args = parser.parse_args()
if args.debug:
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')
else:
    logging.basicConfig(
        format='%(asctime)s %(levelname)-8s %(message)s',
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S')

# Disable logging of libs
for key in logging.Logger.manager.loggerDict:
    if key != 'root':
        logging.getLogger(key).setLevel(logging.WARNING)

http_port = int(os.getenv("WALG_EXPORTER_PORT", "9351"))


# Base backup update
# ------------------


def format_date(bb):
    # fix date format to include timezone
    bb['date_fmt'] = bb['date_fmt'].replace('Z', '%z')
    bb['time'] = parse_date(bb['time'], bb['date_fmt'])
    bb['start_time'] = parse_date(bb['start_time'], bb['date_fmt'])
    bb['finish_time'] = parse_date(bb['finish_time'], bb['date_fmt'])
    return bb


def parse_date(date, fmt):
    fmt = fmt.replace('Z', '%z')
    try:
        return datetime.datetime.strptime(date, fmt)
    except ValueError:
        fmt = fmt.replace('.%f', '')
        return datetime.datetime.strptime(date, fmt)


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


def wal_diff(a, b):
    timeline_a = a[0:8]
    timeline_b = b[0:8]
    if timeline_a != timeline_b:
        return -1
    a_int = int(a[8:16], 16) * 0x100 + int(a[16:24], 16)
    b_int = int(b[8:16], 16) * 0x100 + int(b[16:24], 16)
    return a_int - b_int

def is_delta(bb):
    if re.match(r"^.*_D_.*$", bb['backup_name']):
        return 'delta'
    else:
        return 'full'

class Exporter():

    def __init__(self):
        self.basebackup_exception = False
        self.xlog_exception = False
        self.remote_exception = False
        self.bbs = []
        self.last_archive_check = None
        self.archive_status = None

        # Declare metrics

        self.basebackup = Gauge('walg_basebackup', 'Remote Basebackups', ['start_wal_segment', 'start_lsn', 'backup', 'version'], unit='seconds')
        self.basebackup_count = Gauge('walg_basebackup_count', 'Remote Basebackups count', ['target'])
        self.last_upload = Gauge('walg_last_upload', 'Last upload of incremental or full backup', ['type', 'version'], unit='seconds')
        self.oldest_basebackup = Gauge('walg_oldest_basebackup', 'oldest full backup', ['version'], unit='seconds')
        self.xlog_ready = Gauge('walg_missing_remote_wal_segment_at_end', 'Xlog ready for upload', ['version'])
        self.exception = Gauge('walg_exception',
                               'Wal-g exception: '
                               '0 : no exception everything is OK, '
                               '1 : no basebackups found in remote, '
                               '2 : no archives found in local,  '
                               '3 : basebackup and xlog errors '
                               '4 : remote is unreachable, '

                               '6 : no archives found in local & remote is unreachable.', ['version'])

        self.xlog_since_last_bb = Gauge('walg_xlogs_since_basebackup', 'Xlog uploaded since last base backup', ['version'])


        self.last_backup_duration = Gauge('walg_last_backup_duration', 'Duration of the last full backup', ['version'], unit='seconds')

        self.last_backup_size = Gauge('walg_last_backup_size', 'Size of last uploaded backup. Label compression="compressed" for  compressed size and compression="uncompressed" for uncompressed ', ['compression', 'version'], unit='bytes')

        self.fetch_metrics()

    def fetch_metrics(self):
        self.is_primary()
        self.update_basebackup()
        self.basebackup_count.labels('0.1.4').set_function(lambda: len(self.bbs))
        if self.last_xlog_upload_callback is not None:
            self.last_upload.labels('xlog', '0.1.4').set('0.0')
        else:
            self.last_upload.labels('xlog', '0.1.4').set_function(self.last_xlog_upload_callback)
        self.last_upload.labels('basebackup', '0.1.4').set_function(lambda: self.bbs[len(self.bbs) - 1]['start_time'].timestamp() if self.bbs else 0 )
        self.oldest_basebackup.labels('0.1.4').set_function(lambda: self.bbs[0]['start_time'].timestamp() if self.bbs else 0)
        self.xlog_ready.labels('0.1.4').set_function(self.xlog_ready_callback)
        self.exception.labels('0.1.4').set_function(lambda: ((1 if self.basebackup_exception else 0) + (2 if self.xlog_exception else 0) + (4 if self.remote_exception else 0)))
        self.xlog_since_last_bb.labels('0.1.4').set_function(self.xlog_since_last_bb_callback)
        self.last_backup_duration.labels('0.1.4').set_function(lambda: ((self.bbs[len(self.bbs) - 1]['finish_time'] - self.bbs[len(self.bbs) - 1]['start_time']).total_seconds() if self.bbs else 0))
        self.last_backup_size.labels('compressed', '0.1.4').set_function(lambda: (self.bbs[len(self.bbs) - 1]['compressed_size'] if self.bbs else 0))
        self.last_backup_size.labels('uncompressed', '0.1.4').set_function(lambda: (self.bbs[len(self.bbs) - 1]['uncompressed_size'] if self.bbs else 0))


    def update_basebackup(self, *unused):
        try:
            # Fetch remote backup list
            res = subprocess.run(["wal-g", "backup-list", "--detail", "--json"], capture_output=True, check=True)
            new_bbs = list(map(format_date, json.loads(res.stdout)))
            new_bbs.sort(key=lambda bb: bb['start_time'])
            new_bbs_name = [bb['backup_name'] for bb in new_bbs]
            old_bbs_name = [bb['backup_name'] for bb in self.bbs]
            bb_deleted = 0

            # Remove metrics for deleted backups
            for bb in self.bbs:
                if bb['backup_name'] not in new_bbs_name:
                    # Backup deleted

                    self.basebackup.remove(bb['wal_file_name'], bb['start_lsn'], is_delta(bb), '0.1.4')

                    bb_deleted = bb_deleted + 1
            # Add metrics for new backups
            for bb in new_bbs:
                if bb['backup_name'] not in old_bbs_name:
                    (self.basebackup.labels(bb['wal_file_name'], bb['start_lsn'], is_delta(bb), '0.1.4')

                     .set(bb['start_time'].timestamp()))
            # Update backup list
            self.bbs = new_bbs
            info("%s basebackups found (first: %s, last: %s), %s deleted",
                 len(self.bbs),
                 self.bbs[0]['start_time'],
                 self.bbs[len(self.bbs) - 1]['start_time'],
                 bb_deleted)
            self.remote_exception = False
            self.basebackup_exception = False
        except subprocess.CalledProcessError as e:
            error(e.stderr)
            self.remote_exception = True
            self.basebackup._metrics = {}
            self.bbs = []
        except json.decoder.JSONDecodeError:
            info(res.stderr)
            self.basebackup_exception = True
            self.basebackup._metrics = {}
            self.bbs = []
    def last_archive_status(self):
        if (self.last_archive_check is None or
                datetime.datetime.now().timestamp() -
                self.last_archive_check > 1):
            self.archive_status = self._last_archive_status()
            self.last_archive_check = datetime.datetime.now().timestamp()
        return self.archive_status

    def _last_archive_status(self):
        with psycopg2.connect(
                host=os.getenv('PGHOST', 'localhost'),
                port=os.getenv('PGPORT', '5432'),
                user=os.getenv('PGUSER', 'postgres'),
                password=os.getenv('PGPASSWORD'),
                dbname=os.getenv('PGDATABASE', 'postgres'),

        ) as db_connection:
            db_connection.autocommit = True
            with db_connection.cursor(cursor_factory=DictCursor) as c:
                c.execute('SELECT archived_count, failed_count, '
                          'last_archived_wal, '
                          'last_archived_time, '
                          'last_failed_wal, '
                          'last_failed_time '
                          'FROM pg_stat_archiver')
                res = c.fetchone()
        if not (bool(res[2]) and bool(res[3])):
            self.xlog_exception = True
        else:
            self.xlog_exception = False
        return res

    def last_xlog_upload_callback(self):
        archive_status = self.last_archive_status()
        if archive_status['last_archived_time'] is not None:
            return archive_status['last_archived_time'].timestamp()

    def xlog_ready_callback(self):
        with psycopg2.connect(
                host=os.getenv('PGHOST', '127.0.0.1'),
                port=os.getenv('PGPORT', '5432'),
                user=os.getenv('PGUSER', 'postgres'),
                password=os.getenv('PGPASSWORD'),
                dbname=os.getenv('PGDATABASE', 'postgres'),

        ) as db_connection:
            db_connection.autocommit = True
            with db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as c:
                c.execute(
                    "SELECT COUNT(*) FROM pg_ls_archive_statusdir() WHERE pg_ls_archive_statusdir.name ~ '^[0-9A-F]{24}.ready';")
                res = c.fetchone()
        return res[0]

    def xlog_since_last_bb_callback(self):
        # Compute xlog_since_last_basebackup
        archive_status = self.last_archive_status()
        if self.bbs and archive_status['last_archived_wal'] is not None:
            return wal_diff(archive_status['last_archived_wal'],
                            self.bbs[len(self.bbs) - 1]['wal_file_name'])
        else:
            return 0

    def flush_metrics(self):

        self.basebackup_count._metrics = {}
        self.last_upload._metrics = {}
        self.oldest_basebackup._metrics = {}
        self.xlog_ready._metrics = {}
        self.exception._metrics = {}
        self.xlog_since_last_bb._metrics = {}
        self.last_backup_duration._metrics = {}
        self.last_backup_size._metrics = {}
        self.basebackup._metrics = {}


    # Check if this is a master instance
    def is_primary(self):
        while True:
            try:
                with psycopg2.connect(
                        host=os.getenv('PGHOST', 'localhost'),
                        port=os.getenv('PGPORT', '5432'),
                        user=os.getenv('PGUSER', 'postgres'),
                        password=os.getenv('PGPASSWORD'),
                        dbname=os.getenv('PGDATABASE', 'postgres'),

                ) as db_connection:
                    db_connection.autocommit = True
                    with db_connection.cursor() as c:
                        c.execute("SELECT NOT pg_is_in_recovery()")
                        result = c.fetchone()
                        if bool(result) and result[0]:
                            break
                        else:
                            if self.bbs is not None:
                                self.flush_metrics()
                            info("Running on slave, waiting for promotion...")
                            time.sleep(60)
            except Exception as e:
                if self.bbs is not None:
                    self.flush_metrics()
                error(f"Unable to connect to postgres server: {e}, retrying in 60sec...")
                time.sleep(60)



if __name__ == '__main__':
    info("Startup...")
    info('My PID is: %s', os.getpid())

    # Start up the server to expose the metrics.
    start_http_server(http_port)
    info("Webserver started on port %s", http_port)

    # Launch exporter
    exporter = Exporter()

    # The periodic interval to update basebackup metrics, defaults to 5 minutes
    update_basebackup_interval = float(os.getenv("UPDATE_BASEBACKUP_INTERVAL", "300"))


    ticker = threading.Event()

    while not ticker.wait(update_basebackup_interval):
        exporter.fetch_metrics()
