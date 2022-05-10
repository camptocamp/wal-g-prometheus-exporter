import os
import os.path
import signal
import subprocess
import json
import datetime
import re
import argparse
import logging
import time
import sys
import threading

from logging import warning, info, debug, error  # noqa: F401
from prometheus_client import start_http_server
from prometheus_client import Gauge
import psycopg2
from psycopg2.extras import DictCursor


# Configuration
# -------------

# Function to check that archiv_dir argument is valid
def valid_archiv_dir(dir):
    found = False
    pg_dir = os.getenv('PGDATA',"/var/lib/postgresql" )
    try:
        for root, dirs, files in os.walk(dir, topdown=True):
            if bool(re.match('^' + pg_dir + '/[0-9]+/.+/pg_wal/archive_status', os.path.join(os.getcwd(), root))):
                found = True
                break
    except FileNotFoundError as e:
        error(e)
    return found


parser = argparse.ArgumentParser()
parser.add_argument("archive_dir",
                    help="pg_wal/archive_status/ Directory location")
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
        level=logging.DEBUG,
        datefmt='%Y-%m-%d %H:%M:%S')

# Disable logging of libs
for key in logging.Logger.manager.loggerDict:
    if key != 'root':
        logging.getLogger(key).setLevel(logging.WARNING)

archive_dir = args.archive_dir
http_port = 9351
DONE_WAL_RE = re.compile(r"^[A-F0-9]{24}\.done$")
READY_WAL_RE = re.compile(r"^[A-F0-9]{24}\.ready$")


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


class Exporter():

    def __init__(self):
        self.basebackup_exception = False
        self.xlog_exception = False
        self.remote_exception = False
        self.bbs = []
        self.last_archive_check = None
        self.archive_status = None

        # Declare metrics
        self.basebackup = Gauge('walg_basebackup',
                                'Remote Basebackups',
                                ['start_wal_segment', 'start_lsn'],
                                unit='seconds')
        self.basebackup_count = Gauge('walg_basebackup_count',
                                      'Remote Basebackups count')
        self.basebackup_count.set_function(lambda: len(self.bbs))

        self.last_upload = Gauge('walg_last_upload',
                                 'Last upload of incremental or full backup',
                                 ['type'],
                                 unit='seconds')
        #Set the time of last uplaod to 0 if none is retieved from pg_stat_archiver table
        if self.last_xlog_upload_callback is not None:
            self.last_upload.labels('xlog').set('0.0')
        else:
            self.last_upload.labels('xlog').set_function(
                self.last_xlog_upload_callback)
        self.last_upload.labels('basebackup').set_function(
            lambda: self.bbs[len(self.bbs) - 1]['start_time'].timestamp()
            if self.bbs else 0
        )
        self.oldest_basebackup = Gauge('walg_oldest_basebackup',
                                       'oldest full backup',
                                       unit='seconds')
        self.oldest_basebackup.set_function(
            lambda: self.bbs[0]['start_time'].timestamp() if self.bbs else 0
        )

        self.xlog_ready = Gauge('walg_missing_remote_wal_segment_at_end',
                                'Xlog ready for upload')
        self.xlog_ready.set_function(self.xlog_ready_callback)

        self.exception = Gauge('walg_exception',
                               'Wal-g exception: '
                               '0 : no exception everything is OK, '
                               '1 : no basebackups found in remote, '
                               '2 : no archives found in local,  '
                               '3 : basebackup and xlog errors '
                               '4 : remote is unreachable, '
                               '6 : no archives found in local & remote is unreachable , ')
        self.exception.set_function(
            lambda: ((1 if self.basebackup_exception else 0) +
                     (2 if self.xlog_exception else 0) +
                     (4 if self.remote_exception else 0) ))

        self.xlog_since_last_bb = Gauge('walg_xlogs_since_basebackup',
                                        'Xlog uploaded since last base backup')
        self.xlog_since_last_bb.set_function(self.xlog_since_last_bb_callback)

        self.last_backup_duration = Gauge('walg_last_backup_duration',
                                          'Duration of the last full backup')
        self.last_backup_duration.set_function(
            lambda: ((self.bbs[len(self.bbs) - 1]['finish_time'] -
                      self.bbs[len(self.bbs) - 1]['start_time']).total_seconds()
                     if self.bbs else 0)
        )
        self.last_backup_size = Gauge('walg_last_backup_size',
                                 'Size of last uploaded backup. Label compression="compressed" for  compressed size and compression="uncompressed" for uncompressed ',
                                 ['compression'],
                                 unit='octets')
        self.last_backup_size.labels('no').set_function(
            lambda: (self.bbs[len(self.bbs) - 1]['uncompressed_size']
                    if self.bbs else 0)
        )
        self.last_backup_size.labels('yes').set_function(
            lambda: (self.bbs[len(self.bbs) - 1]['compressed_size']
                    if self.bbs else 0)
        )
        self.walg_backup_fuse = Gauge('walg_backup_fuse',"0 backup fuse is OK, 1 backup fuse is burnt")
        self.walg_backup_fuse.set_function(self.backup_fuse_callback)
        # Fetch remote base backups
        self.update_basebackup()

    def update_basebackup(self, *unused):
        """
            Update metrics about basebackup by calling backup-list
        """

        info('Updating basebackups metrics...')
        try:
            # Fetch remote backup list
            res = subprocess.run(["wal-g", "backup-list",
                                  "--detail", "--json"],
                                 capture_output=True, check=True)
            new_bbs = list(map(format_date, json.loads(res.stdout)))
            new_bbs.sort(key=lambda bb: bb['start_time'])
            new_bbs_name = [bb['backup_name'] for bb in new_bbs]
            old_bbs_name = [bb['backup_name'] for bb in self.bbs]
            bb_deleted = 0

            # Remove metrics for deleted backups
            for bb in self.bbs:
                if bb['backup_name'] not in new_bbs_name:
                    # Backup deleted
                    self.basebackup.remove(bb['wal_file_name'],
                                           bb['start_lsn'])
                    bb_deleted = bb_deleted + 1
            # Add metrics for new backups
            for bb in new_bbs:
                if bb['backup_name'] not in old_bbs_name:
                    (self.basebackup.labels(bb['wal_file_name'],
                                            bb['start_lsn'])
                     .set(bb['start_time'].timestamp()))
            # Update backup list
            self.bbs = new_bbs
            info("%s basebackups found (first: %s, last: %s), %s deleted",
                 len(self.bbs),
                 self.bbs[0]['start_time'],
                 self.bbs[len(self.bbs) - 1]['start_time'],
                 bb_deleted)

            self.basebackup_exception = False
        except subprocess.CalledProcessError as e:
            error(e.stderr)
            self.remote_exception = True
        except json.decoder.JSONDecodeError:
            info(res.stderr)
            self.basebackup_exception = True

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
            # When last_archived_wal & last_archived_time have no values in pg_stat_archiver table (i.e.: archive_mode='off' )
            if not (bool(res[2]) and bool(res[3])):
                info("Cannot fetch archive status. Postgresql archive_mode should be enabled")
                self.xlog_exception = True
            return res

    def last_xlog_upload_callback(self):
        archive_status = self.last_archive_status()
        if archive_status['last_archived_time'] is not None:
            return archive_status['last_archived_time'].timestamp()

    def xlog_ready_callback(self):
        res = 0
        try:
            for f in os.listdir(archive_dir):
                # search for xlog waiting for upload
                if READY_WAL_RE.match(f):
                    res += 1
                    self.xlog_exception = False
        except FileNotFoundError:
            self.xlog_exception = True
        return res

    def xlog_since_last_bb_callback(self):
        # Compute xlog_since_last_basebackup
        archive_status = self.last_archive_status()
        if self.bbs and archive_status['last_archived_wal'] is not None:
            return wal_diff(archive_status['last_archived_wal'],
                            self.bbs[len(self.bbs) - 1]['wal_file_name'])
        else:
            return 0

    def backup_fuse_callback(self):
        return int(os.path.exists('/tmp/failed_pg_archive'))

if __name__ == '__main__':
    info("Startup...")
    info('My PID is: %s', os.getpid())

    # Start up the server to expose the metrics.
    start_http_server(http_port)
    info("Webserver started on port %s", http_port)

    # Check if this is a master instance
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
                        info("Running on slave, waiting for promotion...")
                        time.sleep(60)
        except Exception as e:
            error(f"Unable to connect to postgres server: {e}, retrying in 60sec...")
            time.sleep(60)

    info("Checking on directory %s if valid ...", archive_dir)
    if valid_archiv_dir(archive_dir):
        info("%s is a complete path to a Postgresql data directory", archive_dir)
    else:
        error("Invalid Argument %s. It is not a path to a Postgresql data directory", archive_dir)
        sys.exit()

    # Launch exporter
    exporter = Exporter()

    # The periodic interval to update basebackup metrics, defaults to 15 minutes
    update_basebackup_interval = float(os.getenv("UPDATE_BASEBACKUP_INTERVAL", "900"))

    ticker = threading.Event()
    while not ticker.wait(update_basebackup_interval):
        exporter.update_basebackup()
