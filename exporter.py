import os
import signal
import subprocess
import json
import datetime
import re
import argparse
import logging
import time
from logging import warning, info, debug, error  # noqa: F401
from prometheus_client import start_http_server
from prometheus_client import Gauge
import pyinotify
import boto3  # noqa: F401
import botocore
import psycopg2

# Configuration
# -------------

parser = argparse.ArgumentParser()
parser.add_argument("archive_dir",
                    help="pg_wal/archive_status/ Directory location")
parser.add_argument("--debug", help="enable debug log", action="store_true")
args = parser.parse_args()
if args.debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

# Disable logging of libs
for key in logging.Logger.manager.loggerDict:
    if key != 'root':
        logging.getLogger(key).setLevel(logging.WARNING)

archive_dir = args.archive_dir
http_port = 9351
DONE_WAL_RE = re.compile(r"^[A-F0-9]{24}\.done$")
READY_WAL_RE = re.compile(r"^[A-F0-9]{24}\.ready$")
S3_PREFIX_RE = re.compile(r"^s3://(.*)/(.*)$")

# TODO:
# * walg_last_basebackup_duration

# Base backup update
# ------------------


def format_date(bb):
    # fix date format to include timezone
    bb['date_fmt'] = bb['date_fmt'].replace('Z', '%z')
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


class Exporter():

    def __init__(self):
        self.basebackup_exception = 0
        self.xlog_exception = 0
        self.remote_exception = 0
        self.bbs = []
        self.xlogs_ready = set()
        self.xlogs_done = set()
        self.last_wal_check = None
        botocore_session = botocore.session.get_session()
        self.s3_client = botocore_session.create_client(
            "s3",
            endpoint_url=os.getenv('AWS_ENDPOINT'),
            region_name=os.getenv('AWS_REGION'),
        )
        s3_prefix = os.getenv('WALE_S3_PREFIX')
        matches = S3_PREFIX_RE.match(s3_prefix)

        self.s3_bucket = matches.group(1)
        self.s3_prefix = "%s/wal" % matches.group(2)

        # Declare metrics
        self.basebackup_count = Gauge('walg_basebackup_count',
                                      'Remote Basebackups count')
        self.basebackup = Gauge('walg_basebackup',
                                'Remote Basebackups',
                                ['start_wal_segment', 'start_lsn'])
        self.oldest_valid_basebackup = Gauge('walg_oldest_valid_basebackup',
                                             'Oldest valid backup'
                                             '(without gap)')
        self.last_upload = Gauge('walg_last_upload',
                                 'Last upload of incremental or full backup',
                                 ['type'])
        self.oldest_basebackup = Gauge('walg_oldest_basebackup',
                                       'oldest full backup')
        self.xlog_missing = Gauge('walg_missing_remote_wal_segment',
                                  'Xlog missing (gap)')
        self.xlog_ready = Gauge('walg_missing_remote_wal_segment_at_end',
                                'Xlog ready for upload')
        self.xlog_done = Gauge('walg_total_remote_wal_count',
                               'Xlog uploaded')
        self.exception = Gauge('walg_exception',
                               'Wal-g exception: 1 for xlog error and'
                               ' 2 for basebackup error')
        self.xlog_since_last_bb = Gauge('walg_xlogs_since_basebackup',
                                        'Xlog uploaded since last base backup')
        self.continious_wal = Gauge('walg_continious_wal',
                                    'sequence of xlog without gap')
        self.valid_basebackup_count = Gauge('walg_valid_basebackup_count',
                                            'Basebackup without gap')
        self.useless_remote_wal_segment = (
            Gauge('walg_useless_remote_wal_segment',
                  'Remote useless wal segments')
        )

        # first check local xlog, then remotes and finally basebackups
        self.update_wal()
        self.fetch_remote_xlogs()
        self.update_basebackup()

    def _scan_remote_xlogs(self):

        continuation_token = None
        marker = None
        fetch_method = "V2"
        while True:
            s3_args = {
                "Bucket": self.s3_bucket,
                "Prefix": self.s3_prefix,
            }
            if fetch_method == "V2" and continuation_token:
                s3_args["ContinuationToken"] = continuation_token
            if fetch_method == "V1" and marker:
                s3_args["Marker"] = marker

            # Fetch results by on method
            if fetch_method == "V1":
                response = self.s3_client.list_objects(**s3_args)
            elif fetch_method == "V2":
                response = self.s3_client.list_objects_v2(**s3_args)
            else:
                raise Exception("Invalid fetch method")

            # Check if pagination is broken in V2
            if (fetch_method == "V2" and response.get("IsTruncated")
                    and "NextContinuationToken" not in response):
                # Fallback to list_object() V1 if NextContinuationToken
                # is not in response
                warning("Pagination broken, falling back to list_object V1")
                fetch_method = "V1"
                response = self.s3_client.list_objects(**s3_args)

            for item in response.get("Contents", []):
                yield os.path.splitext(os.path.basename(item['Key']))[0]

            if response.get("IsTruncated"):
                if fetch_method == "V1":
                    marker = response.get('NextMarker')
                elif fetch_method == "V2":
                    continuation_token = response["NextContinuationToken"]
                else:
                    raise Exception("Invalid fetch method")
            else:
                break

    def fetch_remote_xlogs(self):
        info("Fetch remote xlogs")

        for xlog in self._scan_remote_xlogs():
            self.xlogs_done.add(xlog)

        info("%s remote xlogs", len(self.xlogs_done))

    def update_basebackup(self, *unused):
        """
            When this script receive a SIGHUP signal, it will call backup-list
            and update metrics about basebackups
        """

        info('Updating basebackups metrics...')
        try:
            res = subprocess.run(["wal-g", "backup-list",
                                  "--detail", "--json"],
                                 capture_output=True, check=True)
            local_bbs = list(map(format_date, json.loads(res.stdout)))
            local_bbs.sort(key=lambda bb: bb['time'])
            self.bbs = local_bbs
            info("%s basebackups found (last: %s)",
                 len(self.bbs),
                 self.bbs[len(self.bbs) - 1]['time'])
            for bb in self.bbs:
                (self.basebackup.labels(bb['wal_file_name'], bb['start_lsn'])
                 .set(bb['time'].timestamp()))
            if self.bbs:
                self.oldest_basebackup.set(self.bbs[0]['time'].timestamp())
                (self.last_upload.labels('basebackup')
                 .set(self.bbs[len(self.bbs) - 1]['time'].timestamp()))
            self.basebackup_exception = 0
        except subprocess.CalledProcessError as e:
            error(e)
            self.basebackup_exception = 2
        self.basebackup_count.set(len(self.bbs))
        self.exception.set(self.basebackup_exception +
                           self.xlog_exception +
                           self.remote_exception)
        # Clean up: Remove xlog deleted on remote storage
        new_xlogs_done = set()
        for xlog in self._scan_remote_xlogs():
            new_xlogs_done.add(xlog)
        diff = self.xlogs_done - new_xlogs_done
        info("%s xlogs removed", len(diff))
        for x in diff:
            self.xlogs_done.remove(x)

    # Wal backup update
    # -----------------
    def update_wal_callback(self, *unused):
        if (self.last_wal_check is None or
                time.time() - self.last_wal_check > 1.0):
            info("Update local wal triggered by inotify")
            self.update_wal()
            self.last_wal_check = time.time()
        else:
            debug("Skip wal check")

    def update_wal(self):
        info("Updating metrics based on local archive_status")

        current_xlog_done = len(self.xlogs_done)
        current_xlog_ready = len(self.xlogs_ready)
        last_upload = 0
        xlog_since_last_bb = 0
        # Read the archive_status directory to find new done or ready xlog
        try:
            for f in os.listdir(archive_dir):
                # Search for last xlog done
                if DONE_WAL_RE.match(f):
                    self.xlogs_done.add(f[0:-5])
                    if f[0:-5] in self.xlogs_ready:
                        self.xlogs_ready.remove(f[0:-5])
                    mtime = os.stat(os.path.join(archive_dir, f)).st_mtime
                    if mtime > last_upload:
                        last_upload = mtime
                # search for xlog waiting for upload
                elif READY_WAL_RE.match(f):
                    self.xlogs_ready.add(f[0:-6])
            self.xlog_exception = 0
        except FileNotFoundError:
            self.xlog_exception = 1

        info("ready diff: %s done diff: %s",
             len(self.xlogs_ready) - current_xlog_ready,
             len(self.xlogs_done) - current_xlog_done)
        # compute metrics
        self.compute_complex_metrics()
        if self.bbs:
            last_bb_position = self.bbs[len(self.bbs) - 1]['wal_file_name']
            for xlog in self.xlogs_done:
                if is_before(last_bb_position, xlog):
                    xlog_since_last_bb = xlog_since_last_bb + 1
        self.xlog_since_last_bb.set(xlog_since_last_bb)
        self.last_upload.labels('xlog').set(last_upload)
        self.xlog_ready.set(len(self.xlogs_ready))
        self.xlog_done.set(len(self.xlogs_done))
        self.exception.set(self.basebackup_exception +
                           self.xlog_exception +
                           self.remote_exception)

    def compute_complex_metrics(self):
        """
        Scan xlog to find gaps in sequence
        """
        current_xlog = None
        missing_wal = 0
        continious_wal = 0
        oldest_valid_basebackup = None
        valid_basebackup_count = 0
        useless_remote_wal = 0

        for bb in self.bbs:
            if oldest_valid_basebackup is None:
                oldest_valid_basebackup = bb['time']
            valid_basebackup_count = valid_basebackup_count + 1

            if current_xlog is None:
                current_xlog = bb["wal_file_name"]
                continue

            while current_xlog != bb["wal_file_name"]:

                if current_xlog in self.xlogs_done:
                    continious_wal = continious_wal + 1
                else:
                    missing_wal = missing_wal + 1
                    continious_wal = 0
                    oldest_valid_basebackup = None
                    valid_basebackup_count = 0
                current_xlog = get_next_wal(current_xlog)

        # Now we need to test wal segment to the current master position - 1
        master_position = max(self.xlogs_done)
        if current_xlog is not None:
            while is_before(current_xlog, master_position):
                if current_xlog in self.xlogs_done:
                    continious_wal = continious_wal + 1
                else:
                    # Don't care if it's the last WAL segment, it might
                    # be currently uploading
                    # Don't reset stats if last WAL segments are missing
                    remote_xlog_after_current = [xlog for xlog
                                                 in self.xlogs_done
                                                 if is_before(current_xlog,
                                                              xlog)]
                    if remote_xlog_after_current:
                        missing_wal = missing_wal + 1
                        continious_wal = 0
                        oldest_valid_basebackup = None
                        valid_basebackup_count = 0
                current_xlog = get_next_wal(current_xlog)

        # Search for useless wal segments
        if self.bbs:
            first_wal_needed = self.bbs[0]['wal_file_name']
            for wal in self.xlogs_done:
                if is_before(wal, first_wal_needed):
                    useless_remote_wal = useless_remote_wal + 1

        if oldest_valid_basebackup is not None:
            self.oldest_valid_basebackup.set(
                oldest_valid_basebackup.timestamp())

        self.xlog_missing.set(missing_wal)
        self.continious_wal.set(continious_wal)
        self.valid_basebackup_count.set(valid_basebackup_count)
        if self.bbs:
            self.useless_remote_wal_segment.set(useless_remote_wal)


if __name__ == '__main__':
    info("Startup...")
    info('My PID is: %s', os.getpid())

    # Start up the server to expose the metrics.
    start_http_server(http_port)
    info("Webserver started on port %s", http_port)

    # Check if this is a master instance
    while True:
        try:
            db_connection = psycopg2.connect(user="postgres",
                                             database="postgres")
            db_connection.autocommit = True
            with db_connection.cursor() as c:
                c.execute("SELECT NOT pg_is_in_recovery()")
                result = c.fetchone()
                if bool(result) and result[0]:
                    break
                else:
                    info("Running on slave, waiting for promotion...")
                    time.sleep(60)
        except Exception:
            error("Unable to connect postgres server, retrying in 60sec...")
            time.sleep(60)

    # Launch exporter
    exporter = Exporter()

    # listen to SIGHUP signal
    signal.signal(signal.SIGHUP, exporter.update_basebackup)

    # Start inotify on the archive_status directory
    wal_watcher = pyinotify.WatchManager()
    notifier = pyinotify.Notifier(wal_watcher)
    event_mask = (pyinotify.IN_CREATE |
                  pyinotify.IN_DELETE |
                  pyinotify.IN_MODIFY |
                  pyinotify.IN_MOVED_FROM |
                  pyinotify.IN_MOVED_TO)
    wal_watcher.add_watch(archive_dir, event_mask)
    info("Inotify watcher started on %s", archive_dir)

    # Watch for events in archive_status
    notifier.loop(callback=exporter.update_wal_callback)
