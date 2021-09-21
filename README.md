# WAL-G Prometheus exporter

This exporter should run on the same host a wal-g and takes same envs vars as wal-g for configuration.

The exporter have the following metrics:

- total wal segments: total wal segments on remote storage
- continious wal segments: total wal segments without gap starting from last uploaded
- valid base backup: total basebackup starting from last uploaded without wal segments gap
- missing wal segments: Missing wal segment on remote storage between basebackups
- missing wal segments at end: Missing wal segment near the master position, should not go higher than 1. Replication lag for remote storage
