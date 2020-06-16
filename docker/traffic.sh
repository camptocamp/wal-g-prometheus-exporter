#!/bin/bash

while true; do
  sleep 1
  psql -c 'INSERT INTO data (data) SELECT g.id FROM generate_series(1, 50000) AS g (id);'
done
