#!/bin/bash

while true; do
  sleep 5
  psql -c 'INSERT INTO data (data) SELECT g.id FROM generate_series(1, 10000) AS g (id);'
done
