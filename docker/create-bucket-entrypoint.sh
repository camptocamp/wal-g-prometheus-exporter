#!/bin/bash

# Wait minio to start

while true; do
  aws --endpoint-url $MINIO_URL s3 mb s3://$BUCKET 2>&1 | grep 'BucketAlreadyOwnedByYou'
  ret=$?
  echo "Return code $ret"
  if [ $ret -eq 0 ]; then
    echo "Bucket created"
    break
  else
    echo "Minio not started waiting..."
    sleep 1
  fi
done

# Wait forever
while true; do
  sleep 1
done
