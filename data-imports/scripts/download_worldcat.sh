#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/download_worldcat.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

rm -f WorldCatMostHighlyHeld-2012-05-15.nt.gz

aria2c -c -x16 -s16 -j16 https://archive.org/download/WorldCatMostHighlyHeld20120515.nt/WorldCatMostHighlyHeld-2012-05-15.nt.gz
