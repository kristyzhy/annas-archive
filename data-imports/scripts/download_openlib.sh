#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_openlib.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

rm -f ol_dump_latest.txt.gz
aria2c -c -x16 -s16 -j16 -o ol_dump_latest.txt.gz 'https://openlibrary.org/data/ol_dump_latest.txt.gz' # Explicitly adding -o since they redirect to a different filename.
