#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/download_pilimi_isbndb.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

rm -f isbndb_2022_09.jsonl.gz

# Tried ctorrent and aria2, but webtorrent seems to work best overall.
webtorrent /scripts/torrents/isbndb_2022_09.torrent
