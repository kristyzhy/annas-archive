#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_aa_various.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

rm -f aa_lgli_comics_2022_08_files.sql.gz annas-archive-ia-2023-06-metadata-json.tar.gz annas-archive-ia-2023-06-thumbs.txt.gz annas-archive-ia-2023-06-files.csv.gz

# Tried ctorrent and aria2, but webtorrent seems to work best overall.
webtorrent /scripts/torrents/aa_lgli_comics_2022_08_files.sql.gz.torrent
webtorrent /scripts/torrents/annas-archive-ia-2023-06-thumbs.txt.gz.torrent
webtorrent /scripts/torrents/annas-archive-ia-2023-06-metadata-json.tar.gz.torrent
webtorrent /scripts/torrents/annas-archive-ia-2023-06-files.csv.gz.torrent
