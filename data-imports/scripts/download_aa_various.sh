#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/download_aa_various.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

rm -f aa_lgli_comics_2022_08_files.sql.gz annas-archive-ia-2023-06-metadata-json.tar.gz annas-archive-ia-2023-06-thumbs.txt.gz

ctorrent -e 0 /scripts/torrents/aa_lgli_comics_2022_08_files.sql.gz.torrent
ctorrent -e 0 /scripts/torrents/annas-archive-ia-2023-06-thumbs.txt.gz.torrent
ctorrent -e 0 /scripts/torrents/annas-archive-ia-2023-06-metadata-json.tar.gz.torrent
