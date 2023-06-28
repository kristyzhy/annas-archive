#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/download_aa_lgli_comics_2022_08_files.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

rm -f aa_lgli_comics_2022_08_files.sql.gz

ctorrent -e 0 /scripts/torrents/aa_lgli_comics_2022_08_files.sql.gz.torrent
