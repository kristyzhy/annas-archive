#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/download_aac.sh
# Download scripts are idempotent but will RESTART the download from scratch!

rm -rf /temp-dir/aac
mkdir /temp-dir/aac

cd /temp-dir/aac

curl -C - -O https://annas-archive.org/torrents/latest_aac_meta/zlib3_records.torrent
curl -C - -O https://annas-archive.org/torrents/latest_aac_meta/zlib3_files.torrent
curl -C - -O https://annas-archive.org/torrents/latest_aac_meta/ia2_acsmpdf_files.torrent

# Tried ctorrent and aria2, but webtorrent seems to work best overall.
webtorrent download zlib3_records.torrent
webtorrent download zlib3_files.torrent
webtorrent download ia2_acsmpdf_files.torrent
