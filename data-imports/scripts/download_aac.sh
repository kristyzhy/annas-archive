#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_aac.sh
# Download scripts are idempotent but will RESTART the download from scratch!

rm -rf /temp-dir/aac
mkdir /temp-dir/aac

cd /temp-dir/aac

curl -C - -O https://annas-archive.org/dyn/torrents/latest_aac_meta/zlib3_records.torrent
curl -C - -O https://annas-archive.org/dyn/torrents/latest_aac_meta/zlib3_files.torrent
curl -C - -O https://annas-archive.org/dyn/torrents/latest_aac_meta/ia2_records.torrent
curl -C - -O https://annas-archive.org/dyn/torrents/latest_aac_meta/ia2_acsmpdf_files.torrent
curl -C - -O https://annas-archive.org/dyn/torrents/latest_aac_meta/duxiu_records.torrent
curl -C - -O https://annas-archive.org/dyn/torrents/latest_aac_meta/duxiu_files.torrent

# Tried ctorrent and aria2, but webtorrent seems to work best overall.
webtorrent download zlib3_records.torrent &
job1pid=$!
webtorrent download zlib3_files.torrent &
job2pid=$!
webtorrent download ia2_records.torrent &
job3pid=$!
webtorrent download ia2_acsmpdf_files.torrent &
job4pid=$!
webtorrent download duxiu_records.torrent &
job5pid=$!
webtorrent download duxiu_files.torrent &
job6pid=$!

wait $job1pid
wait $job2pid
wait $job3pid
wait $job4pid
wait $job5pid
wait $job6pid
