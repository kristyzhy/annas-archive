#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_worldcat.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir/worldcat

# TODO: make these files always seekable in torrent.
unzstd --keep annas_archive_meta__aacid__worldcat__20231001T025039Z--20231001T235839Z.jsonl.zst
t2sz annas_archive_meta__aacid__worldcat__20231001T025039Z--20231001T235839Z.jsonl -l 2 -s 50M -T 32 -o annas_archive_meta__aacid__worldcat__20231001T025039Z--20231001T235839Z.jsonl.seekable.zst

rm -f /aa-data-import--allthethings-file-data/annas_archive_meta__aacid__worldcat__20231001T025039Z--20231001T235839Z.jsonl.seekable.zst
mv annas_archive_meta__aacid__worldcat__20231001T025039Z--20231001T235839Z.jsonl.seekable.zst /aa-data-import--allthethings-file-data/annas_archive_meta__aacid__worldcat__20231001T025039Z--20231001T235839Z.jsonl.seekable.zst
