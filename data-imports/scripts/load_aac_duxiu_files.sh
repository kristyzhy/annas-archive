#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_aac_duxiu_files.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir/aac_duxiu_files

# TODO: make these files always seekable in torrent.
unzstd --keep annas_archive_meta__aacid__duxiu_files__20240229T082726Z--20240229T131900Z.jsonl.zst
t2sz annas_archive_meta__aacid__duxiu_files__20240229T082726Z--20240229T131900Z.jsonl -l 2 -s 50M -T 32 -o annas_archive_meta__aacid__duxiu_files__20240229T082726Z--20240229T131900Z.jsonl.seekable.zst

rm -f /file-data/annas_archive_meta__aacid__duxiu_files__20240229T082726Z--20240229T131900Z.jsonl.seekable.zst
mv annas_archive_meta__aacid__duxiu_files__20240229T082726Z--20240229T131900Z.jsonl.seekable.zst /file-data/annas_archive_meta__aacid__duxiu_files__20240229T082726Z--20240229T131900Z.jsonl.seekable.zst
