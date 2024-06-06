#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_aac_zlib3_records.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir/aac_zlib3_records

# TODO: make these files always seekable in torrent.
unzstd --keep annas_archive_meta__aacid__zlib3_records__20230808T014342Z--20240322T220922Z.jsonl.zst
t2sz annas_archive_meta__aacid__zlib3_records__20230808T014342Z--20240322T220922Z.jsonl -l 2 -s 50M -T 32 -o annas_archive_meta__aacid__zlib3_records__20230808T014342Z--20240322T220922Z.jsonl.seekable.zst

rm -f /file-data/annas_archive_meta__aacid__zlib3_records__20230808T014342Z--20240322T220922Z.jsonl.seekable.zst
mv annas_archive_meta__aacid__zlib3_records__20230808T014342Z--20240322T220922Z.jsonl.seekable.zst /file-data/annas_archive_meta__aacid__zlib3_records__20230808T014342Z--20240322T220922Z.jsonl.seekable.zst
