#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_aac.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir/aac

PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/load_aac.py /temp-dir/aac/annas_archive_meta__aacid__zlib3_records* &
job1pid=$!
PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/load_aac.py /temp-dir/aac/annas_archive_meta__aacid__zlib3_files* &
job2pid=$!
PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/load_aac.py /temp-dir/aac/annas_archive_meta__aacid__ia2_records* &
job3pid=$!
PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/load_aac.py /temp-dir/aac/annas_archive_meta__aacid__ia2_acsmpdf_files* &
job4pid=$!

wait $job1pid
wait $job2pid
wait $job3pid
wait $job4pid
