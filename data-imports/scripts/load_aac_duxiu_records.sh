#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_aac_duxiu_records.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir/aac_duxiu_records

PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/load_aac.py /temp-dir/aac/annas_archive_meta__aacid__duxiu_records*
