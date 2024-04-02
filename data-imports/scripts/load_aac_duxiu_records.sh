#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_aac_duxiu_records.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir/aac_duxiu_records

PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/load_aac.py /temp-dir/aac/annas_archive_meta__aacid__duxiu_records*

echo 'CREATE TABLE annas_archive_meta__aacid__duxiu_records_by_filename_decoded (aacid VARCHAR(250) NOT NULL, filename_decoded VARCHAR(8000) NOT NULL, PRIMARY KEY(aacid), INDEX filename_decoded (filename_decoded(100))) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin SELECT aacid, JSON_EXTRACT(metadata, "$.record.filename_decoded") AS filename_decoded FROM annas_archive_meta__aacid__duxiu_records WHERE JSON_EXTRACT(metadata, "$.record.filename_decoded") IS NOT NULL;' | mariadb -h aa-data-import--mariadb -u root -ppassword --show-warnings -vv
