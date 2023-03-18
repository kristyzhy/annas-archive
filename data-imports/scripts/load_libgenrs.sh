#!/bin/bash

set -Eeuxo pipefail
# https://stackoverflow.com/a/3355423
cd "$(dirname "$0")"

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/load_libgenrs.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir

rm libgen.sql fiction.sql

unrar e libgen.rar
unrar e fiction.rar
pv libgen.sql  | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb --default-character-set=utf8mb4 -u root -ppassword allthethings
pv fiction.sql | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb --default-character-set=utf8mb4 -u root -ppassword allthethings

mariadb -u root -ppassword allthethings --show-warnings -vv < /scripts/helpers/libgenrs_final.sql

rm libgen.sql fiction.sql
