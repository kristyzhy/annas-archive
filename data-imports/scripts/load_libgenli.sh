#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/load_libgenli.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir

rm -rf libgen_new /var/lib/mysql/libgen_new /var/lib/mysql/libgen_new

unrar x libgen_new.part001.rar

mv /temp-dir/libgen_new /var/lib/mysql/
chown -R mysql /var/lib/mysql/libgen_new
chgrp -R mysql /var/lib/mysql/libgen_new

mariadb -u root -ppassword --show-warnings -vv < /scripts/helpers/libgenli_pre_export.sql

# Split into multiple lines for easier resuming if one fails.
mysqldump -u root -ppassword libgen_new libgenli_elem_descr         | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb --default-character-set=utf8mb4 -u root -ppassword allthethings
mysqldump -u root -ppassword libgen_new libgenli_files              | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb --default-character-set=utf8mb4 -u root -ppassword allthethings
mysqldump -u root -ppassword libgen_new libgenli_editions           | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb --default-character-set=utf8mb4 -u root -ppassword allthethings
mysqldump -u root -ppassword libgen_new libgenli_editions_to_files  | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb --default-character-set=utf8mb4 -u root -ppassword allthethings
mysqldump -u root -ppassword libgen_new libgenli_editions_add_descr | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb --default-character-set=utf8mb4 -u root -ppassword allthethings
mysqldump -u root -ppassword libgen_new libgenli_files_add_descr    | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb --default-character-set=utf8mb4 -u root -ppassword allthethings
mysqldump -u root -ppassword libgen_new libgenli_series             | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb --default-character-set=utf8mb4 -u root -ppassword allthethings
mysqldump -u root -ppassword libgen_new libgenli_series_add_descr   | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb --default-character-set=utf8mb4 -u root -ppassword allthethings
mysqldump -u root -ppassword libgen_new libgenli_publishers         | PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/sanitize_unicode.py | mariadb --default-character-set=utf8mb4 -u root -ppassword allthethings

echo 'DROP DATABASE libgen_new;' | mariadb -u root -ppassword --show-warnings -vv
