#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/load_pilimi_isbndb.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir

rm -f pilimi_isbndb_processed.csv

pv isbndb_2022_09.jsonl.gz | zcat | python3 /scripts/helpers/pilimi_isbndb.py > pilimi_isbndb_processed.csv

# Seems much faster to add the indexes right away than to omit them first and add them later.
pv pilimi_isbndb_processed.csv | mariadb -u root -ppassword allthethings --local-infile=1 --show-warnings -vv -e "DROP TABLE IF EXISTS isbndb_isbns; CREATE TABLE isbndb_isbns (isbn13 CHAR(13) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, isbn10 CHAR(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, json longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL CHECK (json_valid(json)), PRIMARY KEY (isbn13,isbn10), KEY isbn10 (isbn10)) ENGINE=MyISAM; LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE isbndb_isbns FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '';"
