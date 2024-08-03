#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_openlib.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir/torrents_json

pv torrents.json | mariadb -h ${MARIADB_HOST:-aa-data-import--mariadb} -u root -ppassword allthethings --local-infile=1 --show-warnings -vv -e "DROP TABLE IF EXISTS torrents_json; CREATE TABLE torrents_json (json JSON NOT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin; LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE torrents_json FIELDS TERMINATED BY '\t' ENCLOSED BY '' ESCAPED BY '';"
