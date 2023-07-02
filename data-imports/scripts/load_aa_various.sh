#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/load_aa_various.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir

pv aa_lgli_comics_2022_08_files.sql.gz | zcat | sed -e 's/^  `path` text NOT NULL,$/  `path` varchar(400) NOT NULL,/' | sed -e 's/^) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;$/,INDEX(md5)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;/g' | mariadb -u root -ppassword allthethings

pv annas-archive-ia-2023-06-files.csv.gz | zcat | mariadb -u root -ppassword allthethings --local-infile=1 --show-warnings -vv -e "DROP TABLE IF EXISTS aa_ia_2023_06_files; CREATE TABLE aa_ia_2023_06_files (md5 CHAR(32) NOT NULL, type CHAR(5) NOT NULL, filesize INT NOT NULL, ia_id VARCHAR(255), PRIMARY KEY (md5), UNIQUE INDEX ia_id (ia_id)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin; LOAD DATA LOCAL INFILE '/dev/stdin' INTO TABLE aa_ia_2023_06_files FIELDS TERMINATED BY ',' ENCLOSED BY '' ESCAPED BY '';"

PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/load_aa_various.py