#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/load_aa_various.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir

pv aa_lgli_comics_2022_08_files.sql.gz | zcat | sed -e 's/^  `path` text NOT NULL,$/  `path` varchar(400) NOT NULL,/' | sed -e 's/^) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;$/,INDEX(md5)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;/g' | mariadb -u root -ppassword allthethings

PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/load_aa_various.py
