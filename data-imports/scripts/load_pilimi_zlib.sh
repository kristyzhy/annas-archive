#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_pilimi_zlib.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

cd /temp-dir

pv pilimi-zlib2-index-2022-08-24-fixed.sql.gz | zcat | sed -e 's/^) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;$/) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;/g' | mariadb -h aa-data-import--mariadb -u root -ppassword allthethings

mariadb -h aa-data-import--mariadb -u root -ppassword allthethings --show-warnings -vv < /scripts/helpers/pilimi_zlib_final.sql
