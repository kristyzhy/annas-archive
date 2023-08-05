#!/bin/bash

set -Eeuxo pipefail

# For a faster method, see `download_libgenli_proxies_template.sh`.

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/download_libgenli.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

# Delete everything so far, so we don't confuse old and new downloads.
rm -f libgen_new.part* 

for i in $(seq -w 0 45); do
    # Using curl here since it only accepts one connection from any IP anyway,
    # and this way we stay consistent with `libgenli_proxies_template.sh`.
    curl -C - -O "https://libgen.li/dbdumps/libgen_new.part0${i}.rar"
done
