#!/bin/bash

set -Eeuxo pipefail

# libgen.li blocks multiple connections from the same IP address, but we can get around that with a bunch of proxies.
# Fill in the proxies, and rename this file to `download_libgenli_proxies.sh`.
# You don't need unique proxies for all lines; you can also use a limited set and then throw in a `wait` after each set.
# Note that the terminal output will look super garbled when running this! :-)

# After renaming, run this script by running: docker exec -it aa-data-import--mariadb /data-imports/download_libgenli_proxies.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

# Delete everything so far, so we don't confuse old and new downloads.
rm libgen_new.part*

curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part001.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part002.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part003.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part004.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part005.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part006.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part007.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part008.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part009.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part010.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part011.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part012.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part013.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part014.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part015.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part016.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part017.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part018.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part019.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part020.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part021.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part022.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part023.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part024.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part025.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part026.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part027.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part028.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part029.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part030.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part031.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part032.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part033.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part034.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part035.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part036.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part037.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part038.rar &
curl -C - --socks5-hostname (fill in a unique proxy here)  -O https://libgen.li/dbdumps/libgen_new.part039.rar &
wait

# For good measure
for i in $(seq -w 0 39); do
    # Using curl here since it only accepts one connection from any IP anyway,
    # and this way we stay consistent with `libgenli_proxies_template.sh`.
    curl -C - -O "https://libgen.li/dbdumps/libgen_new.part0${i}.rar"
done
