#!/bin/bash

set -Eeuxo pipefail

# libgen.li blocks multiple connections from the same IP address, but we can get around that with a bunch of proxies.
# Fill in the proxies, and rename this file to `download_libgenli_proxies.sh`.
# You don't need unique proxies for all lines; you can also use a limited set and then throw in a `wait` after each set.
# Note that the terminal output will look super garbled when running this! :-)

# After renaming, run this script by running: docker exec -it aa-data-import--mariadb /data-imports/download_libgenli_proxies.sh

cd /temp-dir

# Delete everything so far, so we don't confuse old and new downloads.
rm libgen_new.part*

curl -C - --socks5-hostname socks5://us-atl-wg-socks5-001.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part001.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-101.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part002.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-102.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part003.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-103.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part004.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-104.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part005.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-105.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part006.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-106.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part007.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-107.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part008.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-108.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part009.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-110.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part010.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-201.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part011.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-202.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part012.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-203.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part013.rar &
curl -C - --socks5-hostname socks5://us-atl-wg-socks5-204.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part014.rar &
curl -C - --socks5-hostname socks5://us-chi-wg-socks5-101.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part015.rar &
curl -C - --socks5-hostname socks5://us-chi-wg-socks5-102.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part016.rar &
curl -C - --socks5-hostname socks5://us-chi-wg-socks5-103.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part017.rar &
curl -C - --socks5-hostname socks5://us-chi-wg-socks5-104.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part018.rar &
curl -C - --socks5-hostname socks5://us-chi-wg-socks5-201.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part019.rar &
curl -C - --socks5-hostname socks5://us-chi-wg-socks5-202.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part020.rar &
curl -C - --socks5-hostname socks5://us-chi-wg-socks5-203.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part021.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-101.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part022.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-102.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part023.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-103.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part024.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-104.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part025.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-105.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part026.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-106.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part027.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-107.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part028.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-108.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part029.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-109.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part030.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-110.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part031.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-301.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part032.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-302.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part033.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-303.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part034.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-401.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part035.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-402.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part036.rar &
curl -C - --socks5-hostname socks5://us-dal-wg-socks5-403.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part037.rar &
curl -C - --socks5-hostname socks5://us-den-wg-socks5-001.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part038.rar &
curl -C - --socks5-hostname socks5://us-den-wg-socks5-002.relays.mullvad.net:1080 -O https://libgen.li/dbdumps/libgen_new.part039.rar &
wait

# For good measure
for i in $(seq -w 0 39); do
    # Using curl here since it only accepts one connection from any IP anyway,
    # and this way we stay consistent with `libgenli_proxies_template.sh`.
    curl -C - -O "https://libgen.li/dbdumps/libgen_new.part0${i}.rar"
done
