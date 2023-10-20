#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_scihub.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

rm -f dois-2022-02-12.7z

aria2c -c -x16 -s16 -j16 https://sci-hub.ru/datasets/dois-2022-02-12.7z
