#!/bin/bash

set -Eeuxo pipefail

# For a faster method, see `download_libgenli_proxies_template.sh`.

# Run this script by running: docker exec -it aa-data-import--web /scripts/download_libgenli.sh
# Download scripts are idempotent but will RESTART the download from scratch!

cd /temp-dir

# Delete everything so far, so we don't confuse old and new downloads.
rm -rf libgenli_db

mkdir libgenli_db
cd /temp-dir/libgenli_db

# for i in $(seq -w 1 5); do # retries
#     rclone copy :ftp:/upload/db/ /temp-dir/libgenli_db/ --ftp-host=ftp.libgen.lc --ftp-user=anonymous --ftp-pass=$(rclone obscure dummy) --size-only --progress --multi-thread-streams=1 --transfers=1
# done

for i in $(seq -w 1 48); do
    # Using curl here since it only accepts one connection from any IP anyway,
    # and this way we stay consistent with `libgenli_proxies_template.sh`.

    # Server doesn't support resuming??
    # curl -L -C - -O "https://libgen.li/dbdumps/libgen_new.part0${i}.rar"
    
    # Try bewteen these:
    # *.lc, *.li, *.gs, *.vg, *.pm
    curl -L -O "https://libgen.lc/dbdumps/libgen_new.part0${i}.rar" || curl -L -O "https://libgen.li/dbdumps/libgen_new.part0${i}.rar" || curl -L -O "https://libgen.gs/dbdumps/libgen_new.part0${i}.rar" || curl -L -O "https://libgen.vg/dbdumps/libgen_new.part0${i}.rar" || curl -L -O "https://libgen.pm/dbdumps/libgen_new.part0${i}.rar"
done


#for i in $(seq -w 6 47); do curl -L -O "https://libgen.lc/dbdumps/libgen_new.part0${i}.rar" || curl -L -O "https://libgen.li/dbdumps/libgen_new.part0${i}.rar" || curl -L -O "https://libgen.gs/dbdumps/libgen_new.part0${i}.rar" || curl -L -O "https://libgen.vg/dbdumps/libgen_new.part0${i}.rar" || curl -L -O "https://libgen.pm/dbdumps/libgen_new.part0${i}.rar"; done
