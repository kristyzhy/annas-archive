#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/load_aac_zlib3_files.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Load scripts are idempotent, and can be rerun without losing too much work.

PYTHONIOENCODING=UTF8:ignore python3 /scripts/helpers/load_aac.py /temp-dir/aac_zlib3_files/annas_archive_meta__aacid__zlib3_files*
