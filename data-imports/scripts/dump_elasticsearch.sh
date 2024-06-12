#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/dump_elasticsearch.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Dump scripts are idempotent, and can be rerun without losing too much work.

cd /exports

rm -rf /exports/elasticsearch
mkdir /exports/elasticsearch
# https://github.com/elasticsearch-dump/elasticsearch-dump/issues/651#issuecomment-564545317
export NODE_OPTIONS="--max-old-space-size=16384"
multielasticdump --input=${ELASTICSEARCH_HOST:-http://elasticsearch:9200} --output=/exports/elasticsearch --match='aarecords.*' --parallel=16 --limit=10000 --fsCompress --includeType=data,mapping,analyzer,alias,settings,template
# WARNING: multielasticdump doesn't properly handle children getting out of memory errors.
# Check valid gzips as a workaround. Still somewhat fragile though!
zcat /exports/elasticsearch/*.json.gz | wc -l
