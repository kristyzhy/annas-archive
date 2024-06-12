#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--web /scripts/dump_elasticsearch.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# Dump scripts are idempotent, and can be rerun without losing too much work.

cd /exports

rm -rf /exports/elasticsearchaux
mkdir /exports/elasticsearchaux
# https://github.com/elasticsearch-dump/elasticsearch-dump/issues/651#issuecomment-564545317
NODE_OPTIONS="--max-old-space-size=16384" multielasticdump --input=${ELASTICSEARCHAUX_HOST:-http://elasticsearchaux:9201} --output=/exports/elasticsearchaux --match='aarecords.*' --parallel=32 --limit=10000 --fsCompress --includeType=data,mapping,analyzer,alias,settings,template
# WARNING: multielasticdump doesn't properly handle children getting out of memory errors.
# Check valid gzips as a workaround. Still somewhat fragile though!
zcat /exports/elasticsearchaux/*.json.gz | wc -l
