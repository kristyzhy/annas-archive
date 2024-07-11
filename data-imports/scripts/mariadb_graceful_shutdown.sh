#!/bin/bash

set -Eeuxo pipefail

mariadb -h aa-data-import--mariadb -u root -ppassword allthethings --show-warnings -vv -e 'SHUTDOWN'

sleep 120
