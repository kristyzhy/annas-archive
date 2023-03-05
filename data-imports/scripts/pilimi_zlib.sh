#!/bin/bash

set -Eeuxo pipefail

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/pilimi_zlib.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.

# aria2c torrent downloading is sadly not idempotent, and crashes when the torrent is already downloaded;
# so just comment out those lines if you need to rerun.

cd /temp-dir

# pilimi-zlib2-index-2022-08-24-fixed.torrent
aria2c --seed-time=0 'magnet:?xt=urn:btih:29d0c9de39f94b93b207e2c397490baadb74cd49&dn=pilimi-zlib2-index-2022-08-24-fixed.sql.gz&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&tr=http%3A%2F%2F95.107.48.115%3A80%2Fannounce&tr=http%3A%2F%2Fopen.acgnxtracker.com%3A80%2Fannounce&tr=http%3A%2F%2Ft.acg.rip%3A6699%2Fannounce&tr=http%3A%2F%2Ft.nyaatracker.com%3A80%2Fannounce&tr=http%3A%2F%2Ftracker.bt4g.com%3A2095%2Fannounce&tr=http%3A%2F%2Ftracker.files.fm%3A6969%2Fannounce&tr=http%3A%2F%2Fvps02.net.orel.ru%3A80%2Fannounce&tr=https%3A%2F%2F1337.abcvg.info%3A443%2Fannounce&tr=https%3A%2F%2Ftracker.nanoha.org%3A443%2Fannounce&tr=https%3A%2F%2Ftracker.sloppyta.co%3A443%2Fannounce&tr=udp%3A%2F%2F208.83.20.20%3A6969%2Fannounce&tr=udp%3A%2F%2F37.235.174.46%3A2710%2Fannounce&tr=http%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969%2Fannounce&tr=udp%3A%2F%2Fipv4.tracker.harry.lu%3A80%2Fannounce&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.filemail.com%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.moeking.me%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.tiny-vps.com%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2F75.127.14.224%3A2710%2Fannounce&tr=udp%3A%2F%2Fp4p.arenabg.com%3A1337%2Fannounce&tr=udp%3A%2F%2Fretracker.lanta-net.ru%3A2710%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.swateam.org.uk%3A2710%2Fannounce&tr=udp%3A%2F%2Ftracker.openbittorrent.com%3A6969'

pv pilimi-zlib2-index-2022-08-24-fixed.sql.gz | zcat | sed -e 's/^) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;$/) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;/g' | mariadb -u root -ppassword allthethings

mariadb -u root -ppassword allthethings --show-warnings -vv < /scripts/helpers/pilimi_zlib_final.sql
