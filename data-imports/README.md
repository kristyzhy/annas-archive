Importing the data has been mostly automated, but it's still advisable to run the individual scripts yourself. It can take several days to run everything, but we also support only updating part of the data.

Roughly the steps are:
- (optional) make a copy of the existing MySQL database, if you want to keep existing data.
- Download new data.
- Import data into MySQL.
- Generate derived data (mostly ElasticSearch).
- Swap out the new data in production.

```bash
[ -e ../../aa-data-import--allthethings-mysql-data ] && (echo '../../aa-data-import--allthethings-mysql-data already exists; aborting'; exit 1)
[ -e ../../aa-data-import--allthethings-elastic-data ] && (echo '../../aa-data-import--allthethings-elastic-data already exists; aborting'; exit 1)
# If you wish to download everything from scratch, you should make sure the aa-data-import--temp-dir dir is deleted.
# [ -e ../../aa-data-import--temp-dir ] && (echo '../../aa-data-import--temp-dir already exists; aborting'; exit 1)

mkdir ../../aa-data-import--allthethings-elastic-data
chown 1000 ../../aa-data-import--allthethings-elastic-data

# Uncomment if you want to start off with the existing MySQL data, e.g. if you only want to run a subset of the scripts.
# sudo rsync -av --append ../../allthethings-mysql-data/ ../../aa-data-import--allthethings-mysql-data/

# You might need to adjust the size of ElasticSearch's heap size, by changing `ES_JAVA_OPTS` in `data-imports/docker-compose.yml`.
# If MariaDB wants too much RAM: comment out `key_buffer_size` in `data-imports/mariadb-conf/my.cnf`
docker compose up -d --no-deps --build

# It's a good idea here to look at the Docker logs:
# docker compose logs --tail=200 -f

# Download the data. You can skip any of these scripts if you have already downloaded the data and don't want to repeat it.
# You can also run these in parallel in multiple terminal windows.
# We recommend looking through each script in detail before running it.
docker exec -it aa-data-import--mariadb /scripts/download_libgenli.sh # Look at data-imports/scripts/download_libgenli_proxies_template.sh to speed up downloading.
# E.g.: docker exec -it aa-data-import--mariadb /scripts/download_libgenli_proxies.sh; docker exec -it aa-data-import--mariadb /scripts/download_libgenli.sh
docker exec -it aa-data-import--mariadb /scripts/download_libgenrs.sh
docker exec -it aa-data-import--mariadb /scripts/download_openlib.sh
docker exec -it aa-data-import--mariadb /scripts/download_pilimi_isbndb.sh
docker exec -it aa-data-import--mariadb /scripts/download_pilimi_zlib.sh
docker exec -it aa-data-import--mariadb /scripts/download_aa_various.sh

# Load the data.
docker exec -it aa-data-import--mariadb /scripts/load_libgenli.sh
docker exec -it aa-data-import--mariadb /scripts/load_libgenrs.sh
docker exec -it aa-data-import--mariadb /scripts/load_openlib.sh
docker exec -it aa-data-import--mariadb /scripts/load_pilimi_isbndb.sh
docker exec -it aa-data-import--mariadb /scripts/load_pilimi_zlib.sh
docker exec -it aa-data-import--mariadb /scripts/load_aa_various.sh

# If you ever want to see what is going on in MySQL as these scripts run:
# docker exec -it aa-data-import--mariadb mariadb -u root -ppassword allthethings --show-warnings -vv -e 'SHOW PROCESSLIST;'

# First sanity check to make sure the right tables exist.
docker exec -it aa-data-import--mariadb /scripts/check_after_imports.sh

# Sanity check to make sure the tables are filled.
docker exec -it aa-data-import--mariadb mariadb -u root -ppassword allthethings --show-warnings -vv -e 'SELECT table_name, ROUND(((data_length + index_length) / 1024 / 1024), 2) AS "Size (MB)" FROM information_schema.TABLES WHERE table_schema = "allthethings" ORDER BY table_name;'

# Calculate derived data:
docker exec -it aa-data-import--web flask cli mysql_build_computed_all_md5s && docker exec -it aa-data-import--web flask cli elastic_reset_md5_dicts && docker exec -it aa-data-import--web flask cli elastic_build_md5_dicts

# Make sure to fully stop the databases, so we can move some files around.
docker compose down

# Quickly swap out the new MySQL+ES folders in a production setting.
# cd ..
# docker compose stop mariadb elasticsearch kibana web
# export NOW=$(date +"%Y_%m_%d_%H_%M")
# mv ../allthethings-mysql-data ../allthethings-mysql-data--backup-$NOW
# mv ../allthethings-elastic-data ../allthethings-elastic-data--backup-$NOW
# rsync -a --progress ../aa-data-import--allthethings-mysql-data/ ../allthethings-mysql-data
# rsync -a --progress ../aa-data-import--allthethings-elastic-data/ ../allthethings-elastic-data
# docker compose up -d --no-deps --build; docker compose stop web
# docker compose logs --tail 20 --follow
# docker compose start web

# To restore the backup:
# docker compose stop mariadb elasticsearch kibana
# mv ../allthethings-mysql-data ../allthethings-mysql-data--didnt-work
# mv ../allthethings-elastic-data ../allthethings-elastic-data--didnt-work
# mv ../allthethings-mysql-data--backup-$NOW ../allthethings-mysql-data
# mv ../allthethings-elastic-data--backup-$NOW ../allthethings-elastic-data
# docker compose up -d --no-deps --build
# docker compose logs --tail 20 --follow
```
