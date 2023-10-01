# Anna’s Archive

This is the code hosts annas-archive.org, the search engine for books, papers, comics, magazines, and more.

## Running locally

In one terminal window, run:

```bash
cp .env.dev .env
docker compose up --build
```

It might take a while for everything to settle, so wait a minute until there are no more logs changing. The errors that you get from the `web` container are normal during this first setup.

When everything is settled, in another terminal window, run:

```bash
./run flask cli dbreset
```

Now restart the `docker compose up` from above, and things should work.

Common issues:
* Funky permissions on ElasticSearch data: `sudo chmod 0777 -R ../allthethings-elastic-data/ ../allthethings-elasticsearchaux-data/`
* MariaDB wants too much RAM: comment out `key_buffer_size` in `mariadb-conf/my.cnf`
* Note that the example data is pretty funky / weird because of some joined tables not lining up nicely when only exporting a small number of records.
* You might need to adjust the size of ElasticSearch's heap size, by changing `ES_JAVA_OPTS` in `docker-compose.yml`.

Notes:
* This repo is based on [docker-flask-example](https://github.com/nickjj/docker-flask-example).

## Architecture

This is roughly the structure:
* 1+ web servers
* Heavy caching in front of web servers (e.g. Cloudflare)
* 1+ read-only MariaDB db with MyISAM tables of data ("mariadb")
* 1 read/write MariaDB db for persistent data ("mariapersist")
* 1 persistent data replica ("mariapersistreplica") set up with backups ("mariabackup").

Practically, you also want proxy servers in front of the web servers, so you can control who gets DMCA notices.

## Importing all data

See [data-imports/README.md](data-imports/README.md).

## Translations

These are a work in progress. For now, we check in .po _and_ .mo files. The process is as follows:
```sh
# After updating any `gettext` calls:
pybabel extract --omit-header -F babel.cfg -o messages.pot .
pybabel update --omit-header -i messages.pot -d allthethings/translations --no-fuzzy-matching

# After changing any translations:
pybabel compile -f -d allthethings/translations

# All of the above:
./update-translations.sh

# Only for english:
./update-translations-en.sh

# To add a new translation file:
pybabel init -i messages.pot -d allthethings/translations -l es
```

Try it out by going to `http://es.localhost` (on some systems you might have to add this to your `/etc/hosts` file).

## Production deployment

Be sure to exclude a bunch of stuff, most importantly `docker-compose.override.yml` which is just for local use. E.g.:

```bash
rsync --exclude=.git --exclude=.env --exclude=.DS_Store --exclude=docker-compose.override.yml -av --delete ..
```

To set up mariapersistreplica and mariabackup, check out `mariapersistreplica-conf/README.txt`.

## Contribute

To report bugs or suggest new ideas, please file an ["issue"](https://annas-software.org/AnnaArchivist/annas-archive/-/issues).

To contribute code, also file an [issue](https://annas-software.org/AnnaArchivist/annas-archive/-/issues), and include your `git diff` inline (you can use \`\`\`diff to get some syntax highlighting on the diff). Merge requests are currently disabled for security purposes — if you make consistently useful contributions you might get access.

For larger projects, please contact Anna first on [Twitter](https://twitter.com/AnnaArchivist) or [Reddit](https://www.reddit.com/r/Annas_Archive/).

## License

Released in the public domain under the terms of [CC0](./LICENSE). By contributing you agree to license your code under the same license.
