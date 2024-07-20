# Anna’s Archive

Welcome to the Code repository for Anna's Archive, the comprehensive search engine for books, papers, comics, magazines, and more. This repository contains all the code necessary to run Anna’s Archive locally or deploy it to a production environment.

## Quick Start

To get Anna's Archive running locally:

1. **Initial Setup**

  In a terminal, clone the repository and set up your environment:
  ```bash
  git clone https://software.annas-archive.se/AnnaArchivist/annas-archive.git
  cd annas-archive
  cp .env.dev .env
  ```

2. **Build and Start the Application**

  Use Docker Compose to build and start the application:
  ```bash
  docker compose up --build
  ```
  Wait a few minutes for the setup to complete. It's normal to see some errors from the `web` container during the first setup.

3. **Database Initialization**

  In a new terminal window, initialize the database:
  ```bash
  ./run flask cli dbreset
  ```

4. **Restart the Application**

  Once the database is initialized, restart the Docker Compose process, by killing it (CTRL+C) and running:
  ```bash
  docker compose up --build
  ```

5. **Visit Anna's Archive**

   Open your browser and visit [http://localtest.me:8000](http://localtest.me:8000) to access the application.

## Common Issues and Solutions

- **ElasticSearch Permission Issues**

  If you encounter permission errors related to ElasticSearch data, modify the permissions of the ElasticSearch data directories:
  ```bash
  sudo chmod 0777 -R ../allthethings-elastic-data/ ../allthethings-elasticsearchaux-data/
  ```
  This command grants read, write, and execute permissions to all users for the specified directories, addressing potential startup issues with Elasticsearch.

- **MariaDB Memory Consumption**

  If MariaDB is consuming too much RAM, you might need to adjust its configuration. To do so, comment out the `key_buffer_size` option in `mariadb-conf/my.cnf`.

- **ElasticSearch Heap Size**

  Adjust the size of the ElasticSearch heap by modifying `ES_JAVA_OPTS` in `docker-compose.yml` according to your system's available memory.

## Architecture Overview

Anna’s Archive is built on a scalable architecture designed to support a large volume of data and users:

- **Web Servers:** One or more servers handling web requests, with heavy caching (e.g., Cloudflare) to optimize performance.
- **Database Servers:**
  - Critical for basic operation:
    - 2 ElasticSearch servers "elasticsearch" (main) and "elasticsearchaux" (journal papers, digital lending, and metadata). Split out into two so the full index of "elasticsearch" can be easily forced into memory with `vmtouch` for performance.
  - Currently required for basic operation, but in the future only necessary for generating the search index:
    - MariaDB for read-only data with MyISAM tables ("mariadb")
    - Static read-only files in AAC (Anna’s Archive Container) format, with accompanying index tables (with byte offsets) in MariaDB.
  - Currently required for basic operation, but in the future only necessary for user accounts and other persistence:
    - A separate MariaDB instance for read/write operations ("mariapersist").
    - A persistent data replica ("mariapersistreplica") for backups and redundancy.
- **Caching and Proxy Servers:** Recommended setup includes proxy servers (e.g., nginx) in front of the web servers for added control and security (DMCA notices).

In our setup, the web and database servers are duplicated multiple times on different servers, with the exception of "mariapersist" which is shared between all servers. The ElasticSearch main server (or both servers) can also be run separately on optimized hardware, since search speed is usually a bottleneck.

## Importing Data

To import all necessary data into Anna’s Archive, refer to the detailed instructions in [data-imports/README.md](data-imports/README.md).

## Translations

We check in .po _and_ .mo files. The process is as follows:
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

Try it out by going to `http://es.localtest.me:8000`

## Production deployment

Be sure to exclude a bunch of stuff, most importantly `docker-compose.override.yml` which is just for local use. E.g.:

```bash
rsync --exclude=.git --exclude=.env --exclude=.DS_Store --exclude=docker-compose.override.yml -av --delete ..
```

To set up mariapersistreplica and mariabackup, check out `mariapersistreplica-conf/README.txt`.

## Scraping

Scraping of new datasets is not in scope for this repo, but we nonetheless have a guide here: [SCRAPING.md](SCRAPING.md).
    
## Contributing

To report bugs or suggest new ideas, please file an ["issue"](https://software.annas-archive.se/AnnaArchivist/annas-archive/-/issues).

To contribute code, also file an [issue](https://software.annas-archive.se/AnnaArchivist/annas-archive/-/issues), and include your `git diff` inline (you can use \`\`\`diff to get some syntax highlighting on the diff). Merge requests are currently disabled for security purposes — if you make consistently useful contributions you might get access.

For larger projects, please contact Anna first on [Reddit](https://www.reddit.com/r/Annas_Archive/).
## License


Released in the public domain under the terms of [CC0](./LICENSE). By contributing you agree to license your code under the same license.

