import os
import json
import orjson
import re
import zlib
import isbnlib
import httpx
import functools
import collections
import barcode
import io
import langcodes
import tqdm
import concurrent
import threading
import yappi
import multiprocessing
import langdetect
import gc
import random
import slugify
import elasticsearch.helpers
import time
import pathlib
import ftlangdetect
import traceback
import flask_mail
import click
import pymysql.cursors
import more_itertools
import indexed_zstd
import hashlib

import allthethings.utils

from flask import Blueprint, __version__, render_template, make_response, redirect, request
from allthethings.extensions import engine, mariadb_url, mariadb_url_no_timeout, es, es_aux, Reflected, mail, mariapersist_url
from sqlalchemy import select, func, text, create_engine
from sqlalchemy.dialects.mysql import match
from sqlalchemy.orm import Session
from pymysql.constants import CLIENT
from config.settings import SLOW_DATA_IMPORTS

from allthethings.page.views import get_aarecords_mysql

cli = Blueprint("cli", __name__, template_folder="templates")


#################################################################################################
# ./run flask cli dbreset
@cli.cli.command('dbreset')
def dbreset():
    print("Erasing entire database (2 MariaDB databases servers + 1 ElasticSearch)! Did you double-check that any production/large databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)

    mariapersist_reset_internal()
    nonpersistent_dbreset_internal()
    done_message()

def done_message():
    print("Done!")
    print("Search for example for 'Rhythms of the brain': http://localtest.me:8000/search?q=Rhythms+of+the+brain")
    print("To test SciDB: http://localtest.me:8000/scidb/10.5822/978-1-61091-843-5_15")
    print("See mariadb_dump.sql for various other records you can look at.")

#################################################################################################
# ./run flask cli nonpersistent_dbreset
@cli.cli.command('nonpersistent_dbreset')
def nonpersistent_dbreset():
    print("Erasing nonpersistent databases (1 MariaDB databases servers + 1 ElasticSearch)! Did you double-check that any production/large databases are offline/inaccessible from here?")
    nonpersistent_dbreset_internal()
    done_message()


def nonpersistent_dbreset_internal():
    # Per https://stackoverflow.com/a/4060259
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    engine_multi = create_engine(mariadb_url_no_timeout, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    cursor = engine_multi.raw_connection().cursor()

    # Generated with `docker compose exec mariadb mysqldump -u allthethings -ppassword --opt --where="1 limit 100" --skip-comments --ignore-table=computed_all_md5s allthethings > mariadb_dump.sql`
    mariadb_dump = pathlib.Path(os.path.join(__location__, 'mariadb_dump.sql')).read_text()
    for sql in mariadb_dump.split('# DELIMITER'):
        cursor.execute(sql)
    cursor.close()

    mysql_build_computed_all_md5s_internal()

    time.sleep(1)
    Reflected.prepare(engine_multi)
    elastic_reset_aarecords_internal()
    elastic_build_aarecords_all_internal()

def query_yield_batches(conn, qry, pk_attr, maxrq):
    """specialized windowed query generator (using LIMIT/OFFSET)

    This recipe is to select through a large number of rows thats too
    large to fetch at once. The technique depends on the primary key
    of the FROM clause being an integer value, and selects items
    using LIMIT."""

    firstid = None
    while True:
        q = qry
        if firstid is not None:
            q = qry.where(pk_attr > firstid)
        batch = conn.execute(q.order_by(pk_attr).limit(maxrq)).all()
        if len(batch) == 0:
            break
        yield batch
        firstid = batch[-1][0]


#################################################################################################
# Rebuild "computed_all_md5s" table in MySQL. At the time of writing, this isn't
# used in the app, but it is used for `./run flask cli elastic_build_aarecords_main`.
# ./run flask cli mysql_build_computed_all_md5s
#
# To dump computed_all_md5s to txt: 
#   docker exec mariadb mariadb -uallthethings -ppassword allthethings --skip-column-names -e 'SELECT LOWER(HEX(md5)) from computed_all_md5s;' > md5.txt
@cli.cli.command('mysql_build_computed_all_md5s')
def mysql_build_computed_all_md5s():
    print("Erasing entire MySQL 'computed_all_md5s' table! Did you double-check that any production/large databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)

    mysql_build_computed_all_md5s_internal()

def mysql_build_computed_all_md5s_internal():
    engine_multi = create_engine(mariadb_url_no_timeout, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    cursor = engine_multi.raw_connection().cursor()
    print("Removing table computed_all_md5s (if exists)")
    cursor.execute('DROP TABLE IF EXISTS computed_all_md5s')
    print("Load indexes of libgenli_files")
    cursor.execute('LOAD INDEX INTO CACHE libgenli_files')
    print("Creating table computed_all_md5s and load with libgenli_files")
    cursor.execute('CREATE TABLE computed_all_md5s (md5 BINARY(16) NOT NULL, PRIMARY KEY (md5)) ENGINE=MyISAM ROW_FORMAT=FIXED SELECT UNHEX(md5) AS md5 FROM libgenli_files WHERE md5 IS NOT NULL')
    print("Load indexes of computed_all_md5s")
    cursor.execute('LOAD INDEX INTO CACHE computed_all_md5s')
    print("Load indexes of zlib_book")
    cursor.execute('LOAD INDEX INTO CACHE zlib_book')
    print("Inserting from 'zlib_book' (md5_reported)")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5) SELECT UNHEX(md5_reported) FROM zlib_book WHERE md5_reported != "" AND md5_reported IS NOT NULL')
    print("Inserting from 'zlib_book' (md5)")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5) SELECT UNHEX(md5) FROM zlib_book WHERE zlib_book.md5 != "" AND md5 IS NOT NULL')
    print("Load indexes of libgenrs_fiction")
    cursor.execute('LOAD INDEX INTO CACHE libgenrs_fiction')
    print("Inserting from 'libgenrs_fiction'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5) SELECT UNHEX(md5) FROM libgenrs_fiction WHERE md5 IS NOT NULL')
    print("Load indexes of libgenrs_updated")
    cursor.execute('LOAD INDEX INTO CACHE libgenrs_updated')
    print("Inserting from 'libgenrs_updated'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5) SELECT UNHEX(md5) FROM libgenrs_updated WHERE md5 IS NOT NULL')
    print("Load indexes of aa_lgli_comics_2022_08_files")
    cursor.execute('LOAD INDEX INTO CACHE aa_lgli_comics_2022_08_files')
    print("Inserting from 'aa_lgli_comics_2022_08_files'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5) SELECT UNHEX(md5) FROM aa_lgli_comics_2022_08_files')
    print("Load indexes of aa_ia_2023_06_files and aa_ia_2023_06_metadata")
    cursor.execute('LOAD INDEX INTO CACHE aa_ia_2023_06_files, aa_ia_2023_06_metadata')
    print("Inserting from 'aa_ia_2023_06_files'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5) SELECT UNHEX(md5) FROM aa_ia_2023_06_metadata USE INDEX (libgen_md5) JOIN aa_ia_2023_06_files USING (ia_id) WHERE aa_ia_2023_06_metadata.libgen_md5 IS NULL')
    print("Load indexes of annas_archive_meta__aacid__ia2_acsmpdf_files and aa_ia_2023_06_metadata")
    cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__ia2_acsmpdf_files, aa_ia_2023_06_metadata')
    print("Inserting from 'annas_archive_meta__aacid__ia2_acsmpdf_files'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5) SELECT UNHEX(md5) FROM aa_ia_2023_06_metadata USE INDEX (libgen_md5) JOIN annas_archive_meta__aacid__ia2_acsmpdf_files ON (aa_ia_2023_06_metadata.ia_id = annas_archive_meta__aacid__ia2_acsmpdf_files.primary_id) WHERE aa_ia_2023_06_metadata.libgen_md5 IS NULL')
    print("Load indexes of annas_archive_meta__aacid__zlib3_records")
    cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__zlib3_records')
    print("Inserting from 'annas_archive_meta__aacid__zlib3_records'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5) SELECT UNHEX(md5) FROM annas_archive_meta__aacid__zlib3_records WHERE md5 IS NOT NULL')
    print("Load indexes of annas_archive_meta__aacid__zlib3_files")
    cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__zlib3_files')
    print("Inserting from 'annas_archive_meta__aacid__zlib3_files'")
    cursor.execute('INSERT IGNORE INTO computed_all_md5s (md5) SELECT UNHEX(md5) FROM annas_archive_meta__aacid__zlib3_files WHERE md5 IS NOT NULL')
    cursor.close()
    # engine_multi = create_engine(mariadb_url_no_timeout, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    # cursor = engine_multi.raw_connection().cursor()
    # print("Removing table computed_all_md5s (if exists)")
    # cursor.execute('DROP TABLE IF EXISTS computed_all_md5s')
    # print("Load indexes of libgenli_files")
    # cursor.execute('LOAD INDEX INTO CACHE libgenli_files')
    # # print("Creating table computed_all_md5s and load with libgenli_files")
    # # cursor.execute('CREATE TABLE computed_all_md5s (md5 CHAR(32) NOT NULL, PRIMARY KEY (md5)) ENGINE=MyISAM DEFAULT CHARSET=ascii COLLATE ascii_bin ROW_FORMAT=FIXED SELECT md5 FROM libgenli_files')

    # # print("Load indexes of computed_all_md5s")
    # # cursor.execute('LOAD INDEX INTO CACHE computed_all_md5s')
    # print("Load indexes of zlib_book")
    # cursor.execute('LOAD INDEX INTO CACHE zlib_book')
    # # print("Inserting from 'zlib_book' (md5_reported)")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT md5_reported FROM zlib_book LEFT JOIN computed_all_md5s ON (computed_all_md5s.md5 = zlib_book.md5_reported) WHERE md5_reported != "" AND computed_all_md5s.md5 IS NULL')
    # # print("Inserting from 'zlib_book' (md5)")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT md5 FROM zlib_book LEFT JOIN computed_all_md5s USING (md5) WHERE zlib_book.md5 != "" AND computed_all_md5s.md5 IS NULL')
    # print("Load indexes of libgenrs_fiction")
    # cursor.execute('LOAD INDEX INTO CACHE libgenrs_fiction')
    # # print("Inserting from 'libgenrs_fiction'")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT LOWER(libgenrs_fiction.MD5) FROM libgenrs_fiction LEFT JOIN computed_all_md5s ON (computed_all_md5s.md5 = LOWER(libgenrs_fiction.MD5)) WHERE computed_all_md5s.md5 IS NULL')
    # print("Load indexes of libgenrs_updated")
    # cursor.execute('LOAD INDEX INTO CACHE libgenrs_updated')
    # # print("Inserting from 'libgenrs_updated'")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT MD5 FROM libgenrs_updated LEFT JOIN computed_all_md5s USING (md5) WHERE computed_all_md5s.md5 IS NULL')
    # print("Load indexes of aa_ia_2023_06_files")
    # cursor.execute('LOAD INDEX INTO CACHE aa_ia_2023_06_files')
    # # print("Inserting from 'aa_ia_2023_06_files'")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT MD5 FROM aa_ia_2023_06_files LEFT JOIN aa_ia_2023_06_metadata USING (ia_id) LEFT JOIN computed_all_md5s USING (md5) WHERE aa_ia_2023_06_metadata.libgen_md5 IS NULL AND computed_all_md5s.md5 IS NULL')
    # print("Load indexes of annas_archive_meta__aacid__zlib3_records")
    # cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__zlib3_records')
    # # print("Inserting from 'annas_archive_meta__aacid__zlib3_records'")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT md5 FROM annas_archive_meta__aacid__zlib3_records LEFT JOIN computed_all_md5s USING (md5) WHERE md5 IS NOT NULL AND computed_all_md5s.md5 IS NULL')
    # print("Load indexes of annas_archive_meta__aacid__zlib3_files")
    # cursor.execute('LOAD INDEX INTO CACHE annas_archive_meta__aacid__zlib3_files')
    # # print("Inserting from 'annas_archive_meta__aacid__zlib3_files'")
    # # cursor.execute('INSERT INTO computed_all_md5s SELECT md5 FROM annas_archive_meta__aacid__zlib3_files LEFT JOIN computed_all_md5s USING (md5) WHERE md5 IS NOT NULL AND computed_all_md5s.md5 IS NULL')
    # print("Creating table computed_all_md5s")
    # cursor.execute('CREATE TABLE computed_all_md5s (md5 CHAR(32) NOT NULL, PRIMARY KEY (md5)) ENGINE=MyISAM DEFAULT CHARSET=ascii COLLATE ascii_bin ROW_FORMAT=FIXED IGNORE SELECT DISTINCT md5 AS md5 FROM libgenli_files UNION DISTINCT (SELECT DISTINCT md5_reported AS md5 FROM zlib_book WHERE md5_reported != "") UNION DISTINCT (SELECT DISTINCT md5 AS md5 FROM zlib_book WHERE md5 != "") UNION DISTINCT (SELECT DISTINCT LOWER(libgenrs_fiction.MD5) AS md5 FROM libgenrs_fiction) UNION DISTINCT (SELECT DISTINCT MD5 AS md5 FROM libgenrs_updated) UNION DISTINCT (SELECT DISTINCT md5 AS md5 FROM aa_ia_2023_06_files LEFT JOIN aa_ia_2023_06_metadata USING (ia_id) WHERE aa_ia_2023_06_metadata.libgen_md5 IS NULL) UNION DISTINCT (SELECT DISTINCT md5 AS md5 FROM annas_archive_meta__aacid__zlib3_records WHERE md5 IS NOT NULL) UNION DISTINCT (SELECT DISTINCT md5 AS md5 FROM annas_archive_meta__aacid__zlib3_files WHERE md5 IS NOT NULL)')
    # cursor.close()


#################################################################################################
# Recreate "aarecords" index in ElasticSearch, without filling it with data yet.
# (That is done with `./run flask cli elastic_build_aarecords_*`)
# ./run flask cli elastic_reset_aarecords
@cli.cli.command('elastic_reset_aarecords')
def elastic_reset_aarecords():
    print("Erasing entire ElasticSearch 'aarecords' index! Did you double-check that any production/large databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)

    elastic_reset_aarecords_internal()

def elastic_reset_aarecords_internal():
    print("Deleting ES indices")
    es.options(ignore_status=[400,404]).indices.delete(index='aarecords')
    es_aux.options(ignore_status=[400,404]).indices.delete(index='aarecords_digital_lending')
    es_aux.options(ignore_status=[400,404]).indices.delete(index='aarecords_metadata')
    body = {
        "mappings": {
            "dynamic": False,
            "properties": {
                "search_only_fields": {
                    "properties": {
                        "search_filesize": { "type": "long", "index": False, "doc_values": True },
                        "search_year": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                        "search_extension": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                        "search_content_type": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                        "search_most_likely_language_code": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                        "search_isbn13": { "type": "keyword", "index": True, "doc_values": True },
                        "search_doi": { "type": "keyword", "index": True, "doc_values": True },
                        "search_text": { "type": "text", "index": True, "analyzer": "icu_analyzer" },
                        "search_score_base": { "type": "float", "index": False, "doc_values": True },
                        "search_score_base_rank": { "type": "rank_feature" },
                        "search_access_types": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                        "search_record_sources": { "type": "keyword", "index": True, "doc_values": True, "eager_global_ordinals": True },
                    },
                },
            },
        },
        "settings": {
            "index.number_of_replicas": 0,
            "index.search.slowlog.threshold.query.warn": "4s",
            "index.store.preload": ["nvd", "dvd", "tim", "doc", "dim"],
            "index.sort.field": "search_only_fields.search_score_base",
            "index.sort.order": "desc",
            "index.codec": "best_compression",
        },
    }
    print("Creating ES indices")
    es.indices.create(index='aarecords', body=body)
    es_aux.indices.create(index='aarecords_digital_lending', body=body)
    es_aux.indices.create(index='aarecords_metadata', body=body)



elastic_build_aarecords_job_app = None
def elastic_build_aarecords_job(aarecord_ids):
    global elastic_build_aarecords_job_app
    if elastic_build_aarecords_job_app is None:
        from allthethings.app import create_app
        elastic_build_aarecords_job_app = create_app()
    with elastic_build_aarecords_job_app.app_context():
        try:
            aarecord_ids = list(aarecord_ids)
            # print(f"[{os.getpid()}] elastic_build_aarecords_job start {len(aarecord_ids)}")
            with Session(engine) as session:
                operations_by_es_handle = collections.defaultdict(list)
                dois = []
                isbn13_oclc_insert_data = []
                session.connection().connection.ping(reconnect=True)
                cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
                cursor.execute('CREATE TABLE IF NOT EXISTS aarecords_all (hashed_aarecord_id BINARY(16) NOT NULL, aarecord_id VARCHAR(1000) NOT NULL, md5 BINARY(16) NULL, json JSON NOT NULL, PRIMARY KEY (hashed_aarecord_id), UNIQUE INDEX (aarecord_id), UNIQUE INDEX (md5)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')
                # print(f"[{os.getpid()}] elastic_build_aarecords_job set up aa_records_all")
                aarecords = get_aarecords_mysql(session, aarecord_ids)
                # print(f"[{os.getpid()}] elastic_build_aarecords_job got aarecords {len(aarecords)}")
                aarecords_all_insert_data = []
                for aarecord in aarecords:
                    aarecords_all_insert_data.append({
                        'hashed_aarecord_id': hashlib.md5(aarecord['id'].encode()).digest(),
                        'aarecord_id': aarecord['id'],
                        'md5': bytes.fromhex(aarecord['id'].split(':', 1)[1]) if aarecord['id'].startswith('md5:') else None,
                        'json': orjson.dumps(aarecord),
                    })
                    for index in aarecord['indexes']:
                        operations_by_es_handle[allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[index]].append({ **aarecord, '_op_type': 'index', '_index': index, '_id': aarecord['id'] })
                    for doi in (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []):
                        dois.append(doi)
                    if aarecord['id'].startswith('oclc:'):
                        for isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
                            isbn13_oclc_insert_data.append({ "isbn13": isbn13, "oclc_id": int(aarecord['id'].split(':', 1)[1]) })
                # print(f"[{os.getpid()}] elastic_build_aarecords_job finished for loop")

                if (aarecord_ids[0].startswith('md5:')) and (len(dois) > 0):
                    dois = list(set(dois))
                    session.connection().connection.ping(reconnect=True)
                    count = cursor.execute(f'DELETE FROM scihub_dois_without_matches WHERE doi IN %(dois)s', { "dois": dois })
                    cursor.execute('COMMIT')
                    # print(f'Deleted {count} DOIs')

                if len(isbn13_oclc_insert_data) > 0:
                    session.connection().connection.ping(reconnect=True)
                    cursor.executemany(f"INSERT INTO isbn13_oclc (isbn13, oclc_id) VALUES (%(isbn13)s, %(oclc_id)s) ON DUPLICATE KEY UPDATE isbn13=isbn13", isbn13_oclc_insert_data)
                    cursor.execute('COMMIT')

                # print(f"[{os.getpid()}] elastic_build_aarecords_job processed incidental inserts")
                    
                try:
                    for es_handle, operations in operations_by_es_handle.items():
                        elasticsearch.helpers.bulk(es_handle, operations, request_timeout=30)
                except Exception as err:
                    if hasattr(err, 'errors'):
                        print(err.errors)
                    print(repr(err))
                    print("Got the above error; retrying..")
                    try:
                        for es_handle, operations in operations_by_es_handle.items():
                            elasticsearch.helpers.bulk(es_handle, operations, request_timeout=30)
                    except Exception as err:
                        if hasattr(err, 'errors'):
                            print(err.errors)
                        print(repr(err))
                        print("Got the above error; retrying one more time..")
                        for es_handle, operations in operations_by_es_handle.items():
                            elasticsearch.helpers.bulk(es_handle, operations, request_timeout=30)

                # print(f"[{os.getpid()}] elastic_build_aarecords_job inserted into ES")

                session.connection().connection.ping(reconnect=True)
                cursor.executemany(f'INSERT IGNORE INTO aarecords_all (hashed_aarecord_id, aarecord_id, md5, json) VALUES (%(hashed_aarecord_id)s, %(aarecord_id)s, %(md5)s, %(json)s) ON DUPLICATE KEY UPDATE json=json', aarecords_all_insert_data)
                cursor.execute('COMMIT')
                cursor.close()

                # print(f"[{os.getpid()}] elastic_build_aarecords_job inserted into aarecords_all")
                # print(f"[{os.getpid()}] Processed {len(aarecords)} md5s")

        except Exception as err:
            print(repr(err))
            traceback.print_tb(err.__traceback__)
            raise err

def elastic_build_aarecords_job_oclc(fields):
    fields = list(fields)
    allthethings.utils.set_worldcat_line_cache(fields)
    elastic_build_aarecords_job([f"oclc:{field[0]}" for field in fields])

THREADS = 50
CHUNK_SIZE = 30
BATCH_SIZE = 30000

# Locally
if SLOW_DATA_IMPORTS:
    THREADS = 1
    CHUNK_SIZE = 10
    BATCH_SIZE = 1000

# Uncomment to do them one by one
# THREADS = 1
# CHUNK_SIZE = 1
# BATCH_SIZE = 1

#################################################################################################
# ./run flask cli elastic_build_aarecords_all
@cli.cli.command('elastic_build_aarecords_all')
def elastic_build_aarecords_all():
    elastic_build_aarecords_all_internal()

def elastic_build_aarecords_all_internal():
    elastic_build_aarecords_ia_internal()
    elastic_build_aarecords_isbndb_internal()
    elastic_build_aarecords_ol_internal()
    elastic_build_aarecords_oclc_internal()
    elastic_build_aarecords_main_internal()


#################################################################################################
# ./run flask cli elastic_build_aarecords_ia
@cli.cli.command('elastic_build_aarecords_ia')
def elastic_build_aarecords_ia():
    elastic_build_aarecords_ia_internal()

def elastic_build_aarecords_ia_internal():
    print("Do a dummy detect of language so that we're sure the model is downloaded")
    ftlangdetect.detect('dummy')

    before_first_ia_id = ''

    with engine.connect() as connection:
        print("Processing from aa_ia_2023_06_metadata")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(ia_id) AS count FROM aa_ia_2023_06_metadata LEFT JOIN aa_ia_2023_06_files USING (ia_id) LEFT JOIN annas_archive_meta__aacid__ia2_acsmpdf_files ON (aa_ia_2023_06_metadata.ia_id = annas_archive_meta__aacid__ia2_acsmpdf_files.primary_id) WHERE aa_ia_2023_06_metadata.ia_id > %(from)s AND aa_ia_2023_06_files.md5 IS NULL AND annas_archive_meta__aacid__ia2_acsmpdf_files.md5 IS NULL AND aa_ia_2023_06_metadata.libgen_md5 IS NULL ORDER BY ia_id LIMIT 1', { "from": before_first_ia_id })
        total = list(cursor.fetchall())[0]['count']
        current_ia_id = before_first_ia_id
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            while True:
                connection.connection.ping(reconnect=True)
                cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                cursor.execute('SELECT ia_id FROM aa_ia_2023_06_metadata LEFT JOIN aa_ia_2023_06_files USING (ia_id) LEFT JOIN annas_archive_meta__aacid__ia2_acsmpdf_files ON (aa_ia_2023_06_metadata.ia_id = annas_archive_meta__aacid__ia2_acsmpdf_files.primary_id) WHERE aa_ia_2023_06_metadata.ia_id > %(from)s AND aa_ia_2023_06_files.md5 IS NULL AND annas_archive_meta__aacid__ia2_acsmpdf_files.md5 IS NULL AND aa_ia_2023_06_metadata.libgen_md5 IS NULL ORDER BY ia_id LIMIT %(limit)s', { "from": current_ia_id, "limit": BATCH_SIZE })
                batch = list(cursor.fetchmany(BATCH_SIZE))
                if len(batch) == 0:
                    break
                print(f"Processing {len(batch)} aarecords from aa_ia_2023_06_metadata ( starting ia_id: {batch[0]['ia_id']} , ia_id: {batch[-1]['ia_id']} )...")
                with multiprocessing.Pool(THREADS) as executor:
                    list(executor.map(elastic_build_aarecords_job, more_itertools.ichunked([f"ia:{item['ia_id']}" for item in batch], CHUNK_SIZE)))
                pbar.update(len(batch))
                current_ia_id = batch[-1]['ia_id']

        print(f"Done with IA!")


#################################################################################################
# ./run flask cli elastic_build_aarecords_isbndb
@cli.cli.command('elastic_build_aarecords_isbndb')
def elastic_build_aarecords_isbndb():
    elastic_build_aarecords_isbndb_internal()

def elastic_build_aarecords_isbndb_internal():
    print("Do a dummy detect of language so that we're sure the model is downloaded")
    ftlangdetect.detect('dummy')

    before_first_isbn13 = ''

    with engine.connect() as connection:
        print("Processing from isbndb_isbns")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(isbn13) AS count FROM isbndb_isbns WHERE isbn13 > %(from)s ORDER BY isbn13 LIMIT 1', { "from": before_first_isbn13 })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            current_isbn13 = before_first_isbn13
            while True:
                connection.connection.ping(reconnect=True)
                cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                cursor.execute('SELECT isbn13, isbn10 FROM isbndb_isbns WHERE isbn13 > %(from)s ORDER BY isbn13 LIMIT %(limit)s', { "from": current_isbn13, "limit": BATCH_SIZE })
                batch = list(cursor.fetchmany(BATCH_SIZE))
                if len(batch) == 0:
                    break
                print(f"Processing {len(batch)} aarecords from isbndb_isbns ( starting isbn13: {batch[0]['isbn13']} , ending isbn13: {batch[-1]['isbn13']} )...")
                isbn13s = set()
                for item in batch:
                    if item['isbn10'] != "0000000000":
                        isbn13s.add(f"isbn:{item['isbn13']}")
                        isbn13s.add(f"isbn:{isbnlib.ean13(item['isbn10'])}")
                with multiprocessing.Pool(THREADS) as executor:
                    list(executor.map(elastic_build_aarecords_job, more_itertools.ichunked(list(isbn13s), CHUNK_SIZE)))
                pbar.update(len(batch))
                current_isbn13 = batch[-1]['isbn13']
        print(f"Done with ISBNdb!")

#################################################################################################
# ./run flask cli elastic_build_aarecords_ol
@cli.cli.command('elastic_build_aarecords_ol')
def elastic_build_aarecords_ol():
    elastic_build_aarecords_ol_internal()

def elastic_build_aarecords_ol_internal():
    before_first_ol_key = ''
    # before_first_ol_key = '/books/OL5624024M'
    print("Do a dummy detect of language so that we're sure the model is downloaded")
    ftlangdetect.detect('dummy')

    with engine.connect() as connection:
        print("Processing from ol_base")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(ol_key) AS count FROM ol_base WHERE ol_key LIKE "/books/OL%%" AND ol_key > %(from)s ORDER BY ol_key LIMIT 1', { "from": before_first_ol_key })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            current_ol_key = before_first_ol_key
            while True:
                connection.connection.ping(reconnect=True)
                cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                cursor.execute('SELECT ol_key FROM ol_base WHERE ol_key LIKE "/books/OL%%" AND ol_key > %(from)s ORDER BY ol_key LIMIT %(limit)s', { "from": current_ol_key, "limit": BATCH_SIZE })
                batch = list(cursor.fetchall())
                if len(batch) == 0:
                    break
                print(f"Processing {len(batch)} aarecords from ol_base ( starting ol_key: {batch[0]['ol_key']} , ending ol_key: {batch[-1]['ol_key']} )...")
                with multiprocessing.Pool(THREADS) as executor:
                    list(executor.map(elastic_build_aarecords_job, more_itertools.ichunked([f"ol:{item['ol_key'].replace('/books/','')}" for item in batch if allthethings.utils.validate_ol_editions([item['ol_key'].replace('/books/','')])], CHUNK_SIZE)))
                pbar.update(len(batch))
                current_ol_key = batch[-1]['ol_key']
        print(f"Done with OpenLib!")

#################################################################################################
# ./run flask cli elastic_build_aarecords_oclc
@cli.cli.command('elastic_build_aarecords_oclc')
def elastic_build_aarecords_oclc():
    elastic_build_aarecords_oclc_internal()

def elastic_build_aarecords_oclc_internal():
    print("Do a dummy detect of language so that we're sure the model is downloaded")
    ftlangdetect.detect('dummy')

    MAX_WORLDCAT = 999999999999999
    if SLOW_DATA_IMPORTS:
        MAX_WORLDCAT = 1000

    FIRST_OCLC_ID = None
    # FIRST_OCLC_ID = 123
    OCLC_DONE_ALREADY = 0
    # OCLC_DONE_ALREADY = 100000

    with engine.connect() as connection:
        print("Creating oclc_isbn table")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('CREATE TABLE IF NOT EXISTS isbn13_oclc (isbn13 CHAR(13) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL, oclc_id BIGINT NOT NULL, PRIMARY KEY (isbn13, oclc_id)) ENGINE=MyISAM ROW_FORMAT=FIXED DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin')

    with multiprocessing.Pool(THREADS) as executor:
        print("Processing from oclc")
        oclc_file = indexed_zstd.IndexedZstdFile('/worldcat/annas_archive_meta__aacid__worldcat__20231001T025039Z--20231001T235839Z.jsonl.seekable.zst')
        if FIRST_OCLC_ID is not None:
            oclc_file.seek(allthethings.utils.get_worldcat_pos_before_id(FIRST_OCLC_ID))
        with tqdm.tqdm(total=min(MAX_WORLDCAT, 750000000-OCLC_DONE_ALREADY), bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            last_map = []
            total = 0
            last_seen_id = -1
            extra_line = None
            while True:
                batch = collections.defaultdict(list)
                while True:
                    if extra_line is not None:
                        line = extra_line
                        extra_line = None
                    else:
                        line = oclc_file.readline()
                    if len(line) == 0:
                        break
                    if (b'not_found_title_json' in line) or (b'redirect_title_json' in line):
                        continue
                    oclc_id = int(line[len(b'{"aacid":"aacid__worldcat__20231001T025039Z__'):].split(b'__', 1)[0])
                    if oclc_id != last_seen_id: # Don't break when we're still processing the same id
                        if len(batch) >= BATCH_SIZE:
                            extra_line = line
                            break
                    batch[oclc_id].append(line)
                    last_seen_id = oclc_id
                batch = list(batch.items())

                list(last_map)
                if len(batch) == 0:
                    break
                print(f"Processing {len(batch)} aarecords from oclc (worldcat) file ( starting oclc_id: {batch[0][0]} )...")
                last_map = executor.map(elastic_build_aarecords_job_oclc, more_itertools.ichunked(batch, CHUNK_SIZE))
                pbar.update(len(batch))
                total += len(batch)
                if total >= MAX_WORLDCAT:
                    break
    print(f"Done with WorldCat!")

#################################################################################################
# ./run flask cli elastic_build_aarecords_main
@cli.cli.command('elastic_build_aarecords_main')
def elastic_build_aarecords_main():
    elastic_build_aarecords_main_internal()

def elastic_build_aarecords_main_internal():
    before_first_md5 = ''
    # before_first_md5 = '4dcf17fc02034aadd33e2e5151056b5d'
    before_first_doi = ''
    # before_first_doi = ''

    print("Do a dummy detect of language so that we're sure the model is downloaded")
    ftlangdetect.detect('dummy')

    with engine.connect() as connection:
        print("Processing from computed_all_md5s")
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
        cursor.execute('SELECT COUNT(md5) AS count FROM computed_all_md5s WHERE md5 > %(from)s ORDER BY md5 LIMIT 1', { "from": bytes.fromhex(before_first_md5) })
        total = list(cursor.fetchall())[0]['count']
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            current_md5 = bytes.fromhex(before_first_md5)
            while True:
                connection.connection.ping(reconnect=True)
                cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                cursor.execute('SELECT md5 FROM computed_all_md5s WHERE md5 > %(from)s ORDER BY md5 LIMIT %(limit)s', { "from": current_md5, "limit": BATCH_SIZE })
                batch = list(cursor.fetchall())
                if len(batch) == 0:
                    break
                print(f"Processing {len(batch)} aarecords from computed_all_md5s ( starting md5: {batch[0]['md5'].hex()} , ending md5: {batch[-1]['md5'].hex()} )...")
                with multiprocessing.Pool(THREADS) as executor:
                    list(executor.map(elastic_build_aarecords_job, more_itertools.ichunked([f"md5:{item['md5'].hex()}" for item in batch], CHUNK_SIZE)))
                pbar.update(len(batch))
                current_md5 = batch[-1]['md5']

            print("Processing from scihub_dois_without_matches")
            connection.connection.ping(reconnect=True)
            cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
            cursor.execute('SELECT COUNT(doi) AS count FROM scihub_dois_without_matches WHERE doi > %(from)s ORDER BY doi LIMIT 1', { "from": before_first_doi })
            total = list(cursor.fetchall())[0]['count']
            with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
                current_doi = before_first_doi
                while True:
                    connection.connection.ping(reconnect=True)
                    cursor = connection.connection.cursor(pymysql.cursors.SSDictCursor)
                    cursor.execute('SELECT doi FROM scihub_dois_without_matches WHERE doi > %(from)s ORDER BY doi LIMIT %(limit)s', { "from": current_doi, "limit": BATCH_SIZE })
                    batch = list(cursor.fetchall())
                    if len(batch) == 0:
                        break
                    print(f"Processing {len(batch)} aarecords from scihub_dois_without_matches ( starting doi: {batch[0]['doi']}, ending doi: {batch[-1]['doi']} )...")
                    with multiprocessing.Pool(THREADS) as executor:
                        list(executor.map(elastic_build_aarecords_job, more_itertools.ichunked([f"doi:{item['doi']}" for item in batch], CHUNK_SIZE)))
                    pbar.update(len(batch))
                    current_doi = batch[-1]['doi']

        print(f"Done with main!")


#################################################################################################
# ./run flask cli mariapersist_reset
@cli.cli.command('mariapersist_reset')
def mariapersist_reset():
    print("Erasing entire persistent database ('mariapersist')! Did you double-check that any production databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)
    mariapersist_reset_internal()

def mariapersist_reset_internal():
    # Per https://stackoverflow.com/a/4060259
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    mariapersist_engine_multi = create_engine(mariapersist_url, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    cursor = mariapersist_engine_multi.raw_connection().cursor()

    # From https://stackoverflow.com/a/8248281
    cursor.execute("SELECT concat('DROP TABLE IF EXISTS `', table_name, '`;') FROM information_schema.tables WHERE table_schema = 'mariapersist' AND table_name LIKE 'mariapersist_%';")
    delete_all_query = "\n".join([item[0] for item in cursor.fetchall()])
    if len(delete_all_query) > 0:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute(delete_all_query)
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1; COMMIT;")

    cursor.execute(pathlib.Path(os.path.join(__location__, 'mariapersist_migration.sql')).read_text())
    cursor.close()

#################################################################################################
# Send test email
# ./run flask cli send_test_email <email_addr>
@cli.cli.command('send_test_email')
@click.argument("email_addr")
def send_test_email(email_addr):
    email_msg = flask_mail.Message(subject="Hello", body="Hi there, this is a test!", recipients=[email_addr])
    mail.send(email_msg)
