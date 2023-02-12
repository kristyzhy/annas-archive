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

from config import settings
from flask import Blueprint, __version__, render_template, make_response, redirect, request
from allthethings.extensions import engine, mariadb_url, es, Reflected
from sqlalchemy import select, func, text, create_engine
from sqlalchemy.dialects.mysql import match
from sqlalchemy.orm import Session
from pymysql.constants import CLIENT
from allthethings.extensions import ComputedAllMd5s

from allthethings.page.views import get_md5_dicts_mysql

cli = Blueprint("cli", __name__, template_folder="templates")


#################################################################################################
# ./run flask cli dbreset
@cli.cli.command('dbreset')
def dbreset():
    print("Erasing entire database (2 MariaDB databases servers + 1 ElasticSearch)! Did you double-check that any production/large databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)

    # Per https://stackoverflow.com/a/4060259
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    engine_multi = create_engine(mariadb_url, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    cursor = engine_multi.raw_connection().cursor()

    # Generated with `docker-compose exec mariadb mysqldump -u allthethings -ppassword --opt --where="1 limit 100" --skip-comments --ignore-table=computed_all_md5s allthethings > mariadb_dump.sql`
    cursor.execute(pathlib.Path(os.path.join(__location__, 'mariadb_dump.sql')).read_text())
    cursor.close()

    mysql_build_computed_all_md5s_internal()

    time.sleep(1)
    Reflected.prepare(engine_multi)
    elastic_reset_md5_dicts_internal()
    elastic_build_md5_dicts_internal()

    mariapersist_reset_internal()

    print("Done! Search for example for 'Rhythms of the brain': http://localhost:8000/search?q=Rhythms+of+the+brain")


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]

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
# used in the app, but it is used for `./run flask cli elastic_build_md5_dicts`.
# ./run flask cli mysql_build_computed_all_md5s
@cli.cli.command('mysql_build_computed_all_md5s')
def mysql_build_computed_all_md5s():
    print("Erasing entire MySQL 'computed_all_md5s' table! Did you double-check that any production/large databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)

    mysql_build_computed_all_md5s_internal()

def mysql_build_computed_all_md5s_internal():
    engine_multi = create_engine(mariadb_url, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    cursor = engine_multi.raw_connection().cursor()
    sql = """
        DROP TABLE IF EXISTS `computed_all_md5s`;
        CREATE TABLE computed_all_md5s (
            md5 CHAR(32) NOT NULL,
            PRIMARY KEY (md5)
        ) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 SELECT md5 FROM libgenli_files;
        INSERT IGNORE INTO computed_all_md5s SELECT LOWER(md5) FROM zlib_book WHERE md5 != '';
        INSERT IGNORE INTO computed_all_md5s SELECT LOWER(md5_reported) FROM zlib_book WHERE md5_reported != '';
        INSERT IGNORE INTO computed_all_md5s SELECT LOWER(MD5) FROM libgenrs_updated;
        INSERT IGNORE INTO computed_all_md5s SELECT LOWER(MD5) FROM libgenrs_fiction;
    """
    cursor.execute(sql)
    cursor.close()


#################################################################################################
# Recreate "md5_dicts" index in ElasticSearch, without filling it with data yet.
# (That is done with `./run flask cli elastic_build_md5_dicts`)
# ./run flask cli elastic_reset_md5_dicts
@cli.cli.command('elastic_reset_md5_dicts')
def elastic_reset_md5_dicts():
    print("Erasing entire ElasticSearch 'md5_dicts' index! Did you double-check that any production/large databases are offline/inaccessible from here?")
    time.sleep(2)
    print("Giving you 5 seconds to abort..")
    time.sleep(5)

    elastic_reset_md5_dicts_internal()

def elastic_reset_md5_dicts_internal():
    es.options(ignore_status=[400,404]).indices.delete(index='md5_dicts')
    es.indices.create(index='md5_dicts', body={
        "mappings": {
            "dynamic": "strict",
            "properties": {
                "lgrsnf_book": {
                    "properties": {
                        "id": { "type": "integer", "index": False, "doc_values": False },
                        "md5": { "type": "keyword", "index": False, "doc_values": False }
                    }
                },
                "lgrsfic_book": {
                    "properties": {
                        "id": { "type": "integer", "index": False, "doc_values": False },
                        "md5": { "type": "keyword", "index": False, "doc_values": False }
                    }
                },
                "lgli_file": {
                    "properties": {
                        "f_id": { "type": "integer", "index": False, "doc_values": False },
                        "md5": { "type": "keyword", "index": False, "doc_values": False },
                        "libgen_topic": { "type": "keyword", "index": False, "doc_values": False }
                    }
                },
                "zlib_book": {
                    "properties": {
                        "zlibrary_id": { "type": "integer", "index": False, "doc_values": False },
                        "md5": { "type": "keyword", "index": False, "doc_values": False },
                        "md5_reported": { "type": "keyword", "index": False, "doc_values": False },
                        "filesize": { "type": "long", "index": False, "doc_values": False },
                        "filesize_reported": { "type": "long", "index": False, "doc_values": False },
                        "in_libgen": { "type": "byte", "index": False, "doc_values": False },
                        "pilimi_torrent": { "type": "keyword", "index": False, "doc_values": False }
                    }
                },
                "ipfs_infos": {
                    "properties": {
                        "ipfs_cid": { "type": "keyword", "index": False, "doc_values": False },
                        "filename": { "type": "keyword", "index": False, "doc_values": False },
                        "from": { "type": "keyword", "index": False, "doc_values": False }
                    }
                },
                "file_unified_data": {
                    "properties": {
                        "original_filename_best": { "type": "keyword", "index": False, "doc_values": False },
                        "original_filename_additional": { "type": "keyword", "index": False, "doc_values": False },
                        "original_filename_best_name_only": { "type": "keyword", "index": False, "doc_values": False },
                        "cover_url_best": { "type": "keyword", "index": False, "doc_values": False },
                        "cover_url_additional": { "type": "keyword", "index": False, "doc_values": False },
                        "extension_best": { "type": "keyword", "index": True, "doc_values": True },
                        "extension_additional": { "type": "keyword", "index": False, "doc_values": False },
                        "filesize_best": { "type": "long", "index": False, "doc_values": True },
                        "filesize_additional": { "type": "long", "index": False, "doc_values": False },
                        "title_best": { "type": "keyword", "index": False, "doc_values": False },
                        "title_additional": { "type": "keyword", "index": False, "doc_values": False },
                        "author_best": { "type": "keyword", "index": False, "doc_values": False },
                        "author_additional": { "type": "keyword", "index": False, "doc_values": False },
                        "publisher_best": { "type": "keyword", "index": False, "doc_values": False },
                        "publisher_additional": { "type": "keyword", "index": False, "doc_values": False },
                        "edition_varia_best": { "type": "keyword", "index": False, "doc_values": False },
                        "edition_varia_additional": { "type": "keyword", "index": False, "doc_values": False },
                        "year_best": { "type": "keyword", "index": True, "doc_values": True },
                        "year_additional": { "type": "keyword", "index": False, "doc_values": False },
                        "comments_best": { "type": "keyword", "index": False, "doc_values": False },
                        "comments_additional": { "type": "keyword", "index": False, "doc_values": False },
                        "stripped_description_best": { "type": "keyword", "index": False, "doc_values": False },
                        "stripped_description_additional": { "type": "keyword", "index": False, "doc_values": False },
                        "language_codes": { "type": "keyword", "index": True, "doc_values": True },
                        "most_likely_language_code": { "type": "keyword", "index": True, "doc_values": True },
                        "sanitized_isbns": { "type": "keyword", "index": True, "doc_values": False },
                        "asin_multiple": { "type": "keyword", "index": True, "doc_values": False },
                        "googlebookid_multiple": { "type": "keyword", "index": True, "doc_values": False },
                        "openlibraryid_multiple": { "type": "keyword", "index": True, "doc_values": False },
                        "doi_multiple": { "type": "keyword", "index": True, "doc_values": False },
                        "problems": {
                            "properties": {
                                "type": { "type": "keyword", "index": False, "doc_values": True },
                                "descr": { "type": "keyword", "index": False, "doc_values": False }
                            }
                        },
                        "content_type": { "type": "keyword", "index": True, "doc_values": True }
                    }
                },
                "search_only_fields": {
                    "properties": {
                        "search_text": { "type": "text", "index": True, "analyzer": "icu_analyzer" },
                        "score_base": { "type": "float", "index": False, "doc_values": True }
                    }
                }
            }
        },
        "settings": {
            "index.number_of_replicas": 0,
            "index.search.slowlog.threshold.query.warn": "2s",
            "index.store.preload": ["nvd", "dvd"],
            "index.sort.field": "search_only_fields.score_base",
            "index.sort.order": "desc"
        }
    })

#################################################################################################
# Regenerate "md5_dicts" index in ElasticSearch.
# ./run flask cli elastic_build_md5_dicts
@cli.cli.command('elastic_build_md5_dicts')
def elastic_build_md5_dicts():
    elastic_build_md5_dicts_internal()

def elastic_build_md5_dicts_job(canonical_md5s):
    try:
        with Session(engine) as session:
            md5_dicts = get_md5_dicts_mysql(session, canonical_md5s)
            for md5_dict in md5_dicts:
                md5_dict['_op_type'] = 'index'
                md5_dict['_index'] = 'md5_dicts'
                md5_dict['_id'] = md5_dict['md5']
                del md5_dict['md5']
                
            elasticsearch.helpers.bulk(es, md5_dicts, request_timeout=30)
            # print(f"Processed {len(md5_dicts)} md5s")
    except Exception as err:
        print(repr(err))
        traceback.print_tb(err.__traceback__)
        raise err

def elastic_build_md5_dicts_internal():
    THREADS = 70
    CHUNK_SIZE = 50
    BATCH_SIZE = 100000

    first_md5 = ''
    # Uncomment to resume from a given md5, e.g. after a crash
    # first_md5 = '0337ca7b631f796fa2f465ef42cb815c'

    print("Do a dummy detect of language so that we're sure the model is downloaded")
    ftlangdetect.detect('dummy')

    with engine.connect() as conn:
        total = conn.execute(select([func.count(ComputedAllMd5s.md5)])).scalar()
        with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
            for batch in query_yield_batches(conn, select(ComputedAllMd5s.md5).where(ComputedAllMd5s.md5 >= first_md5), ComputedAllMd5s.md5, BATCH_SIZE):
                with multiprocessing.Pool(THREADS) as executor:
                    print(f"Processing {len(batch)} md5s from computed_all_md5s (starting md5: {batch[0][0]})...")
                    executor.map(elastic_build_md5_dicts_job, chunks([item[0] for item in batch], CHUNK_SIZE))
                    pbar.update(len(batch))

            print(f"Done!")


# Kept for future reference, for future migrations
# #################################################################################################
# # ./run flask cli elastic_migrate_from_md5_dicts_to_md5_dicts2
# @cli.cli.command('elastic_migrate_from_md5_dicts_to_md5_dicts2')
# def elastic_migrate_from_md5_dicts_to_md5_dicts2():
#     print("Erasing entire ElasticSearch 'md5_dicts2' index! Did you double-check that any production/large databases are offline/inaccessible from here?")
#     time.sleep(2)
#     print("Giving you 5 seconds to abort..")
#     time.sleep(5)

#     elastic_migrate_from_md5_dicts_to_md5_dicts2_internal()

# def elastic_migrate_from_md5_dicts_to_md5_dicts2_job(canonical_md5s):
#     try:
#         search_results_raw = es.mget(index="md5_dicts", ids=canonical_md5s)
#         # print(f"{search_results_raw}"[0:10000])
#         new_md5_dicts = []
#         for item in search_results_raw['docs']:
#             new_md5_dicts.append({
#                 **item['_source'],
#                 '_op_type': 'index',
#                 '_index': 'md5_dicts2',
#                 '_id': item['_id'],
#             })
                
#         elasticsearch.helpers.bulk(es, new_md5_dicts, request_timeout=30)
#         # print(f"Processed {len(new_md5_dicts)} md5s")
#     except Exception as err:
#         print(repr(err))
#         raise err

# def elastic_migrate_from_md5_dicts_to_md5_dicts2_internal():
#     elastic_reset_md5_dicts_internal()

#     THREADS = 60
#     CHUNK_SIZE = 70
#     BATCH_SIZE = 100000

#     first_md5 = ''
#     # Uncomment to resume from a given md5, e.g. after a crash (be sure to also comment out the index deletion above)
#     # first_md5 = '0337ca7b631f796fa2f465ef42cb815c'

#     with engine.connect() as conn:
#         total = conn.execute(select([func.count(ComputedAllMd5s.md5)])).scalar()
#         with tqdm.tqdm(total=total, bar_format='{l_bar}{bar}{r_bar} {eta}') as pbar:
#             for batch in query_yield_batches(conn, select(ComputedAllMd5s.md5).where(ComputedAllMd5s.md5 >= first_md5), ComputedAllMd5s.md5, BATCH_SIZE):
#                 with multiprocessing.Pool(THREADS) as executor:
#                     print(f"Processing {len(batch)} md5s from computed_all_md5s (starting md5: {batch[0][0]})...")
#                     executor.map(elastic_migrate_from_md5_dicts_to_md5_dicts2_job, chunks([item[0] for item in batch], CHUNK_SIZE))
#                     pbar.update(len(batch))

#             print(f"Done!")



#################################################################################################
# ./run flask cli mariapersist_reset
@cli.cli.command('mariapersist_reset')
def mariapersist_reset():
    print("Erasing entire persistent database ('mariapersist')! Did you double-check that any production databases are offline/inaccessible from here?")
    # time.sleep(2)
    print("Giving you 5 seconds to abort..")
    # time.sleep(5)
    mariapersist_reset_internal()

def mariapersist_reset_internal():
    # Per https://stackoverflow.com/a/4060259
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))

    mariapersist_engine_multi = create_engine(mariapersist_url, connect_args={"client_flag": CLIENT.MULTI_STATEMENTS})
    cursor = mariapersist_engine_multi.raw_connection().cursor()

    cursor.execute(pathlib.Path(os.path.join(__location__, 'mariapersist_drop_all.sql')).read_text())
    cursor.execute(pathlib.Path(os.path.join(__location__, 'mariapersist_migration_001.sql')).read_text())
    cursor.execute(pathlib.Path(os.path.join(__location__, 'mariapersist_migration_002.sql')).read_text())
    cursor.close()
