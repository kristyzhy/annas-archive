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
import gc
import random
import slugify
import elasticsearch
import elasticsearch.helpers
import ftlangdetect
import traceback
import urllib.parse
import urllib.request
import datetime
import base64
import hashlib
import shortuuid
import pymysql.cursors

from flask import g, Blueprint, __version__, render_template, make_response, redirect, request, send_file
from allthethings.extensions import engine, es, es_aux, babel, mariapersist_engine, ZlibBook, ZlibIsbn, IsbndbIsbns, LibgenliEditions, LibgenliEditionsAddDescr, LibgenliEditionsToFiles, LibgenliElemDescr, LibgenliFiles, LibgenliFilesAddDescr, LibgenliPublishers, LibgenliSeries, LibgenliSeriesAddDescr, LibgenrsDescription, LibgenrsFiction, LibgenrsFictionDescription, LibgenrsFictionHashes, LibgenrsHashes, LibgenrsTopics, LibgenrsUpdated, OlBase, AaLgliComics202208Files, AaIa202306Metadata, AaIa202306Files, Ia2AcsmpdfFiles, MariapersistSmallFiles
from sqlalchemy import select, func, text
from sqlalchemy.dialects.mysql import match
from sqlalchemy.orm import defaultload, Session
from flask_babel import gettext, ngettext, force_locale, get_locale

import allthethings.utils

page = Blueprint("page", __name__, template_folder="templates")

# Per https://annas-software.org/AnnaArchivist/annas-archive/-/issues/37
search_filtered_bad_aarecord_ids = [
    "md5:b0647953a182171074873b61200c71dd",
    "md5:820a4f8961ae0a76ad265f1678b7dfa5",

    # Likely CSAM
    "md5:d897ffc4e64cbaeae53a6005b6f155cc",
    "md5:8ae28a86719e3a4400145ac18b621efd",
    "md5:285171dbb2d1d56aa405ad3f5e1bc718",
    "md5:8ac4facd6562c28d7583d251aa2c9020",
    "md5:6c1b1ea486960a1ad548cd5c02c465a1",
    "md5:414e8f3a8bc0f63de37cd52bd6d8701e",
    "md5:c6cddcf83c558b758094e06b97067c89",
    "md5:5457b152ef9a91ca3e2d8b3a2309a106",
    "md5:02973f6d111c140510fcdf84b1d00c35",
    "md5:d4c01f9370c5ac93eb5ee5c2037ac794",
    "md5:08499f336fbf8d31f8e7fadaaa517477",
    "md5:351024f9b101ac7797c648ff43dcf76e",
]

ES_TIMEOUT_PRIMARY = "3s"
ES_TIMEOUT_ALL_AGG = "10s"
ES_TIMEOUT = "300ms"

# Taken from https://github.com/internetarchive/openlibrary/blob/e7e8aa5b8c/openlibrary/plugins/openlibrary/pages/languages.page
# because https://openlibrary.org/languages.json doesn't seem to give a complete list? (And ?limit=.. doesn't seem to work.)
ol_languages_json = json.load(open(os.path.dirname(os.path.realpath(__file__)) + '/ol_languages.json'))
ol_languages = {}
for language in ol_languages_json:
    ol_languages[language['key']] = language


# Good pages to test with:
# * http://localhost:8000/zlib/1
# * http://localhost:8000/zlib/100
# * http://localhost:8000/zlib/4698900
# * http://localhost:8000/zlib/19005844
# * http://localhost:8000/zlib/2425562
# * http://localhost:8000/ol/OL100362M
# * http://localhost:8000/ol/OL33897070M
# * http://localhost:8000/ol/OL39479373M
# * http://localhost:8000/ol/OL1016679M
# * http://localhost:8000/ol/OL10045347M
# * http://localhost:8000/ol/OL1183530M
# * http://localhost:8000/ol/OL1002667M
# * http://localhost:8000/ol/OL1000021M
# * http://localhost:8000/ol/OL13573618M
# * http://localhost:8000/ol/OL999950M
# * http://localhost:8000/ol/OL998696M
# * http://localhost:8000/ol/OL22555477M
# * http://localhost:8000/ol/OL15990933M
# * http://localhost:8000/ol/OL6785286M
# * http://localhost:8000/ol/OL3296622M
# * http://localhost:8000/ol/OL2862972M
# * http://localhost:8000/ol/OL24764643M
# * http://localhost:8000/ol/OL7002375M
# * http://localhost:8000/db/lgrs/nf/288054.json
# * http://localhost:8000/db/lgrs/nf/3175616.json
# * http://localhost:8000/db/lgrs/nf/2933905.json
# * http://localhost:8000/db/lgrs/nf/1125703.json
# * http://localhost:8000/db/lgrs/nf/59.json
# * http://localhost:8000/db/lgrs/nf/1195487.json
# * http://localhost:8000/db/lgrs/nf/1360257.json
# * http://localhost:8000/db/lgrs/nf/357571.json
# * http://localhost:8000/db/lgrs/nf/2425562.json
# * http://localhost:8000/db/lgrs/nf/3354081.json
# * http://localhost:8000/db/lgrs/nf/3357578.json
# * http://localhost:8000/db/lgrs/nf/3357145.json
# * http://localhost:8000/db/lgrs/nf/2040423.json
# * http://localhost:8000/db/lgrs/fic/1314135.json
# * http://localhost:8000/db/lgrs/fic/25761.json
# * http://localhost:8000/db/lgrs/fic/2443846.json
# * http://localhost:8000/db/lgrs/fic/2473252.json
# * http://localhost:8000/db/lgrs/fic/2340232.json
# * http://localhost:8000/db/lgrs/fic/1122239.json
# * http://localhost:8000/db/lgrs/fic/6862.json
# * http://localhost:8000/db/lgli/file/100.json
# * http://localhost:8000/db/lgli/file/1635550.json
# * http://localhost:8000/db/lgli/file/94069002.json
# * http://localhost:8000/db/lgli/file/40122.json
# * http://localhost:8000/db/lgli/file/21174.json
# * http://localhost:8000/db/lgli/file/91051161.json
# * http://localhost:8000/db/lgli/file/733269.json
# * http://localhost:8000/db/lgli/file/156965.json
# * http://localhost:8000/db/lgli/file/10000000.json
# * http://localhost:8000/db/lgli/file/933304.json
# * http://localhost:8000/db/lgli/file/97559799.json
# * http://localhost:8000/db/lgli/file/3756440.json
# * http://localhost:8000/db/lgli/file/91128129.json
# * http://localhost:8000/db/lgli/file/44109.json
# * http://localhost:8000/db/lgli/file/2264591.json
# * http://localhost:8000/db/lgli/file/151611.json
# * http://localhost:8000/db/lgli/file/1868248.json
# * http://localhost:8000/db/lgli/file/1761341.json
# * http://localhost:8000/db/lgli/file/4031847.json
# * http://localhost:8000/db/lgli/file/2827612.json
# * http://localhost:8000/db/lgli/file/2096298.json
# * http://localhost:8000/db/lgli/file/96751802.json
# * http://localhost:8000/db/lgli/file/5064830.json
# * http://localhost:8000/db/lgli/file/1747221.json
# * http://localhost:8000/db/lgli/file/1833886.json
# * http://localhost:8000/db/lgli/file/3908879.json
# * http://localhost:8000/db/lgli/file/41752.json
# * http://localhost:8000/db/lgli/file/97768237.json
# * http://localhost:8000/db/lgli/file/4031335.json
# * http://localhost:8000/db/lgli/file/1842179.json
# * http://localhost:8000/db/lgli/file/97562793.json
# * http://localhost:8000/db/lgli/file/4029864.json
# * http://localhost:8000/db/lgli/file/2834701.json
# * http://localhost:8000/db/lgli/file/97562143.json
# * http://localhost:8000/isbndb/9789514596933
# * http://localhost:8000/isbndb/9780000000439
# * http://localhost:8000/isbndb/9780001055506
# * http://localhost:8000/isbndb/9780316769174
# * http://localhost:8000/md5/8fcb740b8c13f202e89e05c4937c09ac

def normalize_doi(string):
    if not (('/' in string) and (' ' not in string)):
        return ''
    if string.startswith('doi:10.'):
        return string[len('doi:'):]
    if string.startswith('10.'):
        return string
    return ''

# Example: zlib2/pilimi-zlib2-0-14679999-extra/11078831
def make_temp_anon_zlib_path(zlibrary_id, pilimi_torrent):
    prefix = "zlib1"
    if "-zlib2-" in pilimi_torrent:
        prefix = "zlib2"
    return f"e/{prefix}/{pilimi_torrent.replace('.torrent', '')}/{zlibrary_id}"

def make_temp_anon_aac_path(prefix, file_aac_id, data_folder):
    date = data_folder.split('__')[3][0:8]
    return f"{prefix}/{date}/{data_folder}/{file_aac_id}"

def strip_description(description):
    return re.sub(r'<[^<]+?>', r' ', re.sub(r'<a.+?href="([^"]+)"[^>]*>', r'(\1) ', description.replace('</p>', '\n\n').replace('</P>', '\n\n').replace('<br>', '\n').replace('<BR>', '\n'))).strip()

def nice_json(some_dict):
    json_str = orjson.dumps(some_dict, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS, default=str).decode('utf-8')
    # Triple-slashes means it shouldn't be put on the previous line.
    return re.sub(r'[ \n]*"//(?!/)', ' "//', json_str, flags=re.MULTILINE)

@functools.cache
def get_bcp47_lang_codes_parse_substr(substr):
    lang = ''
    try:
        lang = str(langcodes.get(substr))
    except:
        try:
            lang = str(langcodes.find(substr))
        except:
            # In rare cases, disambiguate by saying that `substr` is written in English
            try:
                lang = str(langcodes.find(substr, language='en'))
            except:
                lang = ''
    # We have a bunch of weird data that gets interpreted as "Egyptian Sign Language" when it's
    # clearly all just Spanish..
    if lang == "esl":
        lang = "es"
    # Further specification of English is unnecessary.
    if lang.startswith("en-"):
        lang = "en"
    return lang

@functools.cache
def get_bcp47_lang_codes(string):
    potential_codes = set()
    potential_codes.add(get_bcp47_lang_codes_parse_substr(string))
    for substr in re.split(r'[-_,;/]', string):
        potential_codes.add(get_bcp47_lang_codes_parse_substr(substr.strip()))
    potential_codes.discard('')
    return list(potential_codes)

def combine_bcp47_lang_codes(sets_of_codes):
    combined_codes = set()
    for codes in sets_of_codes:
        for code in codes:
            combined_codes.add(code)
    return list(combined_codes)

@functools.cache
def get_display_name_for_lang(lang_code, display_lang):
    result = langcodes.Language.make(lang_code).display_name(display_lang)
    if '[' not in result:
        result = result + ' [' + lang_code + ']'
    return result.replace(' []', '')

def add_comments_to_dict(before_dict, comments):
    after_dict = {}
    for key, value in before_dict.items():
        if key in comments:
            comment = comments[key]
            comment_content = comment[1][0] if len(comment[1]) == 1 else comment[1]
            if comment[0] == 'before':
                # Triple-slashes means it shouldn't be put on the previous line by nice_json.
                after_dict["///" + key] = comment_content
            after_dict[key] = value
            if comment[0] == 'after':
                after_dict["//" + key] = comment_content
        else:
            after_dict[key] = value
    return after_dict

@page.get("/")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def home_page():
    torrents_data = get_torrents_data()
    return render_template("page/home.html", header_active="home/home", torrents_data=torrents_data)

@page.get("/login")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def login_page():
    return redirect(f"/account", code=301)
    # return render_template("page/login.html", header_active="account")

@page.get("/about")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def about_page():
    popular_ids = [
        "md5:8336332bf5877e3adbfb60ac70720cd5", # Against intellectual monopoly
        "md5:f0a0beca050610397b9a1c2604c1a472", # Harry Potter
        "md5:61a1797d76fc9a511fb4326f265c957b", # Cryptonomicon
        "md5:4b3cd128c0cc11c1223911336f948523", # Subtle art of not giving a f*ck
        "md5:6d6a96f761636b11f7e397b451c62506", # Game of thrones
        "md5:0d9b713d0dcda4c9832fcb056f3e4102", # Aaron Swartz
        "md5:45126b536bbdd32c0484bd3899e10d39", # Three-body problem
        "md5:6963187473f4f037a28e2fe1153ca793", # How music got free
        "md5:6db7e0c1efc227bc4a11fac3caff619b", # It ends with us
        "md5:7849ad74f44619db11c17b85f1a7f5c8", # Lord of the rings
        "md5:6ed2d768ec1668c73e4fa742e3df78d6", # Physics
    ]
    with Session(engine) as session:
        aarecords = get_aarecords_elasticsearch(popular_ids)
        aarecords.sort(key=lambda aarecord: popular_ids.index(aarecord['id']))

        return render_template(
            "page/about.html",
            header_active="home/about",
            aarecords=aarecords,
        )

@page.get("/security")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def security_page():
    return render_template("page/security.html", header_active="home/security")

@page.get("/mobile")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def mobile_page():
    return render_template("page/mobile.html", header_active="home/mobile")

# @page.get("/wechat")
# @allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
# def wechat_page():
#     return render_template("page/wechat.html", header_active="home/wechat")

@page.get("/llm")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def llm_page():
    return render_template("page/llm.html", header_active="home/llm")

@page.get("/browser_verification")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def browser_verification_page():
    return render_template("page/browser_verification.html", header_active="home/search")

@functools.cache
def get_stats_data():
    with engine.connect() as connection:
        libgenrs_time = connection.execute(select(LibgenrsUpdated.TimeLastModified).order_by(LibgenrsUpdated.ID.desc()).limit(1)).scalars().first()
        libgenrs_date = str(libgenrs_time.date()) if libgenrs_time is not None else ''
        libgenli_time = connection.execute(select(LibgenliFiles.time_last_modified).order_by(LibgenliFiles.f_id.desc()).limit(1)).scalars().first()
        libgenli_date = str(libgenli_time.date()) if libgenli_time is not None else ''
        # OpenLibrary author keys seem randomly distributed, so some random prefix is good enough.
        openlib_time = connection.execute(select(OlBase.last_modified).where(OlBase.ol_key.like("/authors/OL111%")).order_by(OlBase.last_modified.desc()).limit(1)).scalars().first()
        openlib_date = str(openlib_time.date()) if openlib_time is not None else ''
        ia_aacid = connection.execute(select(Ia2AcsmpdfFiles.aacid).order_by(Ia2AcsmpdfFiles.aacid.desc()).limit(1)).scalars().first()
        ia_date_raw = ia_aacid.split('__')[2][0:8]
        ia_date = f"{ia_date_raw[0:4]}-{ia_date_raw[4:6]}-{ia_date_raw[6:8]}"

        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT metadata FROM annas_archive_meta__aacid__zlib3_records ORDER BY aacid DESC LIMIT 1')
        zlib3_record = cursor.fetchone()
        zlib_date = orjson.loads(zlib3_record['metadata'])['date_modified'] if zlib3_record is not None else ''

        stats_data_es = dict(es.msearch(
            request_timeout=30,
            max_concurrent_searches=10,
            max_concurrent_shard_requests=10,
            searches=[
                # { "index": "aarecords", "request_cache": False },
                { "index": "aarecords" },
                { "track_total_hits": True, "timeout": "20s", "size": 0, "aggs": { "total_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } } },
                # { "index": "aarecords", "request_cache": False },
                { "index": "aarecords" },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "query": { "bool": { "must_not": [{ "term": { "search_only_fields.search_content_type": { "value": "journal_article" } } }] } },
                    "aggs": {
                        "search_record_sources": {
                            "terms": { "field": "search_only_fields.search_record_sources" },
                            "aggs": {
                                "search_filesize": { "sum": { "field": "search_only_fields.search_filesize" } },
                                "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } },
                            },
                        },
                    },
                },
                # { "index": "aarecords", "request_cache": False },
                { "index": "aarecords" },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "query": { "term": { "search_only_fields.search_content_type": { "value": "journal_article" } } },
                    "aggs": { "search_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } },
                },
                # { "index": "aarecords", "request_cache": False },
                { "index": "aarecords" },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "query": { "term": { "search_only_fields.search_content_type": { "value": "journal_article" } } },
                    "aggs": { "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } } },
                },
                # { "index": "aarecords", "request_cache": False },
                { "index": "aarecords" },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": { "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } } },
                },
            ],
        ))
        stats_data_es_aux = dict(es_aux.msearch(
            request_timeout=30,
            max_concurrent_searches=10,
            max_concurrent_shard_requests=10,
            searches=[
                # { "index": "aarecords_digital_lending", "request_cache": False },
                { "index": "aarecords_digital_lending" },
                { "track_total_hits": True, "timeout": "20s", "size": 0, "aggs": { "total_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } } },
            ],
        ))
        if any([response['timed_out'] for response in stats_data_es['responses']]):
            raise Exception("One of the 'get_stats_data' responses timed out")

        stats_by_group = {}
        for bucket in stats_data_es['responses'][1]['aggregations']['search_record_sources']['buckets']:
            stats_by_group[bucket['key']] = {
                'count': bucket['doc_count'],
                'filesize': bucket['search_filesize']['value'],
                'aa_count': bucket['search_access_types']['buckets'][0]['doc_count'],
            }
        stats_by_group['journals'] = {
            'count': stats_data_es['responses'][2]['hits']['total']['value'],
            'filesize': stats_data_es['responses'][2]['aggregations']['search_filesize']['value'],
            'aa_count': stats_data_es['responses'][3]['aggregations']['search_access_types']['buckets'][0]['doc_count'],
        }
        stats_by_group['total'] = {
            'count': stats_data_es['responses'][0]['hits']['total']['value'],
            'filesize': stats_data_es['responses'][0]['aggregations']['total_filesize']['value'],
            'aa_count': stats_data_es['responses'][4]['aggregations']['search_access_types']['buckets'][0]['doc_count'],
        }
        stats_by_group['ia']['count'] += stats_data_es_aux['responses'][0]['hits']['total']['value']
        stats_by_group['total']['count'] += stats_data_es_aux['responses'][0]['hits']['total']['value']
        stats_by_group['ia']['filesize'] += stats_data_es_aux['responses'][0]['aggregations']['total_filesize']['value']
        stats_by_group['total']['filesize'] += stats_data_es_aux['responses'][0]['aggregations']['total_filesize']['value']

    return {
        'stats_by_group': stats_by_group,
        'libgenrs_date': libgenrs_date,
        'libgenli_date': libgenli_date,
        'openlib_date': openlib_date,
        'zlib_date': zlib_date,
        'ia_date': ia_date,
        'isbndb_date': '2022-09-01',
        'isbn_country_date': '2022-02-11',
        'oclc_date': '2023-10-01',
    }

def get_torrents_data():
    with mariapersist_engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f'SELECT mariapersist_small_files.created, mariapersist_small_files.file_path, mariapersist_small_files.metadata, s.metadata AS scrape_metadata, s.created AS scrape_created FROM mariapersist_small_files LEFT JOIN (SELECT mariapersist_torrent_scrapes.* FROM mariapersist_torrent_scrapes INNER JOIN (SELECT file_path, MAX(created) AS max_created FROM mariapersist_torrent_scrapes GROUP BY file_path) s2 ON (mariapersist_torrent_scrapes.file_path = s2.file_path AND mariapersist_torrent_scrapes.created = s2.max_created)) s USING (file_path) WHERE mariapersist_small_files.file_path LIKE "torrents/managed_by_aa/%" GROUP BY mariapersist_small_files.file_path ORDER BY created ASC, scrape_created DESC LIMIT 10000')
        small_files = cursor.fetchall()

        group_sizes = collections.defaultdict(int)
        small_file_dicts_grouped = collections.defaultdict(list)
        aac_meta_file_paths_grouped = collections.defaultdict(list)
        seeder_counts = collections.defaultdict(int)
        seeder_sizes = collections.defaultdict(int)
        for small_file in small_files:
            metadata = orjson.loads(small_file['metadata'])
            group = small_file['file_path'].split('/')[2]
            aac_meta_prefix = 'torrents/managed_by_aa/annas_archive_meta__aacid/annas_archive_meta__aacid__'
            if small_file['file_path'].startswith(aac_meta_prefix):
                aac_group = small_file['file_path'][len(aac_meta_prefix):].split('__', 1)[0]
                aac_meta_file_paths_grouped[aac_group].append(small_file['file_path'])
                group = aac_group
            aac_data_prefix = 'torrents/managed_by_aa/annas_archive_data__aacid/annas_archive_data__aacid__'
            if small_file['file_path'].startswith(aac_data_prefix):
                aac_group = small_file['file_path'][len(aac_data_prefix):].split('__', 1)[0]
                group = aac_group
            if 'zlib3' in small_file['file_path']:
                group = 'zlib'
            if 'ia2_acsmpdf_files' in small_file['file_path']:
                group = 'ia'

            scrape_metadata = {"scrape":{}}
            if small_file['scrape_metadata'] is not None:
                scrape_metadata = orjson.loads(small_file['scrape_metadata'])
                if scrape_metadata['scrape']['seeders'] < 4:
                    seeder_counts[0] += 1
                    seeder_sizes[0] += metadata['data_size']
                elif scrape_metadata['scrape']['seeders'] < 11:
                    seeder_counts[1] += 1
                    seeder_sizes[1] += metadata['data_size']
                else:
                    seeder_counts[2] += 1
                    seeder_sizes[2] += metadata['data_size']

            group_sizes[group] += metadata['data_size']
            small_file_dicts_grouped[group].append({ **small_file, "metadata": metadata, "size_string": format_filesize(metadata['data_size']), "display_name": small_file['file_path'].split('/')[-1], "scrape_metadata": scrape_metadata, "scrape_created": small_file['scrape_created'], 'scrape_created_delta': small_file['scrape_created'] - datetime.datetime.now() })

        group_size_strings = { group: format_filesize(total) for group, total in group_sizes.items() }
        seeder_size_strings = { index: format_filesize(seeder_sizes[index]) for index in [0,1,2] }

        obsolete_file_paths = [
            'torrents/managed_by_aa/zlib/pilimi-zlib-index-2022-06-28.torrent'
        ]
        for file_path_list in aac_meta_file_paths_grouped.values():
            obsolete_file_paths += file_path_list[0:-1]

        return {
            'small_file_dicts_grouped': small_file_dicts_grouped,
            'obsolete_file_paths': obsolete_file_paths,
            'group_size_strings': group_size_strings,
            'seeder_counts': seeder_counts,
            'seeder_size_strings': seeder_size_strings,
        }

@page.get("/datasets")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def datasets_page():
    return render_template(
        "page/datasets.html",
        header_active="home/datasets",
        stats_data=get_stats_data(),
    )

@page.get("/datasets/ia")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def datasets_ia_page():
    return render_template("page/datasets_ia.html", header_active="home/datasets", stats_data=get_stats_data())

@page.get("/datasets/zlib")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def datasets_zlib_page():
    return render_template("page/datasets_zlib.html", header_active="home/datasets", stats_data=get_stats_data())

@page.get("/datasets/isbndb")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def datasets_isbndb_page():
    return render_template("page/datasets_isbndb.html", header_active="home/datasets", stats_data=get_stats_data())

@page.get("/datasets/scihub")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def datasets_scihub_page():
    return render_template("page/datasets_scihub.html", header_active="home/datasets", stats_data=get_stats_data())

@page.get("/datasets/libgen_rs")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def datasets_libgen_rs_page():
    with engine.connect() as conn:
        libgenrs_time = conn.execute(select(LibgenrsUpdated.TimeLastModified).order_by(LibgenrsUpdated.ID.desc()).limit(1)).scalars().first()
        libgenrs_date = str(libgenrs_time.date())
    return render_template("page/datasets_libgen_rs.html", header_active="home/datasets", stats_data=get_stats_data())

@page.get("/datasets/libgen_li")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def datasets_libgen_li_page():
    return render_template("page/datasets_libgen_li.html", header_active="home/datasets", stats_data=get_stats_data())

@page.get("/datasets/openlib")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def datasets_openlib_page():
    return render_template("page/datasets_openlib.html", header_active="home/datasets", stats_data=get_stats_data())

@page.get("/datasets/worldcat")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def datasets_worldcat_page():
    return render_template("page/datasets_worldcat.html", header_active="home/datasets", stats_data=get_stats_data())

# @page.get("/datasets/isbn_ranges")
# @allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
# def datasets_isbn_ranges_page():
#     return render_template("page/datasets_isbn_ranges.html", header_active="home/datasets", stats_data=get_stats_data())

@page.get("/copyright")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def copyright_page():
    return render_template("page/copyright.html", header_active="")

@page.get("/fast_download_no_more")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def fast_download_no_more_page():
    return render_template("page/fast_download_no_more.html", header_active="")

@page.get("/fast_download_not_member")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def fast_download_not_member_page():
    return render_template("page/fast_download_not_member.html", header_active="")

@page.get("/torrents")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=10)
def torrents_page():
    torrents_data = get_torrents_data()

    return render_template(
        "page/torrents.html",
        header_active="home/torrents",
        torrents_data=torrents_data,
    )

@page.get("/torrents.json")
@allthethings.utils.no_cache()
def torrents_json_page():
    with mariapersist_engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        small_files = connection.execute(select(MariapersistSmallFiles.created, MariapersistSmallFiles.file_path, MariapersistSmallFiles.metadata).where(MariapersistSmallFiles.file_path.like("torrents/managed_by_aa/%")).order_by(MariapersistSmallFiles.created.asc()).limit(10000)).all()
        output_json = []
        for small_file in small_files:
            output_json.append({ 
                "file_path": small_file.file_path,
                "metadata": orjson.loads(small_file.metadata),
            })
        return orjson.dumps({ "small_files": output_json })

@page.get("/torrents/latest_aac_meta/<string:collection>.torrent")
@allthethings.utils.no_cache()
def torrents_latest_aac_page(collection):
    with mariapersist_engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT data FROM mariapersist_small_files WHERE file_path LIKE CONCAT("torrents/managed_by_aa/annas_archive_meta__aacid/annas_archive_meta__aacid__", %(collection)s, "%%") ORDER BY created DESC LIMIT 1', { "collection": collection })
        file = cursor.fetchone()
        if file is None:
            return "File not found", 404
        return send_file(io.BytesIO(file['data']), as_attachment=True, download_name=f'{collection}.torrent')

@page.get("/small_file/<path:file_path>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def small_file_page(file_path):
    with mariapersist_engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        file = connection.execute(select(MariapersistSmallFiles.data).where(MariapersistSmallFiles.file_path == file_path).limit(10000)).first()
        if file is None:
            return "File not found", 404
        return send_file(io.BytesIO(file.data), as_attachment=True, download_name=file_path.split('/')[-1])


zlib_book_dict_comments = {
    **allthethings.utils.COMMON_DICT_COMMENTS,
    "zlibrary_id": ("before", ["This is a file from the Z-Library collection of Anna's Archive.",
                      "More details at https://annas-archive.org/datasets/zlib",
                      "The source URL is http://loginzlib2vrak5zzpcocc3ouizykn6k5qecgj2tzlnab5wcbqhembyd.onion/md5/<md5_reported>",
                      allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
    "edition_varia_normalized": ("after", ["Anna's Archive version of the 'series', 'volume', 'edition', and 'year' fields; combining them into a single field for display and search."]),
    "in_libgen": ("after", ["Whether at the time of indexing, the book was also available in Libgen."]),
    "pilimi_torrent": ("after", ["Which torrent by Anna's Archive (formerly the Pirate Library Mirror or 'pilimi') the file belongs to."]),
    "filesize_reported": ("after", ["The file size as reported by the Z-Library metadata. Is sometimes different from the actually observed file size of the file, as determined by Anna's Archive."]),
    "md5_reported": ("after", ["The md5 as reported by the Z-Library metadata. Is sometimes different from the actually observed md5 of the file, as determined by Anna's Archive."]),
    "unavailable": ("after", ["Set when Anna's Archive was unable to download the book."]),
    "filesize": ("after", ["The actual filesize as determined by Anna's Archive. Missing for AAC zlib3 records"]),
    "category_id": ("after", ["Z-Library's own categorization system; currently only present for AAC zlib3 records (and not actually used yet)"]),
    "file_data_folder": ("after", ["The AAC data folder / torrent that contains this file"]),
    "record_aacid": ("after", ["The AACID of the corresponding metadata entry in the zlib3_records collection"]),
    "file_aacid": ("after", ["The AACID of the corresponding metadata entry in the zlib3_files collection (corresponding to the data filename)"]),
}
def zlib_add_edition_varia_normalized(zlib_book_dict):
    edition_varia_normalized = []
    if len((zlib_book_dict.get('series') or '').strip()) > 0:
        edition_varia_normalized.append(zlib_book_dict['series'].strip())
    if len((zlib_book_dict.get('volume') or '').strip()) > 0:
        edition_varia_normalized.append(zlib_book_dict['volume'].strip())
    if len((zlib_book_dict.get('edition') or '').strip()) > 0:
        edition_varia_normalized.append(zlib_book_dict['edition'].strip())
    if len((zlib_book_dict.get('year') or '').strip()) > 0:
        edition_varia_normalized.append(zlib_book_dict['year'].strip())
    zlib_book_dict['edition_varia_normalized'] = ', '.join(edition_varia_normalized)

def get_zlib_book_dicts(session, key, values):
    if len(values) == 0:
        return []

    zlib_books = []
    try:
        zlib_books = session.scalars(select(ZlibBook).where(getattr(ZlibBook, key).in_(values))).unique().all()
    except Exception as err:
        print(f"Error in get_zlib_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    zlib_book_dicts = []
    for zlib_book in zlib_books:
        zlib_book_dict = zlib_book.to_dict()
        zlib_book_dict['stripped_description'] = strip_description(zlib_book_dict['description'])
        zlib_book_dict['language_codes'] = get_bcp47_lang_codes(zlib_book_dict['language'] or '')
        zlib_add_edition_varia_normalized(zlib_book_dict)

        allthethings.utils.init_identifiers_and_classification_unified(zlib_book_dict)
        allthethings.utils.add_identifier_unified(zlib_book_dict, 'zlib', zlib_book_dict['zlibrary_id'])
        allthethings.utils.add_isbns_unified(zlib_book_dict, [record.isbn for record in zlib_book.isbns])

        zlib_book_dicts.append(add_comments_to_dict(zlib_book_dict, zlib_book_dict_comments))
    return zlib_book_dicts

def get_aac_zlib3_book_dicts(session, key, values):
    if len(values) == 0:
        return []
    if key == 'zlibrary_id':
        aac_key = 'annas_archive_meta__aacid__zlib3_records.primary_id'
    elif key == 'md5':
        aac_key = 'annas_archive_meta__aacid__zlib3_files.md5'
    elif key == 'md5_reported':
        aac_key = 'annas_archive_meta__aacid__zlib3_records.md5'
    else:
        raise Exception(f"Unexpected 'key' in get_aac_zlib3_book_dicts: '{key}'")
    aac_zlib3_books = []
    try:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f'SELECT annas_archive_meta__aacid__zlib3_records.aacid AS record_aacid, annas_archive_meta__aacid__zlib3_records.metadata AS record_metadata, annas_archive_meta__aacid__zlib3_files.aacid AS file_aacid, annas_archive_meta__aacid__zlib3_files.data_folder AS file_data_folder, annas_archive_meta__aacid__zlib3_files.metadata AS file_metadata FROM annas_archive_meta__aacid__zlib3_records JOIN annas_archive_meta__aacid__zlib3_files USING (primary_id) WHERE {aac_key} IN %(values)s', { "values": [str(value) for value in values] })
        aac_zlib3_books = cursor.fetchall()
    except Exception as err:
        print(f"Error in get_aac_zlib3_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    aac_zlib3_book_dicts = []
    for zlib_book in aac_zlib3_books:
        aac_zlib3_book_dict = orjson.loads(zlib_book['record_metadata'])
        file_metadata = orjson.loads(zlib_book['file_metadata'])
        aac_zlib3_book_dict['md5'] = file_metadata['md5']
        if 'filesize' in file_metadata:
            aac_zlib3_book_dict['filesize'] = file_metadata['filesize']
        aac_zlib3_book_dict['record_aacid'] = zlib_book['record_aacid']
        aac_zlib3_book_dict['file_aacid'] = zlib_book['file_aacid']
        aac_zlib3_book_dict['file_data_folder'] = zlib_book['file_data_folder']
        aac_zlib3_book_dict['stripped_description'] = strip_description(aac_zlib3_book_dict['description'])
        aac_zlib3_book_dict['language_codes'] = get_bcp47_lang_codes(aac_zlib3_book_dict['language'] or '')
        zlib_add_edition_varia_normalized(aac_zlib3_book_dict)

        allthethings.utils.init_identifiers_and_classification_unified(aac_zlib3_book_dict)
        allthethings.utils.add_identifier_unified(aac_zlib3_book_dict, 'zlib', aac_zlib3_book_dict['zlibrary_id'])
        allthethings.utils.add_isbns_unified(aac_zlib3_book_dict, aac_zlib3_book_dict['isbns'])

        aac_zlib3_book_dicts.append(add_comments_to_dict(aac_zlib3_book_dict, zlib_book_dict_comments))
    return aac_zlib3_book_dicts

@page.get("/db/zlib/<int:zlib_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def zlib_book_json(zlib_id):
    with Session(engine) as session:
        zlib_book_dicts = get_zlib_book_dicts(session, "zlibrary_id", [zlib_id])
        if len(zlib_book_dicts) == 0:
            return "{}", 404
        return nice_json(zlib_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/aac_zlib3/<int:zlib_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def aac_zlib3_book_json(zlib_id):
    with Session(engine) as session:
        aac_zlib3_book_dicts = get_aac_zlib3_book_dicts(session, "zlibrary_id", [zlib_id])
        if len(aac_zlib3_book_dicts) == 0:
            return "{}", 404
        return nice_json(aac_zlib3_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def extract_list_from_ia_json_field(ia_record_dict, key):
    val = ia_record_dict['json'].get('metadata', {}).get(key, [])
    if isinstance(val, str):
        return [val]
    return val

def get_ia_record_dicts(session, key, values):
    if len(values) == 0:
        return []

    seen_ia_ids = set()
    ia_entries = []
    try:
        base_query = select(AaIa202306Metadata, AaIa202306Files, Ia2AcsmpdfFiles).join(AaIa202306Files, AaIa202306Files.ia_id == AaIa202306Metadata.ia_id, isouter=True).join(Ia2AcsmpdfFiles, Ia2AcsmpdfFiles.primary_id == AaIa202306Metadata.ia_id, isouter=True)
        if key.lower() in ['md5']:
            # TODO: we should also consider matching on libgen_md5, but we used to do that before and it had bad SQL performance,
            # when combined in a single query, so we'd have to split it up.
            ia_entries = list(session.execute(
                base_query.where(AaIa202306Files.md5.in_(values))
            ).unique().all()) + list(session.execute(
                base_query.where(Ia2AcsmpdfFiles.md5.in_(values))
            ).unique().all())
        else:
            ia_entries = session.execute(
                base_query.where(getattr(AaIa202306Metadata, key).in_(values))
            ).unique().all()
    except Exception as err:
        print(f"Error in get_ia_record_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    ia_record_dicts = []
    for ia_record, ia_file, ia2_acsmpdf_file in ia_entries:
        ia_record_dict = ia_record.to_dict()

        # TODO: When querying by ia_id we can match multiple files. For now we just pick the first one.
        if ia_record_dict['ia_id'] in seen_ia_ids:
            continue
        seen_ia_ids.add(ia_record_dict['ia_id'])

        ia_record_dict['aa_ia_file'] = None
        if ia_record_dict['libgen_md5'] is None: # If there's a Libgen MD5, then we do NOT serve our IA file.
            if ia_file is not None:
                ia_record_dict['aa_ia_file'] = ia_file.to_dict()
                ia_record_dict['aa_ia_file']['extension'] = 'pdf'
            elif ia2_acsmpdf_file is not None:
                ia2_acsmpdf_file_dict = ia2_acsmpdf_file.to_dict()
                ia2_acsmpdf_file_metadata = orjson.loads(ia2_acsmpdf_file_dict['metadata'])
                ia_record_dict['aa_ia_file'] = {
                    'md5': ia2_acsmpdf_file_dict['md5'],
                    'type': 'ia2_acsmpdf',
                    'filesize': ia2_acsmpdf_file_metadata['filesize'],
                    'ia_id': ia2_acsmpdf_file_dict['primary_id'],
                    'extension': 'pdf',
                    'aacid': ia2_acsmpdf_file_dict['aacid'],
                    'data_folder': ia2_acsmpdf_file_dict['data_folder'],
                }

        ia_record_dict['json'] = orjson.loads(ia_record_dict['json'])

        ia_record_dict['aa_ia_derived'] = {}
        ia_record_dict['aa_ia_derived']['printdisabled_only'] = 'inlibrary' not in ((ia_record_dict['json'].get('metadata') or {}).get('collection') or [])
        ia_record_dict['aa_ia_derived']['original_filename'] = (ia_record_dict['ia_id'] + '.pdf') if ia_record_dict['aa_ia_file'] is not None else None
        ia_record_dict['aa_ia_derived']['cover_url'] = f"https://archive.org/download/{ia_record_dict['ia_id']}/__ia_thumb.jpg"
        ia_record_dict['aa_ia_derived']['title'] = (' '.join(extract_list_from_ia_json_field(ia_record_dict, 'title'))).replace(' : ', ': ')
        ia_record_dict['aa_ia_derived']['author'] = ('; '.join(extract_list_from_ia_json_field(ia_record_dict, 'creator') + extract_list_from_ia_json_field(ia_record_dict, 'associated-names'))).replace(' : ', ': ')
        ia_record_dict['aa_ia_derived']['publisher'] = ('; '.join(extract_list_from_ia_json_field(ia_record_dict, 'publisher'))).replace(' : ', ': ')
        ia_record_dict['aa_ia_derived']['combined_comments'] = '\n\n'.join(extract_list_from_ia_json_field(ia_record_dict, 'notes') + extract_list_from_ia_json_field(ia_record_dict, 'comment') + extract_list_from_ia_json_field(ia_record_dict, 'curation'))
        ia_record_dict['aa_ia_derived']['subjects'] = '\n\n'.join(extract_list_from_ia_json_field(ia_record_dict, 'subject') + extract_list_from_ia_json_field(ia_record_dict, 'level_subject'))
        ia_record_dict['aa_ia_derived']['stripped_description_and_references'] = strip_description('\n\n'.join(extract_list_from_ia_json_field(ia_record_dict, 'description') + extract_list_from_ia_json_field(ia_record_dict, 'references')))
        ia_record_dict['aa_ia_derived']['language_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes(lang) for lang in (extract_list_from_ia_json_field(ia_record_dict, 'language') + extract_list_from_ia_json_field(ia_record_dict, 'ocr_detected_lang'))])
        ia_record_dict['aa_ia_derived']['all_dates'] = list(set(extract_list_from_ia_json_field(ia_record_dict, 'year') + extract_list_from_ia_json_field(ia_record_dict, 'date') + extract_list_from_ia_json_field(ia_record_dict, 'range')))
        ia_record_dict['aa_ia_derived']['longest_date_field'] = max([''] + ia_record_dict['aa_ia_derived']['all_dates'])
        ia_record_dict['aa_ia_derived']['year'] = ''
        for date in ([ia_record_dict['aa_ia_derived']['longest_date_field']] + ia_record_dict['aa_ia_derived']['all_dates']):
            potential_year = re.search(r"(\d\d\d\d)", date)
            if potential_year is not None:
                ia_record_dict['aa_ia_derived']['year'] = potential_year[0]
                break

        ia_record_dict['aa_ia_derived']['content_type'] = 'book_unknown'
        if ia_record_dict['ia_id'].split('_', 1)[0] in ['sim', 'per'] or extract_list_from_ia_json_field(ia_record_dict, 'pub_type') in ["Government Documents", "Historical Journals", "Law Journals", "Magazine", "Magazines", "Newspaper", "Scholarly Journals", "Trade Journals"]:
            ia_record_dict['aa_ia_derived']['content_type'] = 'magazine'

        ia_record_dict['aa_ia_derived']['edition_varia_normalized'] = ', '.join([
            *extract_list_from_ia_json_field(ia_record_dict, 'series'),
            *extract_list_from_ia_json_field(ia_record_dict, 'series_name'),
            *[f"Volume {volume}" for volume in extract_list_from_ia_json_field(ia_record_dict, 'volume')],
            *[f"Issue {issue}" for issue in extract_list_from_ia_json_field(ia_record_dict, 'issue')],
            *extract_list_from_ia_json_field(ia_record_dict, 'edition'),
            *extract_list_from_ia_json_field(ia_record_dict, 'city'),
            ia_record_dict['aa_ia_derived']['longest_date_field']
        ])

        allthethings.utils.init_identifiers_and_classification_unified(ia_record_dict['aa_ia_derived'])
        allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'ocaid', ia_record_dict['ia_id'])
        for item in (extract_list_from_ia_json_field(ia_record_dict, 'openlibrary_edition') + extract_list_from_ia_json_field(ia_record_dict, 'openlibrary_work')):
            allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'ol', item)
        for item in extract_list_from_ia_json_field(ia_record_dict, 'item'):
            allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'lccn', item)

        isbns = extract_list_from_ia_json_field(ia_record_dict, 'isbn')
        for urn in extract_list_from_ia_json_field(ia_record_dict, 'external-identifier'):
            if urn.startswith('urn:oclc:record:'):
                allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'oclc', urn[len('urn:oclc:record:'):])
            elif urn.startswith('urn:oclc:'):
                allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'oclc', urn[len('urn:oclc:'):])
            elif urn.startswith('urn:isbn:'):
                isbns.append(urn[len('urn:isbn:'):])
        allthethings.utils.add_isbns_unified(ia_record_dict['aa_ia_derived'], isbns)

        aa_ia_derived_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "ia_id": ("before", ["This is an Internet Archive record, augmented by Anna's Archive.",
                              "More details at https://annas-archive.org/datasets/ia",
                              "A lot of these fields are explained at https://archive.org/developers/metadata-schema/index.html",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "cover_url": ("before", "Constructed directly from ia_id."),
            "author": ("after", "From `metadata.creator` and `metadata.associated-names`."),
            "combined_comments": ("after", "From `metadata.notes`, `metadata.comment`, and `metadata.curation`."),
            "subjects": ("after", "From `metadata.subject` and `metadata.level_subject`."),
            "stripped_description_and_references": ("after", "From `metadata.description` and `metadata.references`, stripped from HTML tags."),
            "all_dates": ("after", "All potential dates, combined from `metadata.year`, `metadata.date`, and `metadata.range`."),
            "longest_date_field": ("after", "The longest field in `all_dates`."),
            "year": ("after", "Found by applying a \d{4} regex to `longest_date_field`."),
            "content_type": ("after", "Magazines determined by ia_id prefix (like 'sim_' and 'per_') and `metadata.pub_type` field."),
            "edition_varia_normalized": ("after", "From `metadata.series`, `metadata.series_name`, `metadata.volume`, `metadata.issue`, `metadata.edition`, `metadata.city`, and `longest_date_field`."),
        }
        ia_record_dict['aa_ia_derived'] = add_comments_to_dict(ia_record_dict['aa_ia_derived'], aa_ia_derived_comments)


        ia_record_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "ia_id": ("before", ["This is an Internet Archive record, augmented by Anna's Archive.",
                              "More details at https://annas-archive.org/datasets/ia",
                              "A lot of these fields are explained at https://archive.org/developers/metadata-schema/index.html",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "libgen_md5": ("after", "If the metadata refers to a Libgen MD5 from which IA imported, it will be filled in here."),
            "has_thumb": ("after", "Whether Anna's Archive has stored a thumbnail (scraped from __ia_thumb.jpg)."),
            "json": ("before", "The original metadata JSON, scraped from https://archive.org/metadata/<ia_id>.",
                               "We did strip out the full file list, since it's a bit long, and replaced it with a shorter `aa_shorter_files`."),
            "aa_ia_file": ("before", "File metadata, if we have it."),
            "aa_ia_derived": ("before", "Derived metadata."),
        }
        ia_record_dicts.append(add_comments_to_dict(ia_record_dict, ia_record_dict_comments))

    return ia_record_dicts

@page.get("/db/ia/<string:ia_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def ia_record_json(ia_id):
    with Session(engine) as session:
        ia_record_dicts = get_ia_record_dicts(session, "ia_id", [ia_id])
        if len(ia_record_dicts) == 0:
            return "{}", 404
        return nice_json(ia_record_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def extract_ol_str_field(field):
    if field is None:
        return ""
    if type(field) in [str, float, int]:
        return field
    return str(field.get('value')) or ""

def extract_ol_author_field(field):
    if type(field) == str:
        return field
    elif 'author' in field:
        if type(field['author']) == str:
            return field['author']
        elif 'key' in field['author']:
            return field['author']['key']
    elif 'key' in field:
        return field['key']
    return ""

def get_ol_book_dicts(session, key, values):
    if key != 'ol_edition':
        raise Exception(f"Unsupported get_ol_dicts key: {key}")
    if not allthethings.utils.validate_ol_editions(values):
        raise Exception(f"Unsupported get_ol_dicts ol_edition value: {values}")
    if len(values) == 0:
        return []

    with engine.connect() as conn:
        ol_books = conn.execute(select(OlBase).where(OlBase.ol_key.in_([f"/books/{ol_edition}" for ol_edition in values]))).unique().all()

        ol_book_dicts = []
        for ol_book in ol_books:
            ol_book_dict = {
                'ol_edition': ol_book.ol_key.replace('/books/', ''),
                'edition': dict(ol_book),
            }
            ol_book_dict['edition']['json'] = orjson.loads(ol_book_dict['edition']['json'])
            ol_book_dicts.append(ol_book_dict)

        # Load works
        works_ol_keys = []
        for ol_book_dict in ol_book_dicts:
            ol_book_dict['work'] = None
            if 'works' in ol_book_dict['edition']['json'] and len(ol_book_dict['edition']['json']['works']) > 0:
                key = ol_book_dict['edition']['json']['works'][0]['key']
                works_ol_keys.append(key)
        if len(works_ol_keys) > 0:
            ol_works_by_key = {ol_work.ol_key: ol_work for ol_work in conn.execute(select(OlBase).where(OlBase.ol_key.in_(list(set(works_ol_keys))))).all()}
            for ol_book_dict in ol_book_dicts:
                ol_book_dict['work'] = None
                if 'works' in ol_book_dict['edition']['json'] and len(ol_book_dict['edition']['json']['works']) > 0:
                    key = ol_book_dict['edition']['json']['works'][0]['key']
                    if key in ol_works_by_key:
                        ol_book_dict['work'] = dict(ol_works_by_key[key])
                        ol_book_dict['work']['json'] = orjson.loads(ol_book_dict['work']['json'])

        # Load authors
        author_keys = []
        author_keys_by_ol_edition = collections.defaultdict(list)
        for ol_book_dict in ol_book_dicts:
            if 'authors' in ol_book_dict['edition']['json'] and len(ol_book_dict['edition']['json']['authors']) > 0:
                for author in ol_book_dict['edition']['json']['authors']:
                    author_str = extract_ol_author_field(author)
                    if author_str != '' and author_str not in author_keys_by_ol_edition[ol_book_dict['ol_edition']]:
                        author_keys.append(author_str)
                        author_keys_by_ol_edition[ol_book_dict['ol_edition']].append(author_str)
            if ol_book_dict['work'] and 'authors' in ol_book_dict['work']['json']:
                for author in ol_book_dict['work']['json']['authors']:
                    author_str = extract_ol_author_field(author)
                    if author_str != '' and author_str not in author_keys_by_ol_edition[ol_book_dict['ol_edition']]:
                        author_keys.append(author_str)
                        author_keys_by_ol_edition[ol_book_dict['ol_edition']].append(author_str)
            ol_book_dict['authors'] = []

        if len(author_keys) > 0:
            author_keys = list(set(author_keys))
            unredirected_ol_authors = {ol_author.ol_key: ol_author for ol_author in conn.execute(select(OlBase).where(OlBase.ol_key.in_(author_keys))).all()}
            author_redirect_mapping = {}
            for unredirected_ol_author in list(unredirected_ol_authors.values()):
                if unredirected_ol_author.type == '/type/redirect':
                    json = orjson.loads(unredirected_ol_author.json)
                    if 'location' not in json:
                        continue
                    author_redirect_mapping[unredirected_ol_author.ol_key] = json['location']
            redirected_ol_authors = []
            if len(author_redirect_mapping) > 0:
                redirected_ol_authors = {ol_author.ol_key: ol_author for ol_author in conn.execute(select(OlBase).where(OlBase.ol_key.in_([ol_key for ol_key in author_redirect_mapping.values() if ol_key not in author_keys]))).all()}
            for ol_book_dict in ol_book_dicts:
                ol_authors = []
                for author_ol_key in author_keys_by_ol_edition[ol_book_dict['ol_edition']]:
                    if author_ol_key in author_redirect_mapping:
                        remapped_author_ol_key = author_redirect_mapping[author_ol_key]
                        if remapped_author_ol_key in redirected_ol_authors:
                            ol_authors.append(redirected_ol_authors[remapped_author_ol_key])
                        elif remapped_author_ol_key in unredirected_ol_authors:
                            ol_authors.append(unredirected_ol_authors[remapped_author_ol_key])
                    elif author_ol_key in unredirected_ol_authors:
                        ol_authors.append(unredirected_ol_authors[author_ol_key])
                for author in ol_authors:
                    if author.type == '/type/redirect':
                        # Yet another redirect.. this is too much for now, skipping.
                        continue
                    if author.type != '/type/author':
                        print(f"Warning: found author without /type/author: {author}")
                        continue
                    author_dict = dict(author)
                    author_dict['json'] = orjson.loads(author_dict['json'])
                    ol_book_dict['authors'].append(author_dict)

        # Everything else
        for ol_book_dict in ol_book_dicts:
            allthethings.utils.init_identifiers_and_classification_unified(ol_book_dict['edition'])
            allthethings.utils.add_identifier_unified(ol_book_dict['edition'], 'ol', ol_book_dict['ol_edition'])
            allthethings.utils.add_isbns_unified(ol_book_dict['edition'], (ol_book_dict['edition']['json'].get('isbn_10') or []) + (ol_book_dict['edition']['json'].get('isbn_13') or []))
            for item in (ol_book_dict['edition']['json'].get('lc_classifications') or []):
                allthethings.utils.add_classification_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['lc_classifications'], item)
            for item in (ol_book_dict['edition']['json'].get('dewey_decimal_class') or []):
                allthethings.utils.add_classification_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_decimal_class'], item)
            for item in (ol_book_dict['edition']['json'].get('dewey_number') or []):
                allthethings.utils.add_classification_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_number'], item)
            for classification_type, items in (ol_book_dict['edition']['json'].get('classifications') or {}).items():
                if classification_type in allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING:
                    # Sometimes identifiers are incorrectly in the classifications list
                    for item in items:
                        allthethings.utils.add_identifier_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING[classification_type], item)
                    continue
                if classification_type not in allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING:
                    # TODO: Do a scrape / review of all classification types in OL.
                    print(f"Warning: missing classification_type: {classification_type}")
                    continue
                for item in items:
                    allthethings.utils.add_classification_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[classification_type], item)
            if ol_book_dict['work']:
                allthethings.utils.init_identifiers_and_classification_unified(ol_book_dict['work'])
                allthethings.utils.add_identifier_unified(ol_book_dict['work'], 'ol', ol_book_dict['work']['ol_key'].replace('/works/', ''))
                for item in (ol_book_dict['work']['json'].get('lc_classifications') or []):
                    allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['lc_classifications'], item)
                for item in (ol_book_dict['work']['json'].get('dewey_decimal_class') or []):
                    allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_decimal_class'], item)
                for item in (ol_book_dict['work']['json'].get('dewey_number') or []):
                    allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_number'], item)
                for classification_type, items in (ol_book_dict['work']['json'].get('classifications') or {}).items():
                    if classification_type in allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING:
                        # Sometimes identifiers are incorrectly in the classifications list
                        for item in items:
                            allthethings.utils.add_identifier_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING[classification_type], item)
                        continue
                    if classification_type not in allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING:
                        # TODO: Do a scrape / review of all classification types in OL.
                        print(f"Warning: missing classification_type: {classification_type}")
                        continue
                    for item in items:
                        allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[classification_type], item)
            for item in (ol_book_dict['edition']['json'].get('lccn') or []):
                if item is not None:
                    # For some reason there's a bunch of nulls in the raw data here.
                    allthethings.utils.add_identifier_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING['lccn'], item)
            for item in (ol_book_dict['edition']['json'].get('oclc_numbers') or []):
                allthethings.utils.add_identifier_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING['oclc_numbers'], item)
            if 'ocaid' in ol_book_dict['edition']['json']:
                allthethings.utils.add_identifier_unified(ol_book_dict['edition'], 'ocaid', ol_book_dict['edition']['json']['ocaid'])
            for identifier_type, items in (ol_book_dict['edition']['json'].get('identifiers') or {}).items():
                if 'isbn' in identifier_type or identifier_type == 'ean':
                    allthethings.utils.add_isbns_unified(ol_book_dict['edition'], items)
                    continue
                if identifier_type in allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING:
                    # Sometimes classifications are incorrectly in the identifiers list
                    for item in items:
                        allthethings.utils.add_classification_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[identifier_type], item)
                    continue
                if identifier_type not in allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING:
                    # TODO: Do a scrape / review of all identifier types in OL.
                    print(f"Warning: missing identifier_type: {identifier_type}")
                    continue
                for item in items:
                    allthethings.utils.add_identifier_unified(ol_book_dict['edition'], allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING[identifier_type], item)

            ol_book_dict['language_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes((ol_languages.get(lang['key']) or {'name':lang['key']})['name']) for lang in (ol_book_dict['edition']['json'].get('languages') or [])])
            ol_book_dict['translated_from_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes((ol_languages.get(lang['key']) or {'name':lang['key']})['name']) for lang in (ol_book_dict['edition']['json'].get('translated_from') or [])])

            ol_book_dict['identifiers_unified'] = allthethings.utils.merge_unified_fields([ol_book_dict['edition']['identifiers_unified'], (ol_book_dict.get('work') or {'identifiers_unified': {}})['identifiers_unified']])
            ol_book_dict['classifications_unified'] = allthethings.utils.merge_unified_fields([ol_book_dict['edition']['classifications_unified'], (ol_book_dict.get('work') or {'classifications_unified': {}})['classifications_unified']])

            ol_book_dict['cover_url_normalized'] = ''
            if len(ol_book_dict['edition']['json'].get('covers') or []) > 0:
                ol_book_dict['cover_url_normalized'] = f"https://covers.openlibrary.org/b/id/{extract_ol_str_field(ol_book_dict['edition']['json']['covers'][0])}-L.jpg"
            elif ol_book_dict['work'] and len(ol_book_dict['work']['json'].get('covers') or []) > 0:
                ol_book_dict['cover_url_normalized'] = f"https://covers.openlibrary.org/b/id/{extract_ol_str_field(ol_book_dict['work']['json']['covers'][0])}-L.jpg"

            ol_book_dict['title_normalized'] = ''
            if len(ol_book_dict['title_normalized'].strip()) == 0 and 'title' in ol_book_dict['edition']['json']:
                if 'title_prefix' in ol_book_dict['edition']['json']:
                    ol_book_dict['title_normalized'] = extract_ol_str_field(ol_book_dict['edition']['json']['title_prefix']) + " " + extract_ol_str_field(ol_book_dict['edition']['json']['title'])
                else:
                    ol_book_dict['title_normalized'] = extract_ol_str_field(ol_book_dict['edition']['json']['title'])
            if len(ol_book_dict['title_normalized'].strip()) == 0 and ol_book_dict['work'] and 'title' in ol_book_dict['work']['json']:
                ol_book_dict['title_normalized'] = extract_ol_str_field(ol_book_dict['work']['json']['title'])
            if len(ol_book_dict['title_normalized'].strip()) == 0 and len(ol_book_dict['edition']['json'].get('work_titles') or []) > 0:
                ol_book_dict['title_normalized'] = extract_ol_str_field(ol_book_dict['edition']['json']['work_titles'][0])
            if len(ol_book_dict['title_normalized'].strip()) == 0 and len(ol_book_dict['edition']['json'].get('work_titles') or []) > 0:
                ol_book_dict['title_normalized'] = extract_ol_str_field(ol_book_dict['edition']['json']['work_titles'][0])
            ol_book_dict['title_normalized'] = ol_book_dict['title_normalized'].replace(' : ', ': ')

            ol_book_dict['authors_normalized'] = ''
            if len(ol_book_dict['authors_normalized'].strip()) == 0 and 'by_statement' in ol_book_dict['edition']['json']:
                ol_book_dict['authors_normalized'] = extract_ol_str_field(ol_book_dict['edition']['json']['by_statement']).strip()
            if len(ol_book_dict['authors_normalized'].strip()) == 0:
                ol_book_dict['authors_normalized'] = ", ".join([extract_ol_str_field(author['json']['name']) for author in ol_book_dict['authors'] if 'name' in author['json']])

            ol_book_dict['authors_normalized'] = ol_book_dict['authors_normalized'].replace(' ; ', '; ').replace(' , ', ', ')
            if ol_book_dict['authors_normalized'].endswith('.'):
                ol_book_dict['authors_normalized'] = ol_book_dict['authors_normalized'][0:-1]

            ol_book_dict['publishers_normalized'] = (", ".join([extract_ol_str_field(field) for field in ol_book_dict['edition']['json'].get('publishers') or []])).strip()
            if len(ol_book_dict['publishers_normalized']) == 0:
                ol_book_dict['publishers_normalized'] = (", ".join([extract_ol_str_field(field) for field in ol_book_dict['edition']['json'].get('distributors') or []])).strip()

            ol_book_dict['all_dates'] = [item.strip() for item in [
                extract_ol_str_field(ol_book_dict['edition']['json'].get('publish_date')),
                extract_ol_str_field(ol_book_dict['edition']['json'].get('copyright_date')),
                extract_ol_str_field(((ol_book_dict.get('work') or {}).get('json') or {}).get('first_publish_date')),
            ] if item and item.strip() != '']
            ol_book_dict['longest_date_field'] = max([''] + ol_book_dict['all_dates'])

            ol_book_dict['edition_varia_normalized'] = ", ".join([item.strip() for item in [
                *([extract_ol_str_field(field) for field in ol_book_dict['edition']['json'].get('series') or []]),
                extract_ol_str_field(ol_book_dict['edition']['json'].get('edition_name') or ''),
                *([extract_ol_str_field(field) for field in ol_book_dict['edition']['json'].get('publish_places') or []]),
                # TODO: translate?
                allthethings.utils.marc_country_code_to_english(extract_ol_str_field(ol_book_dict['edition']['json'].get('publish_country') or '')),
                ol_book_dict['longest_date_field'],
            ] if item and item.strip() != ''])

            for date in ([ol_book_dict['longest_date_field']] + ol_book_dict['all_dates']):
                potential_year = re.search(r"(\d\d\d\d)", date)
                if potential_year is not None:
                    ol_book_dict['year_normalized'] = potential_year[0]
                    break

            ol_book_dict['stripped_description'] = ''
            if len(ol_book_dict['stripped_description']) == 0 and 'description' in ol_book_dict['edition']['json']:
                ol_book_dict['stripped_description'] = strip_description(extract_ol_str_field(ol_book_dict['edition']['json']['description']))
            if len(ol_book_dict['stripped_description']) == 0 and ol_book_dict['work'] and 'description' in ol_book_dict['work']['json']:
                ol_book_dict['stripped_description'] = strip_description(extract_ol_str_field(ol_book_dict['work']['json']['description']))
            if len(ol_book_dict['stripped_description']) == 0 and 'first_sentence' in ol_book_dict['edition']['json']:
                ol_book_dict['stripped_description'] = strip_description(extract_ol_str_field(ol_book_dict['edition']['json']['first_sentence']))
            if len(ol_book_dict['stripped_description']) == 0 and ol_book_dict['work'] and 'first_sentence' in ol_book_dict['work']['json']:
                ol_book_dict['stripped_description'] = strip_description(extract_ol_str_field(ol_book_dict['work']['json']['first_sentence']))

            ol_book_dict['comments_normalized'] = [item.strip() for item in [
                extract_ol_str_field(ol_book_dict['edition']['json'].get('notes') or ''),
                extract_ol_str_field(((ol_book_dict.get('work') or {}).get('json') or {}).get('notes') or ''),
            ] if item and item.strip() != '']

            # {% for source_record in ol_book_dict.json.source_records %}
            #   <div class="flex odd:bg-[#0000000d] hover:bg-[#0000001a]">
            #     <div class="flex-none w-[150] px-2 py-1">{{ 'Source records' if loop.index0 == 0 else ' ' }}&nbsp;</div>
            #     <div class="px-2 py-1 grow break-words line-clamp-[8]">{{source_record}}</div>
            #     <div class="px-2 py-1 whitespace-nowrap text-right">
            #       <!-- Logic roughly based on https://github.com/internetarchive/openlibrary/blob/e7e8aa5b/openlibrary/templates/history/sources.html#L27 -->
            #       {% if '/' not in source_record and '_meta.mrc:' in source_record %}
            #         <a href="https://openlibrary.org/show-records/ia:{{source_record | split('_') | first}}">url</a></div>
            #       {% else %}
            #         <a href="https://openlibrary.org/show-records/{{source_record | replace('marc:','')}}">url</a></div>
            #       {% endif %}
            #   </div>
            # {% endfor %}

        return ol_book_dicts

@page.get("/db/ol/<string:ol_edition>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def ol_book_json(ol_edition):
    with Session(engine) as session:
        ol_book_dicts = get_ol_book_dicts(session, "ol_edition", [ol_edition])
        if len(ol_book_dicts) == 0:
            return "{}", 404
        return nice_json(ol_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def get_aa_lgli_comics_2022_08_file_dicts(session, key, values):
    if len(values) == 0:
        return []
    aa_lgli_comics_2022_08_files = []
    try:
        aa_lgli_comics_2022_08_files = session.connection().execute(
                select(AaLgliComics202208Files)
                .where(getattr(AaLgliComics202208Files, key).in_(values))
            ).all()
    except Exception as err:
        print(f"Error in get_aa_lgli_comics_2022_08_file_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    aa_lgli_comics_2022_08_file_dicts = [dict(aa_lgli_comics_2022_08_file) for aa_lgli_comics_2022_08_file in aa_lgli_comics_2022_08_files]
    return aa_lgli_comics_2022_08_file_dicts


def get_lgrsnf_book_dicts(session, key, values):
    if len(values) == 0:
        return []

    lgrsnf_books = []
    try:
        # Hack: we explicitly name all the fields, because otherwise some get overwritten below due to lowercasing the column names.
        lgrsnf_books = session.connection().execute(
                select(LibgenrsUpdated, LibgenrsDescription.descr, LibgenrsDescription.toc, LibgenrsHashes.crc32, LibgenrsHashes.edonkey, LibgenrsHashes.aich, LibgenrsHashes.sha1, LibgenrsHashes.tth, LibgenrsHashes.torrent, LibgenrsHashes.btih, LibgenrsHashes.sha256, LibgenrsHashes.ipfs_cid, LibgenrsTopics.topic_descr)
                .join(LibgenrsDescription, LibgenrsUpdated.MD5 == LibgenrsDescription.md5, isouter=True)
                .join(LibgenrsHashes, LibgenrsUpdated.MD5 == LibgenrsHashes.md5, isouter=True)
                .join(LibgenrsTopics, (LibgenrsUpdated.Topic == LibgenrsTopics.topic_id) & (LibgenrsTopics.lang == "en"), isouter=True)
                .where(getattr(LibgenrsUpdated, key).in_(values))
            ).all()
    except Exception as err:
        print(f"Error in get_lgrsnf_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    lgrs_book_dicts = []
    for lgrsnf_book in lgrsnf_books:
        lgrs_book_dict = dict((k.lower(), v) for k,v in dict(lgrsnf_book).items())
        lgrs_book_dict['stripped_description'] = strip_description(lgrs_book_dict.get('descr') or '')
        lgrs_book_dict['language_codes'] = get_bcp47_lang_codes(lgrs_book_dict.get('language') or '')
        lgrs_book_dict['cover_url_normalized'] = f"https://libgen.rs/covers/{lgrs_book_dict['coverurl']}" if len(lgrs_book_dict.get('coverurl') or '') > 0 else ''

        edition_varia_normalized = []
        if len((lgrs_book_dict.get('series') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['series'].strip())
        if len((lgrs_book_dict.get('volume') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['volume'].strip())
        if len((lgrs_book_dict.get('edition') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['edition'].strip())
        if len((lgrs_book_dict.get('periodical') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['periodical'].strip())
        if len((lgrs_book_dict.get('year') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['year'].strip())
        lgrs_book_dict['edition_varia_normalized'] = ', '.join(edition_varia_normalized)

        allthethings.utils.init_identifiers_and_classification_unified(lgrs_book_dict)
        allthethings.utils.add_identifier_unified(lgrs_book_dict, 'lgrsnf', lgrs_book_dict['id'])
        allthethings.utils.add_isbns_unified(lgrs_book_dict, lgrsnf_book.Identifier.split(",") + lgrsnf_book.IdentifierWODash.split(","))
        for name, unified_name in allthethings.utils.LGRS_TO_UNIFIED_IDENTIFIERS_MAPPING.items():
            if name in lgrs_book_dict:
                allthethings.utils.add_identifier_unified(lgrs_book_dict, unified_name, lgrs_book_dict[name])
        for name, unified_name in allthethings.utils.LGRS_TO_UNIFIED_CLASSIFICATIONS_MAPPING.items():
            if name in lgrs_book_dict:
                allthethings.utils.add_classification_unified(lgrs_book_dict, unified_name, lgrs_book_dict[name])

        lgrs_book_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "id": ("before", ["This is a Libgen.rs Non-Fiction record, augmented by Anna's Archive.",
                              "More details at https://annas-archive.org/datasets/libgen_rs",
                              "Most of these fields are explained at https://wiki.mhut.org/content:bibliographic_data",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
        }
        lgrs_book_dicts.append(add_comments_to_dict(lgrs_book_dict, lgrs_book_dict_comments))

    return lgrs_book_dicts


def get_lgrsfic_book_dicts(session, key, values):
    if len(values) == 0:
        return []

    lgrsfic_books = []
    try:
        # Hack: we explicitly name all the fields, because otherwise some get overwritten below due to lowercasing the column names.
        lgrsfic_books = session.connection().execute(
                select(LibgenrsFiction, LibgenrsFictionDescription.Descr, LibgenrsFictionHashes.crc32, LibgenrsFictionHashes.edonkey, LibgenrsFictionHashes.aich, LibgenrsFictionHashes.sha1, LibgenrsFictionHashes.tth, LibgenrsFictionHashes.btih, LibgenrsFictionHashes.sha256, LibgenrsFictionHashes.ipfs_cid)
                .join(LibgenrsFictionDescription, LibgenrsFiction.MD5 == LibgenrsFictionDescription.MD5, isouter=True)
                .join(LibgenrsFictionHashes, LibgenrsFiction.MD5 == LibgenrsFictionHashes.md5, isouter=True)
                .where(getattr(LibgenrsFiction, key).in_(values))
            ).all()
    except Exception as err:
        print(f"Error in get_lgrsfic_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    lgrs_book_dicts = []

    for lgrsfic_book in lgrsfic_books:
        lgrs_book_dict = dict((k.lower(), v) for k,v in dict(lgrsfic_book).items())
        lgrs_book_dict['stripped_description'] = strip_description(lgrs_book_dict.get('descr') or '')
        lgrs_book_dict['language_codes'] = get_bcp47_lang_codes(lgrs_book_dict.get('language') or '')
        lgrs_book_dict['cover_url_normalized'] = f"https://libgen.rs/fictioncovers/{lgrs_book_dict['coverurl']}" if len(lgrs_book_dict.get('coverurl') or '') > 0 else ''

        edition_varia_normalized = []
        if len((lgrs_book_dict.get('series') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['series'].strip())
        if len((lgrs_book_dict.get('edition') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['edition'].strip())
        if len((lgrs_book_dict.get('year') or '').strip()) > 0:
            edition_varia_normalized.append(lgrs_book_dict['year'].strip())
        lgrs_book_dict['edition_varia_normalized'] = ', '.join(edition_varia_normalized)

        allthethings.utils.init_identifiers_and_classification_unified(lgrs_book_dict)
        allthethings.utils.add_identifier_unified(lgrs_book_dict, 'lgrsfic', lgrs_book_dict['id'])
        allthethings.utils.add_isbns_unified(lgrs_book_dict, lgrsfic_book.Identifier.split(","))
        for name, unified_name in allthethings.utils.LGRS_TO_UNIFIED_IDENTIFIERS_MAPPING.items():
            if name in lgrs_book_dict:
                allthethings.utils.add_identifier_unified(lgrs_book_dict, unified_name, lgrs_book_dict[name])
        for name, unified_name in allthethings.utils.LGRS_TO_UNIFIED_CLASSIFICATIONS_MAPPING.items():
            if name in lgrs_book_dict:
                allthethings.utils.add_classification_unified(lgrs_book_dict, unified_name, lgrs_book_dict[name])


        lgrs_book_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "id": ("before", ["This is a Libgen.rs Fiction record, augmented by Anna's Archive.",
                              "More details at https://annas-archive.org/datasets/libgen_rs",
                              "Most of these fields are explained at https://wiki.mhut.org/content:bibliographic_data",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
        }
        lgrs_book_dicts.append(add_comments_to_dict(lgrs_book_dict, lgrs_book_dict_comments))

    return lgrs_book_dicts


@page.get("/db/lgrs/nf/<int:lgrsnf_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def lgrsnf_book_json(lgrsnf_book_id):
    with Session(engine) as session:
        lgrs_book_dicts = get_lgrsnf_book_dicts(session, "ID", [lgrsnf_book_id])
        if len(lgrs_book_dicts) == 0:
            return "{}", 404
        return nice_json(lgrs_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}
@page.get("/db/lgrs/fic/<int:lgrsfic_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def lgrsfic_book_json(lgrsfic_book_id):
    with Session(engine) as session:
        lgrs_book_dicts = get_lgrsfic_book_dicts(session, "ID", [lgrsfic_book_id])
        if len(lgrs_book_dicts) == 0:
            return "{}", 404
        return nice_json(lgrs_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

libgenli_elem_descr_output = None
def libgenli_elem_descr(conn):
    global libgenli_elem_descr_output
    if libgenli_elem_descr_output is None:
        all_descr = conn.execute(select(LibgenliElemDescr).limit(10000)).all()
        output = {}
        for descr in all_descr:
            output[descr.key] = dict(descr)
        libgenli_elem_descr_output = output
    return libgenli_elem_descr_output

def lgli_normalize_meta_field(field_name):
    return field_name.lower().replace(' ', '').replace('-', '').replace('.', '').replace('/', '').replace('(','').replace(')', '')

def lgli_map_descriptions(descriptions):
    descrs_mapped = {}
    for descr in descriptions:
        normalized_base_field = lgli_normalize_meta_field(descr['meta']['name_en'])
        normalized_base_field_meta = '///' + normalized_base_field
        if normalized_base_field_meta not in descrs_mapped:
            meta_dict_comments = {
                "link_pattern": ("after", ["Relative links are relative to the Libgen.li domains, e.g. https://libgen.li"]),
            }
            descrs_mapped[normalized_base_field_meta] = {
                "libgenli": add_comments_to_dict({k: v for k, v in descr['meta'].items() if v and v != "" and v != 0}, meta_dict_comments),
            }
            if normalized_base_field in allthethings.utils.LGLI_IDENTIFIERS:
                descrs_mapped[normalized_base_field_meta]["annas_archive"] = allthethings.utils.LGLI_IDENTIFIERS[normalized_base_field]
            # LGLI_IDENTIFIERS and LGLI_CLASSIFICATIONS are non-overlapping
            if normalized_base_field in allthethings.utils.LGLI_CLASSIFICATIONS:
                descrs_mapped[normalized_base_field_meta]["annas_archive"] = allthethings.utils.LGLI_CLASSIFICATIONS[normalized_base_field]
        if normalized_base_field in descrs_mapped:
            descrs_mapped[normalized_base_field].append(descr['value'])
        else:
            descrs_mapped[normalized_base_field] = [descr['value']]
        for i in [1,2,3]:
            add_field_name = f"name_add{i}_en"
            add_field_value = f"value_add{i}"
            if len(descr['meta'][add_field_name]) > 0:
                normalized_add_field = normalized_base_field + "_" + lgli_normalize_meta_field(descr['meta'][add_field_name])
                if normalized_add_field in descrs_mapped:
                    descrs_mapped[normalized_add_field].append(descr[add_field_value])
                else:
                    descrs_mapped[normalized_add_field] = [descr[add_field_value]]
        if len(descr.get('publisher_title') or '') > 0:
            normalized_base_field = 'publisher_title'
            normalized_base_field_meta = '///' + normalized_base_field
            if normalized_base_field_meta not in descrs_mapped:
                descrs_mapped[normalized_base_field_meta] = "Publisher title is a virtual field added by Anna's Archive based on the `publishers` table and the value of `publisherid`."
            if normalized_base_field in descrs_mapped:
                descrs_mapped[normalized_base_field].append(descr['publisher_title'])
            else:
                descrs_mapped[normalized_base_field] = [descr['publisher_title']]

    return descrs_mapped



# See https://libgen.li/community/app.php/article/new-database-structure-published-o%CF%80y6%D0%BB%D0%B8%C4%B8o%D0%B2a%D0%BDa-%D0%BDo%D0%B2a%D1%8F-c%D1%82py%C4%B8%D1%82ypa-6a%D0%B7%C6%85i-%D0%B4a%D0%BD%D0%BD%C6%85ix
def get_lgli_file_dicts(session, key, values):
    if len(values) == 0:
        return []

    description_metadata = libgenli_elem_descr(session.connection())

    lgli_files = session.scalars(
        select(LibgenliFiles)
            .where(getattr(LibgenliFiles, key).in_(values))
            .options(
                defaultload("add_descrs").load_only("key", "value", "value_add1", "value_add2", "value_add3"),
                defaultload("editions.add_descrs").load_only("key", "value", "value_add1", "value_add2", "value_add3"),
                defaultload("editions.series").load_only("title", "publisher", "volume", "volume_name"),
                defaultload("editions.series.issn_add_descrs").load_only("value"),
                defaultload("editions.add_descrs.publisher").load_only("title"),
            )
    ).all()

    lgli_file_dicts = []
    for lgli_file in lgli_files:
        lgli_file_dict = lgli_file.to_dict()
        lgli_file_descriptions_dict = [{**descr.to_dict(), 'meta': description_metadata[descr.key]} for descr in lgli_file.add_descrs]
        lgli_file_dict['descriptions_mapped'] = lgli_map_descriptions(lgli_file_descriptions_dict)
        lgli_file_dict['editions'] = []

        for edition in lgli_file.editions:
            edition_dict = {
                **edition.to_dict(),
                'issue_series_title': edition.series.title if edition.series else '',
                'issue_series_publisher': edition.series.publisher if edition.series else '',
                'issue_series_volume_number': edition.series.volume if edition.series else '',
                'issue_series_volume_name': edition.series.volume_name if edition.series else '',
                'issue_series_issn': edition.series.issn_add_descrs[0].value if edition.series and edition.series.issn_add_descrs else '',
            }

            edition_dict['descriptions_mapped'] = lgli_map_descriptions({
                **descr.to_dict(),
                'meta': description_metadata[descr.key],
                'publisher_title': descr.publisher[0].title if len(descr.publisher) > 0 else '',
            } for descr in edition.add_descrs)
            edition_dict['authors_normalized'] = edition_dict['author'].strip()
            if len(edition_dict['authors_normalized']) == 0 and len(edition_dict['descriptions_mapped'].get('author') or []) > 0:
                edition_dict['authors_normalized'] = ", ".join(author.strip() for author in edition_dict['descriptions_mapped']['author'])

            edition_dict['cover_url_guess'] = edition_dict['cover_url']
            coverurls = edition_dict['descriptions_mapped'].get('coverurl') or []
            if (len(coverurls) > 0) and (len(coverurls[0]) > 0):
                edition_dict['cover_url_guess'] = coverurls[0]
            if edition_dict['cover_exists'] > 0:
                edition_dict['cover_url_guess'] = f"https://libgen.li/editioncovers/{(edition_dict['e_id'] // 1000) * 1000}/{edition_dict['e_id']}.jpg"

            issue_other_fields = dict((key, edition_dict[key]) for key in allthethings.utils.LGLI_ISSUE_OTHER_FIELDS if edition_dict[key] not in ['', '0', 0, None])
            if len(issue_other_fields) > 0:
                edition_dict['issue_other_fields_json'] = nice_json(issue_other_fields)
            standard_info_fields = dict((key, edition_dict['descriptions_mapped'][key]) for key in allthethings.utils.LGLI_STANDARD_INFO_FIELDS if edition_dict['descriptions_mapped'].get(key) not in ['', '0', 0, None])
            if len(standard_info_fields) > 0:
                edition_dict['standard_info_fields_json'] = nice_json(standard_info_fields)
            date_info_fields = dict((key, edition_dict['descriptions_mapped'][key]) for key in allthethings.utils.LGLI_DATE_INFO_FIELDS if edition_dict['descriptions_mapped'].get(key) not in ['', '0', 0, None])
            if len(date_info_fields) > 0:
                edition_dict['date_info_fields_json'] = nice_json(date_info_fields)

            issue_series_title_normalized = []
            if len((edition_dict['issue_series_title'] or '').strip()) > 0:
                issue_series_title_normalized.append(edition_dict['issue_series_title'].strip())
            if len((edition_dict['issue_series_volume_name'] or '').strip()) > 0:
                issue_series_title_normalized.append(edition_dict['issue_series_volume_name'].strip())
            if len((edition_dict['issue_series_volume_number'] or '').strip()) > 0:
                issue_series_title_normalized.append('Volume ' + edition_dict['issue_series_volume_number'].strip())
            elif len((issue_other_fields.get('issue_year_number') or '').strip()) > 0:
                issue_series_title_normalized.append('#' + issue_other_fields['issue_year_number'].strip())
            edition_dict['issue_series_title_normalized'] = ", ".join(issue_series_title_normalized) if len(issue_series_title_normalized) > 0 else ''

            publisher_titles = (edition_dict['descriptions_mapped'].get('publisher_title') or [])
            edition_dict['publisher_normalized'] = ''
            if len((edition_dict['publisher'] or '').strip()) > 0:
                edition_dict['publisher_normalized'] = edition_dict['publisher'].strip()
            elif len(publisher_titles) > 0 and len(publisher_titles[0].strip()) > 0:
                edition_dict['publisher_normalized'] = publisher_titles[0].strip()
            elif len((edition_dict['issue_series_publisher'] or '').strip()) > 0:
                edition_dict['publisher_normalized'] = edition_dict['issue_series_publisher'].strip()
                if len((edition_dict['issue_series_issn'] or '').strip()) > 0:
                    edition_dict['publisher_normalized'] += ' (ISSN ' + edition_dict['issue_series_issn'].strip() + ')'

            date_normalized = []
            if len((edition_dict['year'] or '').strip()) > 0:
                date_normalized.append(edition_dict['year'].strip())
            if len((edition_dict['month'] or '').strip()) > 0:
                date_normalized.append(edition_dict['month'].strip())
            if len((edition_dict['day'] or '').strip()) > 0:
                date_normalized.append(edition_dict['day'].strip())
            edition_dict['date_normalized'] = " ".join(date_normalized)

            edition_varia_normalized = []
            if len((edition_dict['issue_series_title_normalized'] or '').strip()) > 0:
                edition_varia_normalized.append(edition_dict['issue_series_title_normalized'].strip())
            if len((edition_dict['issue_number'] or '').strip()) > 0:
                edition_varia_normalized.append('#' + edition_dict['issue_number'].strip())
            if len((edition_dict['issue_year_number'] or '').strip()) > 0:
                edition_varia_normalized.append('#' + edition_dict['issue_year_number'].strip())
            if len((edition_dict['issue_volume'] or '').strip()) > 0:
                edition_varia_normalized.append(edition_dict['issue_volume'].strip())
            if (len((edition_dict['issue_first_page'] or '').strip()) > 0) or (len((edition_dict['issue_last_page'] or '').strip()) > 0):
                edition_varia_normalized.append('pages ' + (edition_dict['issue_first_page'] or '').strip() + '-' + (edition_dict['issue_last_page'] or '').strip())
            if len((edition_dict['series_name'] or '').strip()) > 0:
                edition_varia_normalized.append(edition_dict['series_name'].strip())
            if len((edition_dict['edition'] or '').strip()) > 0:
                edition_varia_normalized.append(edition_dict['edition'].strip())
            if len((edition_dict['date_normalized'] or '').strip()) > 0:
                edition_varia_normalized.append(edition_dict['date_normalized'].strip())
            edition_dict['edition_varia_normalized'] = ', '.join(edition_varia_normalized)

            language_codes = [get_bcp47_lang_codes(language_code) for language_code in (edition_dict['descriptions_mapped'].get('language') or [])]
            edition_dict['language_codes'] = combine_bcp47_lang_codes(language_codes)
            languageoriginal_codes = [get_bcp47_lang_codes(language_code) for language_code in (edition_dict['descriptions_mapped'].get('languageoriginal') or [])]
            edition_dict['languageoriginal_codes'] = combine_bcp47_lang_codes(languageoriginal_codes)

            allthethings.utils.init_identifiers_and_classification_unified(edition_dict)
            allthethings.utils.add_identifier_unified(edition_dict, 'doi', edition_dict['doi'])
            for key, values in edition_dict['descriptions_mapped'].items():
                if key in allthethings.utils.LGLI_IDENTIFIERS:
                    for value in values:
                        allthethings.utils.add_identifier_unified(edition_dict, allthethings.utils.LGLI_IDENTIFIERS_MAPPING.get(key, key), value)
            for key, values in edition_dict['descriptions_mapped'].items():
                if key in allthethings.utils.LGLI_CLASSIFICATIONS:
                    for value in values:
                        allthethings.utils.add_classification_unified(edition_dict, allthethings.utils.LGLI_CLASSIFICATIONS_MAPPING.get(key, key), value)
            allthethings.utils.add_isbns_unified(edition_dict, edition_dict['descriptions_mapped'].get('isbn') or [])

            edition_dict['stripped_description'] = ''
            if len(edition_dict['descriptions_mapped'].get('description') or []) > 0:
                edition_dict['stripped_description'] = strip_description("\n\n".join(edition_dict['descriptions_mapped']['description']))

            edition_dict['edition_type_full'] = allthethings.utils.LGLI_EDITION_TYPE_MAPPING.get(edition_dict['type'], '')

            edition_dict_comments = {
                **allthethings.utils.COMMON_DICT_COMMENTS,
                "editions": ("before", ["Files can be associated with zero or more editions."
                                        "Sometimes it corresponds to a particular physical version of a book (similar to ISBN records, or 'editions' in Open Library), but it may also represent a chapter in a periodical (more specific than a single book), or a collection of multiple books (more general than a single book). However, in practice, in most cases files only have a single edition.",
                                        "Note that while usually there is only one 'edition' associated with a file, it is common to have multiple files associated with an edition. For example, different people might have scanned a book."]),
                "issue_series_title": ("before", ["The `issue_series_*` fields were loaded from the `series` table using `issue_s_id`."]),
                "authors_normalized": ("before", ["Anna's Archive best guess at the authors, based on the regular `author` field and `author` from `descriptions_mapped`."]),
                "cover_url_guess": ("before", ["Anna's Archive best guess at the full URL to the cover image on libgen.li, for this specific edition."]),
                "issue_series_title_normalized": ("before", ["Anna's Archive version of the 'issue_series_title', 'issue_series_volume_name', 'issue_series_volume_number', and 'issue_year_number' fields; combining them into a single field for display and search."]),
                "publisher_normalized": ("before", ["Anna's Archive version of the 'publisher', 'publisher_title_first', 'issue_series_publisher', and 'issue_series_issn' fields; combining them into a single field for display and search."]),
                "date_normalized": ("before", ["Anna's Archive combined version of the 'year', 'month', and 'day' fields."]),
                "edition_varia_normalized": ("before", ["Anna's Archive version of the 'issue_series_title_normalized', 'issue_number', 'issue_year_number', 'issue_volume', 'issue_first_page', 'issue_last_page', 'series_name', 'edition', and 'date_normalized' fields; combining them into a single field for display and search."]),
                "language_codes": ("before", ["Anna's Archive version of the 'language' field, where we attempted to parse them into BCP 47 tags."]),
                "languageoriginal_codes": ("before", ["Same as 'language_codes' but for the 'languageoriginal' field, which contains the original language if the work is a translation."]),
                "edition_type_full": ("after", ["Anna's Archive expansion of the `type` field in the edition, based on the `descr_elems` table."]),
            }
            lgli_file_dict['editions'].append(add_comments_to_dict(edition_dict, edition_dict_comments))

        lgli_file_dict['cover_url_guess'] = ''
        if lgli_file_dict['cover_exists'] > 0:
            lgli_file_dict['cover_url_guess'] = f"https://libgen.li/comicscovers/{lgli_file_dict['md5'].lower()}.jpg"
            if lgli_file_dict['libgen_id'] and lgli_file_dict['libgen_id'] > 0:
                lgli_file_dict['cover_url_guess'] = f"https://libgen.li/covers/{(lgli_file_dict['libgen_id'] // 1000) * 1000}/{lgli_file_dict['md5'].lower()}.jpg"
            if lgli_file_dict['comics_id'] and lgli_file_dict['comics_id'] > 0:
                lgli_file_dict['cover_url_guess'] = f"https://libgen.li/comicscovers_repository/{(lgli_file_dict['comics_id'] // 1000) * 1000}/{lgli_file_dict['md5'].lower()}.jpg"
            if lgli_file_dict['fiction_id'] and lgli_file_dict['fiction_id'] > 0:
                lgli_file_dict['cover_url_guess'] = f"https://libgen.li/fictioncovers/{(lgli_file_dict['fiction_id'] // 1000) * 1000}/{lgli_file_dict['md5'].lower()}.jpg"
            if lgli_file_dict['fiction_rus_id'] and lgli_file_dict['fiction_rus_id'] > 0:
                lgli_file_dict['cover_url_guess'] = f"https://libgen.li/fictionruscovers/{(lgli_file_dict['fiction_rus_id'] // 1000) * 1000}/{lgli_file_dict['md5'].lower()}.jpg"
            if lgli_file_dict['magz_id'] and lgli_file_dict['magz_id'] > 0:
                lgli_file_dict['cover_url_guess'] = f"https://libgen.li/magzcovers/{(lgli_file_dict['magz_id'] // 1000) * 1000}/{lgli_file_dict['md5'].lower()}.jpg"

        lgli_file_dict['cover_url_guess_normalized'] = ''
        if len(lgli_file_dict['cover_url_guess']) > 0:
            lgli_file_dict['cover_url_guess_normalized'] = lgli_file_dict['cover_url_guess']
        else:
            for edition_dict in lgli_file_dict['editions']:
                if len(edition_dict['cover_url_guess']) > 0:
                    lgli_file_dict['cover_url_guess_normalized'] = edition_dict['cover_url_guess']

        lgli_file_dict['scimag_url_guess'] = ''
        if len(lgli_file_dict['scimag_archive_path']) > 0:
            lgli_file_dict['scimag_url_guess'] = lgli_file_dict['scimag_archive_path'].replace('\\', '/')
            if lgli_file_dict['scimag_url_guess'].endswith('.' + lgli_file_dict['extension']):
                lgli_file_dict['scimag_url_guess'] = lgli_file_dict['scimag_url_guess'][0:-len('.' + lgli_file_dict['extension'])]
            if lgli_file_dict['scimag_url_guess'].startswith('10.0000/') and '%2F' in lgli_file_dict['scimag_url_guess']:
                lgli_file_dict['scimag_url_guess'] = 'http://' + lgli_file_dict['scimag_url_guess'][len('10.0000/'):].replace('%2F', '/')
            else:
                lgli_file_dict['scimag_url_guess'] = 'https://doi.org/' + lgli_file_dict['scimag_url_guess']

        allthethings.utils.init_identifiers_and_classification_unified(lgli_file_dict)
        allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli', lgli_file_dict['f_id'])
        lgli_file_dict['scimag_archive_path_decoded'] = urllib.parse.unquote(lgli_file_dict['scimag_archive_path'].replace('\\', '/'))
        potential_doi_scimag_archive_path = lgli_file_dict['scimag_archive_path_decoded']
        if potential_doi_scimag_archive_path.endswith('.pdf'):
            potential_doi_scimag_archive_path = potential_doi_scimag_archive_path[:-len('.pdf')]
        potential_doi_scimag_archive_path = normalize_doi(potential_doi_scimag_archive_path)
        if potential_doi_scimag_archive_path != '':
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'doi', potential_doi_scimag_archive_path)


        lgli_file_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "f_id": ("before", ["This is a Libgen.li file record, augmented by Anna's Archive.",
                     "More details at https://annas-archive.org/datasets/libgen_li",
                     "Most of these fields are explained at https://libgen.li/community/app.php/article/new-database-structure-published-o%CF%80y6%D0%BB%D0%B8%C4%B8o%D0%B2a%D0%BDa-%D0%BDo%D0%B2a%D1%8F-c%D1%82py%C4%B8%D1%82ypa-6a%D0%B7%C6%85i-%D0%B4a%D0%BD%D0%BD%C6%85ix",
                     "The source URL is https://libgen.li/file.php?id=<f_id>",
                     allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "cover_url_guess": ("after", ["Anna's Archive best guess at the full URL to the cover image on libgen.li, for this specific file (not taking into account editions)."]),
            "cover_url_guess_normalized": ("after", ["Anna's Archive best guess at the full URL to the cover image on libgen.li, using the guess from the first edition that has a non-empty guess, if the file-specific guess is empty."]),
            "scimag_url_guess": ("after", ["Anna's Archive best guess at the canonical URL for journal articles."]),
            "scimag_archive_path_decoded": ("after", ["scimag_archive_path but with URL decoded"]),
            "libgen_topic": ("after", ["The primary subcollection this file belongs to: l=Non-fiction ('libgen'), s=Standards document, m=Magazine, c=Comic, f=Fiction, r=Russian Fiction, a=Journal article (Sci-Hub/scimag)"]),
        }
        lgli_file_dicts.append(add_comments_to_dict(lgli_file_dict, lgli_file_dict_comments))

    return lgli_file_dicts


@page.get("/db/lgli/file/<int:lgli_file_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def lgli_file_json(lgli_file_id):
    with Session(engine) as session:
        lgli_file_dicts = get_lgli_file_dicts(session, "f_id", [lgli_file_id])
        if len(lgli_file_dicts) == 0:
            return "{}", 404
        return nice_json(lgli_file_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def get_isbndb_dicts(session, canonical_isbn13s):
    if len(canonical_isbn13s) == 0:
        return []

    isbndb13_grouped = collections.defaultdict(list)
    for row in session.connection().execute(select(IsbndbIsbns).where(IsbndbIsbns.isbn13.in_(canonical_isbn13s))).all():
        isbndb13_grouped[row['isbn13']].append(row)
    isbndb10_grouped = collections.defaultdict(list)
    isbn10s = list(filter(lambda x: x is not None, [isbnlib.to_isbn10(isbn13) for isbn13 in canonical_isbn13s]))
    if len(isbn10s) > 0:
        for row in session.connection().execute(select(IsbndbIsbns).where(IsbndbIsbns.isbn10.in_(isbn10s))).all():
            # ISBNdb has a bug where they just chop off the prefix of ISBN-13, which is incorrect if the prefix is anything
            # besides "978"; so we double-check on this.
            if row['isbn13'][0:3] == '978':
                isbndb10_grouped[row['isbn10']].append(row)

    isbn_dicts = []
    for canonical_isbn13 in canonical_isbn13s:
        isbn13_mask = isbnlib.mask(canonical_isbn13)
        isbn_dict = {
            "ean13": isbnlib.ean13(canonical_isbn13),
            "isbn10": isbnlib.to_isbn10(canonical_isbn13),
        }

        isbndb_books = {}
        if isbn_dict['isbn10']:
            isbndb10_all = isbndb10_grouped[isbn_dict['isbn10']]
            for isbndb10 in isbndb10_all:
                isbndb_books[isbndb10['isbn13'] + '-' + isbndb10['isbn10']] = { **isbndb10, 'source_isbn': isbn_dict['isbn10'], 'matchtype': 'ISBN-10' }
        isbndb13_all = isbndb13_grouped[canonical_isbn13]
        for isbndb13 in isbndb13_all:
            key = isbndb13['isbn13'] + '-' + isbndb13['isbn10']
            if key in isbndb_books:
                isbndb_books[key]['matchtype'] = 'ISBN-10 and ISBN-13'
            else:
                isbndb_books[key] = { **isbndb13, 'source_isbn': canonical_isbn13, 'matchtype': 'ISBN-13' }

        for isbndb_book in isbndb_books.values():
            isbndb_book['json'] = orjson.loads(isbndb_book['json'])
            isbndb_book['json']['subjects'] = isbndb_book['json'].get('subjects', None) or []

        # There seem to be a bunch of ISBNdb books with only a language, which is not very useful.
        isbn_dict['isbndb'] = [isbndb_book for isbndb_book in isbndb_books.values() if len(isbndb_book['json'].get('title') or '') > 0 or len(isbndb_book['json'].get('title_long') or '') > 0 or len(isbndb_book['json'].get('authors') or []) > 0 or len(isbndb_book['json'].get('synopsis') or '') > 0 or len(isbndb_book['json'].get('overview') or '') > 0]

        for index, isbndb_dict in enumerate(isbn_dict['isbndb']):
            isbndb_dict['language_codes'] = get_bcp47_lang_codes(isbndb_dict['json'].get('language') or '')
            isbndb_dict['edition_varia_normalized'] = ", ".join(list(set([item for item in [
                str(isbndb_dict['json'].get('edition') or '').strip(),
                str(isbndb_dict['json'].get('date_published') or '').split('T')[0].strip(),
            ] if item != ''])))
            isbndb_dict['title_normalized'] = max([isbndb_dict['json'].get('title') or '', isbndb_dict['json'].get('title_long') or ''], key=len)
            isbndb_dict['year_normalized'] = ''
            potential_year = re.search(r"(\d\d\d\d)", str(isbndb_dict['json'].get('date_published') or '').split('T')[0])
            if potential_year is not None:
                isbndb_dict['year_normalized'] = potential_year[0]
            # There is often also isbndb_dict['json']['image'], but sometimes images get added later, so we can make a guess ourselves.
            isbndb_dict['cover_url_guess'] = f"https://images.isbndb.com/covers/{isbndb_dict['isbn13'][-4:-2]}/{isbndb_dict['isbn13'][-2:]}/{isbndb_dict['isbn13']}.jpg"

            allthethings.utils.init_identifiers_and_classification_unified(isbndb_dict)
            allthethings.utils.add_isbns_unified(isbndb_dict, [canonical_isbn13])

            isbndb_inner_comments = {
                "edition_varia_normalized": ("after", ["Anna's Archive version of the 'edition', and 'date_published' fields; combining them into a single field for display and search."]),
                "title_normalized": ("after", ["Anna's Archive version of the 'title', and 'title_long' fields; we take the longest of the two."]),
                "json": ("before", ["Raw JSON straight from the ISBNdb API."]),
                "cover_url_guess": ("after", ["Anna's Archive best guess of the cover URL, since sometimes the 'image' field is missing from the JSON."]),
                "year_normalized": ("after", ["Anna's Archive version of the year of publication, by extracting it from the 'date_published' field."]),
                "language_codes": ("before", ["Anna's Archive version of the 'language' field, where we attempted to parse them into BCP 47 tags."]),
                "matchtype": ("after", ["Whether the canonical ISBN-13 matched the API's ISBN-13, ISBN-10, or both."]),
            }
            isbn_dict['isbndb'][index] = add_comments_to_dict(isbn_dict['isbndb'][index], isbndb_inner_comments)

        isbndb_wrapper_comments = {
            "ean13": ("before", ["Metadata from our ISBNdb collection, augmented by Anna's Archive.",
                               "More details at https://annas-archive.org/datasets",
                               allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "isbndb": ("before", ["All matching records from the ISBNdb database."]),
        }
        isbn_dicts.append(add_comments_to_dict(isbn_dict, isbndb_wrapper_comments))

    return isbn_dicts

@page.get("/db/isbndb/<string:isbn>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def isbndb_json(isbn):
    with Session(engine) as session:
        isbndb_dicts = get_isbndb_dicts(session, [isbn])
        if len(isbndb_dicts) == 0:
            return "{}", 404
        return nice_json(isbndb_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}


def get_scihub_doi_dicts(session, key, values):
    if len(values) == 0:
        return []
    if key != 'doi':
        raise Exception(f"Unexpected 'key' in get_scihub_doi_dicts: '{key}'")

    scihub_dois = []
    try:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute(f'SELECT doi FROM scihub_dois WHERE doi IN %(values)s', { "values": [str(value) for value in values] })
        scihub_dois = cursor.fetchall()
    except Exception as err:
        print(f"Error in get_scihub_doi_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    scihub_doi_dicts = []
    for scihub_doi in scihub_dois:
        scihub_doi_dict = { "doi": scihub_doi["doi"] }
        allthethings.utils.init_identifiers_and_classification_unified(scihub_doi_dict)
        allthethings.utils.add_identifier_unified(scihub_doi_dict, "doi", scihub_doi_dict["doi"])
        scihub_doi_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "doi": ("before", ["This is a file from Sci-Hub's dois-2022-02-12.7z dataset.",
                              "More details at https://annas-archive.org/datasets/scihub",
                              "The source URL is https://sci-hub.ru/datasets/dois-2022-02-12.7z",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
        }
        scihub_doi_dicts.append(add_comments_to_dict(scihub_doi_dict, scihub_doi_dict_comments))
    return scihub_doi_dicts

@page.get("/db/scihub_doi/<path:doi>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def scihub_doi_json(doi):
    with Session(engine) as session:
        scihub_doi_dicts = get_scihub_doi_dicts(session, 'doi', [doi])
        if len(scihub_doi_dicts) == 0:
            return "{}", 404
        return nice_json(scihub_doi_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}


def oclc_get_authors_from_contributors(contributors):
    has_primary = any(contributor['isPrimary'] for contributor in contributors)
    has_author_relator = any('aut' in (contributor.get('relatorCodes') or []) for contributor in contributors)
    authors = []
    for contributor in contributors:
        author = []
        if has_primary and (not contributor['isPrimary']):
            continue
        if has_author_relator and ('aut' not in (contributor.get('relatorCodes') or [])):
            continue
        if 'nonPersonName' in contributor:
            author = [contributor['nonPersonName'].get('text') or '']
        else:
            author = [((contributor.get('firstName') or {}).get('text') or ''), ((contributor.get('secondName') or {}).get('text') or '')]

        author_full = ' '.join(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in author]))
        if len(author_full) > 0:
            authors.append(author_full)
    return "; ".join(authors)

def oclc_get_authors_from_authors(authors):
    contributors = []
    for author in authors:
        contributors.append({
            'firstName': {'text': (author['firstNameObject'].get('data') or '')},
            'secondName': {'text': ', '.join(filter(len, [(author['lastNameObject'].get('data') or ''), (author.get('notes') or '')]))},
            'isPrimary': author['primary'],
            'relatorCodes': [(relator.get('code') or '') for relator in (author.get('relatorList') or {'relators':[]})['relators']],
        })
    return oclc_get_authors_from_contributors(contributors)

def get_oclc_dicts(session, key, values):
    if len(values) == 0:
        return []
    if key != 'oclc':
        raise Exception(f"Unexpected 'key' in get_oclc_dicts: '{key}'")

    oclc_dicts = []
    for oclc_id in values:
        aac_records = allthethings.utils.get_worldcat_records(oclc_id)

        oclc_dict = {}
        oclc_dict["oclc_id"] = oclc_id
        oclc_dict["aa_oclc_derived"] = {}
        oclc_dict["aa_oclc_derived"]["title_multiple"] = []
        oclc_dict["aa_oclc_derived"]["author_multiple"] = []
        oclc_dict["aa_oclc_derived"]["publisher_multiple"] = []
        oclc_dict["aa_oclc_derived"]["edition_multiple"] = []
        oclc_dict["aa_oclc_derived"]["place_multiple"] = []
        oclc_dict["aa_oclc_derived"]["date_multiple"] = []
        oclc_dict["aa_oclc_derived"]["year_multiple"] = []
        oclc_dict["aa_oclc_derived"]["series_multiple"] = []
        oclc_dict["aa_oclc_derived"]["volume_multiple"] = []
        oclc_dict["aa_oclc_derived"]["description_multiple"] = []
        oclc_dict["aa_oclc_derived"]["languages_multiple"] = []
        oclc_dict["aa_oclc_derived"]["isbn_multiple"] = []
        oclc_dict["aa_oclc_derived"]["issn_multiple"] = []
        oclc_dict["aa_oclc_derived"]["doi_multiple"] = []
        oclc_dict["aa_oclc_derived"]["general_format_multiple"] = []
        oclc_dict["aa_oclc_derived"]["specific_format_multiple"] = []
        oclc_dict["aa_oclc_derived"]["content_type"] = "other"
        oclc_dict["aa_oclc_derived"]["rft_multiple"] = []
        oclc_dict["aac_records"] = aac_records

        for aac_record in aac_records:
            aac_metadata = aac_record['metadata']
            if aac_metadata['type'] in 'title_json':
                oclc_dict["aa_oclc_derived"]["title_multiple"].append((aac_metadata['record'].get('title') or ''))
                oclc_dict["aa_oclc_derived"]["author_multiple"].append(oclc_get_authors_from_contributors(aac_metadata['record'].get('contributors') or []))
                oclc_dict["aa_oclc_derived"]["publisher_multiple"].append((aac_metadata['record'].get('publisher') or ''))
                oclc_dict["aa_oclc_derived"]["edition_multiple"].append((aac_metadata['record'].get('edition') or ''))
                oclc_dict["aa_oclc_derived"]["place_multiple"].append((aac_metadata['record'].get('publicationPlace') or ''))
                oclc_dict["aa_oclc_derived"]["date_multiple"].append((aac_metadata['record'].get('publicationDate') or ''))
                oclc_dict["aa_oclc_derived"]["series_multiple"].append((aac_metadata['record'].get('series') or ''))
                oclc_dict["aa_oclc_derived"]["volume_multiple"] += (aac_metadata['record'].get('seriesVolumes') or [])
                oclc_dict["aa_oclc_derived"]["description_multiple"].append((aac_metadata['record'].get('summary') or ''))
                oclc_dict["aa_oclc_derived"]["languages_multiple"].append((aac_metadata['record'].get('catalogingLanguage') or ''))
                oclc_dict["aa_oclc_derived"]["isbn_multiple"].append((aac_metadata['record'].get('isbn13') or ''))
                oclc_dict["aa_oclc_derived"]["isbn_multiple"] += (aac_metadata['record'].get('isbns') or [])
                oclc_dict["aa_oclc_derived"]["issn_multiple"].append((aac_metadata['record'].get('sourceIssn') or ''))
                oclc_dict["aa_oclc_derived"]["issn_multiple"] += (aac_metadata['record'].get('issns') or [])
                oclc_dict["aa_oclc_derived"]["doi_multiple"].append((aac_metadata['record'].get('doi') or ''))
                oclc_dict["aa_oclc_derived"]["general_format_multiple"].append((aac_metadata['record'].get('generalFormat') or ''))
                oclc_dict["aa_oclc_derived"]["specific_format_multiple"].append((aac_metadata['record'].get('specificFormat') or ''))
            elif aac_metadata['type'] == 'briefrecords_json':
                oclc_dict["aa_oclc_derived"]["title_multiple"].append((aac_metadata['record'].get('title') or ''))
                oclc_dict["aa_oclc_derived"]["author_multiple"].append(oclc_get_authors_from_contributors(aac_metadata['record'].get('contributors') or []))
                oclc_dict["aa_oclc_derived"]["publisher_multiple"].append((aac_metadata['record'].get('publisher') or ''))
                oclc_dict["aa_oclc_derived"]["edition_multiple"].append((aac_metadata['record'].get('edition') or ''))
                oclc_dict["aa_oclc_derived"]["place_multiple"].append((aac_metadata['record'].get('publicationPlace') or ''))
                oclc_dict["aa_oclc_derived"]["date_multiple"].append((aac_metadata['record'].get('publicationDate') or ''))
                oclc_dict["aa_oclc_derived"]["description_multiple"].append((aac_metadata['record'].get('summary') or ''))
                oclc_dict["aa_oclc_derived"]["description_multiple"] += (aac_metadata['record'].get('summaries') or [])
                oclc_dict["aa_oclc_derived"]["languages_multiple"].append((aac_metadata['record'].get('catalogingLanguage') or ''))
                oclc_dict["aa_oclc_derived"]["isbn_multiple"].append((aac_metadata['record'].get('isbn13') or ''))
                oclc_dict["aa_oclc_derived"]["isbn_multiple"] += (aac_metadata['record'].get('isbns') or [])
                oclc_dict["aa_oclc_derived"]["general_format_multiple"].append((aac_metadata['record'].get('generalFormat') or ''))
                oclc_dict["aa_oclc_derived"]["specific_format_multiple"].append((aac_metadata['record'].get('specificFormat') or ''))
                # TODO: unverified:
                oclc_dict["aa_oclc_derived"]["issn_multiple"].append((aac_metadata['record'].get('sourceIssn') or ''))
                oclc_dict["aa_oclc_derived"]["issn_multiple"] += (aac_metadata['record'].get('issns') or [])
                oclc_dict["aa_oclc_derived"]["doi_multiple"].append((aac_metadata['record'].get('doi') or ''))
                # TODO: series/volume?
            elif aac_metadata['type'] == 'providersearchrequest_json':
                rft = urllib.parse.parse_qs((aac_metadata['record'].get('openUrlContextObject') or ''))
                oclc_dict["aa_oclc_derived"]["rft_multiple"].append(rft)

                oclc_dict["aa_oclc_derived"]["title_multiple"].append((aac_metadata['record'].get('titleObject') or {}).get('data') or '')
                oclc_dict["aa_oclc_derived"]["author_multiple"].append(oclc_get_authors_from_authors(aac_metadata['record'].get('authors') or []))
                oclc_dict["aa_oclc_derived"]["publisher_multiple"] += (rft.get('rft.pub') or [])
                oclc_dict["aa_oclc_derived"]["edition_multiple"].append((aac_metadata['record'].get('edition') or ''))
                oclc_dict["aa_oclc_derived"]["place_multiple"] += (rft.get('rft.place') or [])
                oclc_dict["aa_oclc_derived"]["date_multiple"] += (rft.get('rft.date') or [])
                oclc_dict["aa_oclc_derived"]["date_multiple"].append((aac_metadata['record'].get('date') or ''))
                oclc_dict["aa_oclc_derived"]["description_multiple"] += [(summary.get('data') or '') for summary in (aac_metadata['record'].get('summariesObjectList') or [])]
                oclc_dict["aa_oclc_derived"]["languages_multiple"].append((aac_metadata['record'].get('language') or ''))
                oclc_dict["aa_oclc_derived"]["general_format_multiple"] += [orjson.loads(dat)['stdrt1'] for dat in (rft.get('rft_dat') or [])]
                oclc_dict["aa_oclc_derived"]["specific_format_multiple"] += [orjson.loads(dat)['stdrt2'] for dat in (rft.get('rft_dat') or [])]

                # TODO: series/volume?
                # lcNumber, masterCallNumber
            elif aac_metadata['type'] == 'legacysearch_html':
                rft = {}
                rft_match = re.search('url_ver=Z39.88-2004[^"]+', aac_metadata['html'])
                if rft_match is not None:
                    rft = urllib.parse.parse_qs(rft_match.group())
                oclc_dict["aa_oclc_derived"]["rft_multiple"].append(rft)

                oclc_dict["aa_oclc_derived"]["title_multiple"] += (rft.get('rft.title') or [])
                legacy_author_match = re.search('<div class="author">([^<]+)</div>', aac_metadata['html'])
                if legacy_author_match:
                    legacy_authors = legacy_author_match.group(1)
                    if legacy_authors.startswith('by '):
                        legacy_authors = legacy_authors[len('by '):]
                    oclc_dict["aa_oclc_derived"]["author_multiple"].append(legacy_authors)
                oclc_dict["aa_oclc_derived"]["publisher_multiple"] += (rft.get('rft.pub') or [])
                oclc_dict["aa_oclc_derived"]["edition_multiple"] += (rft.get('rft.edition') or [])
                oclc_dict["aa_oclc_derived"]["place_multiple"] += (rft.get('rft.place') or [])
                oclc_dict["aa_oclc_derived"]["date_multiple"] += (rft.get('rft.date') or [])
                legacy_language_match = re.search('<span class="itemLanguage">([^<]+)</span>', aac_metadata['html'])
                if legacy_language_match:
                    legacy_language = legacy_language_match.group(1)
                    oclc_dict["aa_oclc_derived"]["languages_multiple"].append(legacy_language)
                oclc_dict["aa_oclc_derived"]["general_format_multiple"] += [orjson.loads(dat)['stdrt1'] for dat in (rft.get('rft_dat') or [])]
                oclc_dict["aa_oclc_derived"]["specific_format_multiple"] += [orjson.loads(dat)['stdrt2'] for dat in (rft.get('rft_dat') or [])]
                # TODO: series/volume?
            elif aac_metadata['type'] in ['not_found_title_json', 'redirect_title_json']:
                pass
            else:
                raise Exception(f"Unexpected aac_metadata.type: {aac_metadata['type']}")

        oclc_dict["aa_oclc_derived"]["title_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["title_multiple"]])))
        oclc_dict["aa_oclc_derived"]["author_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["author_multiple"]])))
        oclc_dict["aa_oclc_derived"]["publisher_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["publisher_multiple"]])))
        oclc_dict["aa_oclc_derived"]["edition_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["edition_multiple"]])))
        oclc_dict["aa_oclc_derived"]["place_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["place_multiple"]])))
        oclc_dict["aa_oclc_derived"]["date_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["date_multiple"]])))
        oclc_dict["aa_oclc_derived"]["series_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["series_multiple"]])))
        oclc_dict["aa_oclc_derived"]["volume_multiple"] = list(dict.fromkeys(filter(len, [re.sub(r'[ ]+', ' ', s.strip(' \n\t,.;[]')) for s in oclc_dict["aa_oclc_derived"]["volume_multiple"]])))
        oclc_dict["aa_oclc_derived"]["description_multiple"] = list(dict.fromkeys(filter(len, oclc_dict["aa_oclc_derived"]["description_multiple"])))
        oclc_dict["aa_oclc_derived"]["languages_multiple"] = list(dict.fromkeys(filter(len, oclc_dict["aa_oclc_derived"]["languages_multiple"])))
        oclc_dict["aa_oclc_derived"]["isbn_multiple"] = list(dict.fromkeys(filter(len, oclc_dict["aa_oclc_derived"]["isbn_multiple"])))
        oclc_dict["aa_oclc_derived"]["issn_multiple"] = list(dict.fromkeys(filter(len, oclc_dict["aa_oclc_derived"]["issn_multiple"])))
        oclc_dict["aa_oclc_derived"]["doi_multiple"] = list(dict.fromkeys(filter(len, oclc_dict["aa_oclc_derived"]["doi_multiple"])))
        oclc_dict["aa_oclc_derived"]["general_format_multiple"] = list(dict.fromkeys(filter(len, [s.lower() for s in oclc_dict["aa_oclc_derived"]["general_format_multiple"]])))
        oclc_dict["aa_oclc_derived"]["specific_format_multiple"] = list(dict.fromkeys(filter(len, [s.lower() for s in oclc_dict["aa_oclc_derived"]["specific_format_multiple"]])))

        for s in oclc_dict["aa_oclc_derived"]["date_multiple"]:
            potential_year = re.search(r"(\d\d\d\d)", s)
            if potential_year is not None:
                oclc_dict["aa_oclc_derived"]["year_multiple"].append(potential_year[0])

        if "thsis" in oclc_dict["aa_oclc_derived"]["specific_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'journal_article'
        elif "mss" in oclc_dict["aa_oclc_derived"]["specific_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'journal_article'
        elif "book" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'book_unknown'
        elif "artchap" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'journal_article'
        elif "artcl" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'journal_article'
        elif "news" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'magazine'
        elif "jrnl" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'magazine'
        elif "msscr" in oclc_dict["aa_oclc_derived"]["general_format_multiple"]:
            oclc_dict["aa_oclc_derived"]["content_type"] = 'musical_score'

        oclc_dict["aa_oclc_derived"]['edition_varia_normalized'] = ', '.join(list(dict.fromkeys(filter(len, [
            max(['', *oclc_dict["aa_oclc_derived"]["series_multiple"]], key=len),
            max(['', *oclc_dict["aa_oclc_derived"]["volume_multiple"]], key=len),
            max(['', *oclc_dict["aa_oclc_derived"]["edition_multiple"]], key=len),
            max(['', *oclc_dict["aa_oclc_derived"]["place_multiple"]], key=len),
            max(['', *oclc_dict["aa_oclc_derived"]["date_multiple"]], key=len),
        ]))))

        oclc_dict['aa_oclc_derived']['stripped_description_multiple'] = [strip_description(description) for description in oclc_dict['aa_oclc_derived']['description_multiple']]
        oclc_dict['aa_oclc_derived']['language_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes(language) for language in oclc_dict['aa_oclc_derived']['languages_multiple']])

        allthethings.utils.init_identifiers_and_classification_unified(oclc_dict['aa_oclc_derived'])
        allthethings.utils.add_identifier_unified(oclc_dict['aa_oclc_derived'], 'oclc', oclc_id)
        allthethings.utils.add_isbns_unified(oclc_dict['aa_oclc_derived'], oclc_dict['aa_oclc_derived']['isbn_multiple'])
        for issn in oclc_dict['aa_oclc_derived']['issn_multiple']:
            allthethings.utils.add_identifier_unified(oclc_dict['aa_oclc_derived'], 'issn', issn)
        for doi in oclc_dict['aa_oclc_derived']['doi_multiple']:
            allthethings.utils.add_identifier_unified(oclc_dict['aa_oclc_derived'], 'doi', doi)

        # TODO:
        # * cover_url
        # * comments
        # * other/related OCLC numbers
        # * Genre for fiction detection
        # * Full audit of all fields
        # * dict comments

        oclc_dicts.append(oclc_dict)


    return oclc_dicts

@page.get("/db/oclc/<path:oclc>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def oclc_oclc_json(oclc):
    with Session(engine) as session:
        oclc_dicts = get_oclc_dicts(session, 'oclc', [oclc])
        if len(oclc_dicts) == 0:
            return "{}", 404
        return nice_json(oclc_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def is_string_subsequence(needle, haystack):
    i_needle = 0
    i_haystack = 0
    while i_needle < len(needle) and i_haystack < len(haystack):
        if needle[i_needle].lower() == haystack[i_haystack].lower():
            i_needle += 1
        i_haystack += 1
    return i_needle == len(needle)

def sort_by_length_and_filter_subsequences_with_longest_string(strings):
    strings = [string for string in sorted(set(strings), key=len, reverse=True) if len(string) > 0]
    if len(strings) == 0:
        return []
    longest_string = strings[0]
    strings_filtered = [longest_string]
    for string in strings[1:]:
        if not is_string_subsequence(string, longest_string):
            strings_filtered.append(string)
    return strings_filtered

def get_aarecords_elasticsearch(aarecord_ids):
    if not allthethings.utils.validate_aarecord_ids(aarecord_ids):
        raise Exception("Invalid aarecord_ids")

    # Filter out bad data
    aarecord_ids = [val for val in aarecord_ids if val not in search_filtered_bad_aarecord_ids]

    if len(aarecord_ids) == 0:
        return []

    # Uncomment the following lines to use MySQL directly; useful for local development.
    # with Session(engine) as session:
    #     return [add_additional_to_aarecord(aarecord) for aarecord in get_aarecords_mysql(session, aarecord_ids)]

    docs_by_es_handle = collections.defaultdict(list)
    for aarecord_id in aarecord_ids:
        index = allthethings.utils.AARECORD_PREFIX_SEARCH_INDEX_MAPPING[aarecord_id.split(':', 1)[0]]
        es_handle = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[index]
        docs_by_es_handle[es_handle].append({'_id': aarecord_id, '_index': index })

    search_results_raw = []
    for es_handle, docs in docs_by_es_handle.items():
        search_results_raw += es_handle.mget(docs=docs)['docs']
    return [add_additional_to_aarecord(aarecord_raw['_source']) for aarecord_raw in search_results_raw if aarecord_raw['found'] and (aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids)]


def aarecord_score_base(aarecord):
    if len(aarecord['file_unified_data'].get('problems') or []) > 0:
        return 0.01

    score = 10000.0
    # Filesize of >0.2MB is overriding everything else.
    if (aarecord['file_unified_data'].get('filesize_best') or 0) > 200000:
        score += 1000.0
    if (aarecord['file_unified_data'].get('filesize_best') or 0) > 700000:
        score += 5.0
    if (aarecord['file_unified_data'].get('filesize_best') or 0) > 1200000:
        score += 5.0
    # If we're not confident about the language, demote.
    if len(aarecord['file_unified_data'].get('language_codes') or []) == 0:
        score -= 2.0
    # Bump English a little bit regardless of the user's language
    if (aarecord['search_only_fields']['search_most_likely_language_code'] == 'en'):
        score += 5.0
    if (aarecord['file_unified_data'].get('extension_best') or '') in ['epub', 'pdf']:
        score += 15.0
    if (aarecord['file_unified_data'].get('extension_best') or '') in ['cbr', 'mobi', 'fb2', 'cbz', 'azw3', 'djvu', 'fb2.zip']:
        score += 5.0
    if len(aarecord['file_unified_data'].get('cover_url_best') or '') > 0:
        score += 3.0
    if (aarecord['file_unified_data'].get('has_aa_downloads') or 0) > 0:
        score += 5.0
    # Don't bump IA too much.
    if (aarecord['file_unified_data'].get('has_aa_exclusive_downloads') or 0) > 0:
        score += 3.0
    if len(aarecord['file_unified_data'].get('title_best') or '') > 0:
        score += 10.0
    if len(aarecord['file_unified_data'].get('author_best') or '') > 0:
        score += 2.0
    if len(aarecord['file_unified_data'].get('publisher_best') or '') > 0:
        score += 2.0
    if len(aarecord['file_unified_data'].get('edition_varia_best') or '') > 0:
        score += 2.0
    score += min(8.0, 2.0*len(aarecord['file_unified_data'].get('identifiers_unified') or []))
    if len(aarecord['file_unified_data'].get('content_type') or '') in ['journal_article', 'standards_document', 'book_comic', 'magazine']:
        # For now demote non-books quite a bit, since they can drown out books.
        # People can filter for them directly.
        score -= 70.0
    if len(aarecord['file_unified_data'].get('stripped_description_best') or '') > 0:
        score += 3.0
    return score

def get_aarecords_mysql(session, aarecord_ids):
    if not allthethings.utils.validate_aarecord_ids(aarecord_ids):
        raise Exception("Invalid aarecord_ids")

    # Filter out bad data
    aarecord_ids = list(set([val for val in aarecord_ids if val not in search_filtered_bad_aarecord_ids]))

    split_ids = allthethings.utils.split_aarecord_ids(aarecord_ids)
    lgrsnf_book_dicts = dict(('md5:' + item['md5'].lower(), item) for item in get_lgrsnf_book_dicts(session, "MD5", split_ids['md5']))
    lgrsfic_book_dicts = dict(('md5:' + item['md5'].lower(), item) for item in get_lgrsfic_book_dicts(session, "MD5", split_ids['md5']))
    lgli_file_dicts = dict(('md5:' + item['md5'].lower(), item) for item in get_lgli_file_dicts(session, "md5", split_ids['md5']))
    zlib_book_dicts1 = dict(('md5:' + item['md5_reported'].lower(), item) for item in get_zlib_book_dicts(session, "md5_reported", split_ids['md5']))
    zlib_book_dicts2 = dict(('md5:' + item['md5'].lower(), item) for item in get_zlib_book_dicts(session, "md5", split_ids['md5']))
    aac_zlib3_book_dicts1 = dict(('md5:' + item['md5_reported'].lower(), item) for item in get_aac_zlib3_book_dicts(session, "md5_reported", split_ids['md5']))
    aac_zlib3_book_dicts2 = dict(('md5:' + item['md5'].lower(), item) for item in get_aac_zlib3_book_dicts(session, "md5", split_ids['md5']))
    aa_lgli_comics_2022_08_file_dicts = dict(('md5:' + item['md5'].lower(), item) for item in get_aa_lgli_comics_2022_08_file_dicts(session, "md5", split_ids['md5']))
    ia_record_dicts = dict(('md5:' + item['aa_ia_file']['md5'].lower(), item) for item in get_ia_record_dicts(session, "md5", split_ids['md5']) if item.get('aa_ia_file') is not None)
    ia_record_dicts2 = dict(('ia:' + item['ia_id'].lower(), item) for item in get_ia_record_dicts(session, "ia_id", split_ids['ia']) if item.get('aa_ia_file') is None)
    isbndb_dicts = {('isbn:' + item['ean13']): item['isbndb'] for item in get_isbndb_dicts(session, split_ids['isbn'])}
    ol_book_dicts = {('ol:' + item['ol_edition']): [item] for item in get_ol_book_dicts(session, 'ol_edition', split_ids['ol'])}
    scihub_doi_dicts = {('doi:' + item['doi']): [item] for item in get_scihub_doi_dicts(session, 'doi', split_ids['doi'])}
    oclc_dicts = {('oclc:' + item['oclc_id']): [item] for item in get_oclc_dicts(session, 'oclc', split_ids['oclc'])}

    # First pass, so we can fetch more dependencies.
    aarecords = []
    canonical_isbn13s = []
    ol_editions = []
    dois = []
    oclc_ids = []
    for aarecord_id in aarecord_ids:
        aarecord_id_split = aarecord_id.split(':', 1)
        aarecord = {}
        aarecord['id'] = aarecord_id
        aarecord['lgrsnf_book'] = lgrsnf_book_dicts.get(aarecord_id)
        aarecord['lgrsfic_book'] = lgrsfic_book_dicts.get(aarecord_id)
        aarecord['lgli_file'] = lgli_file_dicts.get(aarecord_id)
        if aarecord.get('lgli_file'):
            aarecord['lgli_file']['editions'] = aarecord['lgli_file']['editions'][0:5]
        aarecord['zlib_book'] = zlib_book_dicts1.get(aarecord_id) or zlib_book_dicts2.get(aarecord_id)
        aarecord['aac_zlib3_book'] = aac_zlib3_book_dicts1.get(aarecord_id) or aac_zlib3_book_dicts2.get(aarecord_id)
        aarecord['aa_lgli_comics_2022_08_file'] = aa_lgli_comics_2022_08_file_dicts.get(aarecord_id)
        aarecord['ia_record'] = ia_record_dicts.get(aarecord_id) or ia_record_dicts2.get(aarecord_id)
        aarecord['isbndb'] = list(isbndb_dicts.get(aarecord_id) or [])
        aarecord['ol'] = list(ol_book_dicts.get(aarecord_id) or [])
        aarecord['scihub_doi'] = list(scihub_doi_dicts.get(aarecord_id) or [])
        aarecord['oclc'] = list(oclc_dicts.get(aarecord_id) or [])
        
        lgli_all_editions = aarecord['lgli_file']['editions'] if aarecord.get('lgli_file') else []

        aarecord['file_unified_data'] = {}
        # Duplicated below, with more fields
        aarecord['file_unified_data']['identifiers_unified'] = allthethings.utils.merge_unified_fields([
            ((aarecord['lgrsnf_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['aac_zlib3_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['zlib_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgli_file'] or {}).get('identifiers_unified') or {}),
            *[(edition['identifiers_unified'].get('identifiers_unified') or {}) for edition in lgli_all_editions],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('identifiers_unified') or {}),
            *[isbndb['identifiers_unified'] for isbndb in aarecord['isbndb']],
            *[ol_book_dict['identifiers_unified'] for ol_book_dict in aarecord['ol']],
            *[scihub_doi['identifiers_unified'] for scihub_doi in aarecord['scihub_doi']],
            *[oclc['aa_oclc_derived']['identifiers_unified'] for oclc in aarecord['oclc']],
        ])
        # TODO: This `if` is not necessary if we make sure that the fields of the primary records get priority.
        if aarecord_id_split[0] not in ['isbn', 'ol', 'oclc']:
            for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
                canonical_isbn13s.append(canonical_isbn13)
            for potential_ol_edition in (aarecord['file_unified_data']['identifiers_unified'].get('ol') or []):
                if allthethings.utils.validate_ol_editions([potential_ol_edition]):
                    ol_editions.append(potential_ol_edition)
            for doi in (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []):
                dois.append(doi)
            for oclc_id in (aarecord['file_unified_data']['identifiers_unified'].get('oclc') or []):
                oclc_ids.append(oclc_id)

        aarecords.append(aarecord)

    isbndb_dicts2 = {item['ean13']: item for item in get_isbndb_dicts(session, list(set(canonical_isbn13s)))}
    ol_book_dicts2 = {item['ol_edition']: item for item in get_ol_book_dicts(session, 'ol_edition', list(set(ol_editions)))}
    scihub_doi_dicts2 = {item['doi']: item for item in get_scihub_doi_dicts(session, 'doi', list(set(dois)))}
    oclc_dicts2 = {item['oclc_id']: item for item in get_oclc_dicts(session, 'oclc', list(set(oclc_ids)))}

    # Second pass
    for aarecord in aarecords:
        aarecord_id = aarecord['id']
        aarecord_id_split = aarecord_id.split(':', 1)
        lgli_single_edition = aarecord['lgli_file']['editions'][0] if len((aarecord.get('lgli_file') or {}).get('editions') or []) == 1 else None
        lgli_all_editions = aarecord['lgli_file']['editions'] if aarecord.get('lgli_file') else []

        if aarecord_id_split[0] in allthethings.utils.AARECORD_PREFIX_SEARCH_INDEX_MAPPING:
            aarecord['indexes'] = [allthethings.utils.AARECORD_PREFIX_SEARCH_INDEX_MAPPING[aarecord_id_split[0]]]
        else:
            raise Exception(f"Unknown aarecord_id prefix: {aarecord_id}")

        if allthethings.utils.AARECORD_PREFIX_SEARCH_INDEX_MAPPING[aarecord_id_split[0]] != 'aarecords_metadata':
            isbndb_all = []
            existing_isbn13s = set([isbndb['isbn13'] for isbndb in aarecord['isbndb']])
            for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
                if canonical_isbn13 not in existing_isbn13s:
                    for isbndb in isbndb_dicts2[canonical_isbn13]['isbndb']:
                        isbndb_all.append(isbndb)
            if len(isbndb_all) > 5:
                isbndb_all = []
            aarecord['isbndb'] = (aarecord['isbndb'] + isbndb_all)

            ol_book_dicts_all = []
            existing_ol_editions = set([ol_book_dict['ol_edition'] for ol_book_dict in aarecord['ol']])
            for potential_ol_edition in (aarecord['file_unified_data']['identifiers_unified'].get('ol') or []):
                if (potential_ol_edition in ol_book_dicts2) and (potential_ol_edition not in existing_ol_editions):
                    ol_book_dicts_all.append(ol_book_dicts2[potential_ol_edition])
            if len(ol_book_dicts_all) > 3:
                ol_book_dicts_all = []
            aarecord['ol'] = (aarecord['ol'] + ol_book_dicts_all)

            scihub_doi_all = []
            existing_dois = set([scihub_doi['doi'] for scihub_doi in aarecord['scihub_doi']])
            for doi in (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []):
                if (doi in scihub_doi_dicts2) and (doi not in existing_dois):
                    scihub_doi_all.append(scihub_doi_dicts2[doi])
            if len(scihub_doi_all) > 3:
                scihub_doi_all = []
            aarecord['scihub_doi'] = (aarecord['scihub_doi'] + scihub_doi_all)

            oclc_all = []
            existing_oclc_ids = set([oclc['oclc_id'] for oclc in aarecord['oclc']])
            for oclc_id in (aarecord['file_unified_data']['identifiers_unified'].get('oclc') or []):
                if (oclc_id in oclc_dicts2) and (oclc_id not in existing_oclc_ids):
                    oclc_all.append(oclc_dicts2[oclc_id])
            if len(oclc_all) > 3:
                oclc_all = []
            aarecord['oclc'] = (aarecord['oclc'] + oclc_all)

        aarecord['ipfs_infos'] = []
        if aarecord['lgrsnf_book'] and len(aarecord['lgrsnf_book'].get('ipfs_cid') or '') > 0:
            aarecord['ipfs_infos'].append({ 'ipfs_cid': aarecord['lgrsnf_book']['ipfs_cid'].lower(), 'from': 'lgrsnf' })
        if aarecord['lgrsfic_book'] and len(aarecord['lgrsfic_book'].get('ipfs_cid') or '') > 0:
            aarecord['ipfs_infos'].append({ 'ipfs_cid': aarecord['lgrsfic_book']['ipfs_cid'].lower(), 'from': 'lgrsfic' })

        original_filename_multiple = [
            ((aarecord['lgrsnf_book'] or {}).get('locator') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('locator') or '').strip(),
            ((aarecord['lgli_file'] or {}).get('locator') or '').strip(),
            *[filename.strip() for filename in (((aarecord['lgli_file'] or {}).get('descriptions_mapped') or {}).get('library_filename') or [])],
            ((aarecord['lgli_file'] or {}).get('scimag_archive_path_decoded') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('original_filename') or '').strip(),
        ]
        original_filename_multiple_processed = sort_by_length_and_filter_subsequences_with_longest_string(original_filename_multiple)
        aarecord['file_unified_data']['original_filename_best'] = min(original_filename_multiple_processed, key=len) if len(original_filename_multiple_processed) > 0 else ''
        original_filename_multiple += [(scihub_doi['doi'].strip() + '.pdf') for scihub_doi in aarecord['scihub_doi']]
        if aarecord['file_unified_data']['original_filename_best'] == '':
            original_filename_multiple_processed = sort_by_length_and_filter_subsequences_with_longest_string(original_filename_multiple)
            aarecord['file_unified_data']['original_filename_best'] = min(original_filename_multiple_processed, key=len) if len(original_filename_multiple_processed) > 0 else ''
        aarecord['file_unified_data']['original_filename_additional'] = [s for s in original_filename_multiple_processed if s != aarecord['file_unified_data']['original_filename_best']]
        aarecord['file_unified_data']['original_filename_best_name_only'] = re.split(r'[\\/]', aarecord['file_unified_data']['original_filename_best'])[-1] if not aarecord['file_unified_data']['original_filename_best'].startswith('10.') else aarecord['file_unified_data']['original_filename_best']
        if len(aarecord['file_unified_data']['original_filename_additional']) == 0:
            del aarecord['file_unified_data']['original_filename_additional']

        # Select the cover_url_normalized in order of what is likely to be the best one: ia, zlib, lgrsnf, lgrsfic, lgli.
        cover_url_multiple = [
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('cover_url') or '').strip(),
            ((aarecord['zlib_book'] or {}).get('cover_url') or '').strip(),
            ((aarecord['lgrsnf_book'] or {}).get('cover_url_normalized') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('cover_url_normalized') or '').strip(),
            ((aarecord['lgli_file'] or {}).get('cover_url_guess_normalized') or '').strip(),
            *[ol_book_dict['cover_url_normalized'] for ol_book_dict in aarecord['ol']],
            *[(isbndb['json'].get('image') or '').strip() for isbndb in aarecord['isbndb']],
            *[isbndb['cover_url_guess'] for isbndb in aarecord['isbndb']],
        ]
        cover_url_multiple_processed = list(dict.fromkeys(filter(len, cover_url_multiple)))
        aarecord['file_unified_data']['cover_url_best'] = (cover_url_multiple_processed + [''])[0]
        aarecord['file_unified_data']['cover_url_additional'] = [s for s in cover_url_multiple_processed if s != aarecord['file_unified_data']['cover_url_best']]
        if len(aarecord['file_unified_data']['cover_url_additional']) == 0:
            del aarecord['file_unified_data']['cover_url_additional']

        extension_multiple = [
            (((aarecord['ia_record'] or {}).get('aa_ia_file') or {}).get('extension') or '').strip(),
            ((aarecord['aac_zlib3_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['zlib_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgrsnf_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgrsfic_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgli_file'] or {}).get('extension') or '').strip().lower(),
            ('pdf' if aarecord_id_split[0] == 'doi' else ''),
        ]
        if "epub" in extension_multiple:
            aarecord['file_unified_data']['extension_best'] = "epub"
        elif "pdf" in extension_multiple:
            aarecord['file_unified_data']['extension_best'] = "pdf"
        else:
            aarecord['file_unified_data']['extension_best'] = max(extension_multiple, key=len)
        aarecord['file_unified_data']['extension_additional'] = [s for s in dict.fromkeys(filter(len, extension_multiple)) if s != aarecord['file_unified_data']['extension_best']]
        if len(aarecord['file_unified_data']['extension_additional']) == 0:
            del aarecord['file_unified_data']['extension_additional']

        filesize_multiple = [
            ((aarecord['ia_record'] or {}).get('aa_ia_file') or {}).get('filesize') or 0,
            (aarecord['aac_zlib3_book'] or {}).get('filesize_reported') or 0,
            (aarecord['zlib_book'] or {}).get('filesize_reported') or 0,
            (aarecord['zlib_book'] or {}).get('filesize') or 0,
            (aarecord['lgrsnf_book'] or {}).get('filesize') or 0,
            (aarecord['lgrsfic_book'] or {}).get('filesize') or 0,
            (aarecord['lgli_file'] or {}).get('filesize') or 0,
        ]
        aarecord['file_unified_data']['filesize_best'] = max(filesize_multiple)
        if aarecord['ia_record'] is not None and len(aarecord['ia_record']['json']['aa_shorter_files']) > 0:
            filesize_multiple.append(max(int(file.get('size') or '0') for file in aarecord['ia_record']['json']['aa_shorter_files']))
        if aarecord['file_unified_data']['filesize_best'] == 0:
            aarecord['file_unified_data']['filesize_best'] = max(filesize_multiple)
        zlib_book_filesize = (aarecord['zlib_book'] or {}).get('filesize') or 0
        if zlib_book_filesize > 0:
            # If we have a zlib_book with a `filesize`, then that is leading, since we measured it ourselves.
            aarecord['file_unified_data']['filesize_best'] = zlib_book_filesize
        aarecord['file_unified_data']['filesize_additional'] = [s for s in dict.fromkeys(filter(lambda fz: fz > 0, filesize_multiple)) if s != aarecord['file_unified_data']['filesize_best']]
        if len(aarecord['file_unified_data']['filesize_additional']) == 0:
            del aarecord['file_unified_data']['filesize_additional']

        title_multiple = [
            ((aarecord['lgrsnf_book'] or {}).get('title') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('title') or '').strip(),
            ((lgli_single_edition or {}).get('title') or '').strip(),
            ((aarecord['aac_zlib3_book'] or {}).get('title') or '').strip(),
            ((aarecord['zlib_book'] or {}).get('title') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('title') or '').strip(),
        ]
        aarecord['file_unified_data']['title_best'] = max(title_multiple, key=len)
        title_multiple += [(edition.get('title') or '').strip() for edition in lgli_all_editions]
        title_multiple += [title.strip() for edition in lgli_all_editions for title in (edition['descriptions_mapped'].get('maintitleonoriginallanguage') or [])]
        title_multiple += [title.strip() for edition in lgli_all_editions for title in (edition['descriptions_mapped'].get('maintitleonenglishtranslate') or [])]
        title_multiple += [(ol_book_dict.get('title_normalized') or '').strip() for ol_book_dict in aarecord['ol']]
        title_multiple += [(isbndb.get('title_normalized') or '').strip() for isbndb in aarecord['isbndb']]
        for oclc in aarecord['oclc']:
            title_multiple += oclc['aa_oclc_derived']['title_multiple']
        if aarecord['file_unified_data']['title_best'] == '':
            aarecord['file_unified_data']['title_best'] = max(title_multiple, key=len)
        aarecord['file_unified_data']['title_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(title_multiple) if s != aarecord['file_unified_data']['title_best']]
        if len(aarecord['file_unified_data']['title_additional']) == 0:
            del aarecord['file_unified_data']['title_additional']

        author_multiple = [
            (aarecord['lgrsnf_book'] or {}).get('author', '').strip(),
            (aarecord['lgrsfic_book'] or {}).get('author', '').strip(),
            (lgli_single_edition or {}).get('authors_normalized', '').strip(),
            (aarecord['aac_zlib3_book'] or {}).get('author', '').strip(),
            (aarecord['zlib_book'] or {}).get('author', '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('author') or '').strip(),
        ]
        aarecord['file_unified_data']['author_best'] = max(author_multiple, key=len)
        author_multiple += [edition.get('authors_normalized', '').strip() for edition in lgli_all_editions]
        author_multiple += [ol_book_dict['authors_normalized'] for ol_book_dict in aarecord['ol']]
        author_multiple += [", ".join(isbndb['json'].get('authors') or []) for isbndb in aarecord['isbndb']]
        for oclc in aarecord['oclc']:
            author_multiple += oclc['aa_oclc_derived']['author_multiple']
        if aarecord['file_unified_data']['author_best'] == '':
            aarecord['file_unified_data']['author_best'] = max(author_multiple, key=len)
        aarecord['file_unified_data']['author_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(author_multiple) if s != aarecord['file_unified_data']['author_best']]
        if len(aarecord['file_unified_data']['author_additional']) == 0:
            del aarecord['file_unified_data']['author_additional']

        publisher_multiple = [
            ((aarecord['lgrsnf_book'] or {}).get('publisher') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('publisher') or '').strip(),
            ((lgli_single_edition or {}).get('publisher_normalized') or '').strip(),
            ((aarecord['aac_zlib3_book'] or {}).get('publisher') or '').strip(),
            ((aarecord['zlib_book'] or {}).get('publisher') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('publisher') or '').strip(),
        ]
        aarecord['file_unified_data']['publisher_best'] = max(publisher_multiple, key=len)
        publisher_multiple += [(edition.get('publisher_normalized') or '').strip() for edition in lgli_all_editions]
        publisher_multiple += [(ol_book_dict.get('publishers_normalized') or '').strip() for ol_book_dict in aarecord['ol']]
        publisher_multiple += [(isbndb['json'].get('publisher') or '').strip() for isbndb in aarecord['isbndb']]
        for oclc in aarecord['oclc']:
            publisher_multiple += oclc['aa_oclc_derived']['publisher_multiple']
        if aarecord['file_unified_data']['publisher_best'] == '':
            aarecord['file_unified_data']['publisher_best'] = max(publisher_multiple, key=len)
        aarecord['file_unified_data']['publisher_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(publisher_multiple) if s != aarecord['file_unified_data']['publisher_best']]
        if len(aarecord['file_unified_data']['publisher_additional']) == 0:
            del aarecord['file_unified_data']['publisher_additional']

        edition_varia_multiple = [
            ((aarecord['lgrsnf_book'] or {}).get('edition_varia_normalized') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('edition_varia_normalized') or '').strip(),
            ((lgli_single_edition or {}).get('edition_varia_normalized') or '').strip(),
            ((aarecord['aac_zlib3_book'] or {}).get('edition_varia_normalized') or '').strip(),
            ((aarecord['zlib_book'] or {}).get('edition_varia_normalized') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('edition_varia_normalized') or '').strip(),
        ]
        aarecord['file_unified_data']['edition_varia_best'] = max(edition_varia_multiple, key=len)
        edition_varia_multiple += [(edition.get('edition_varia_normalized') or '').strip() for edition in lgli_all_editions]
        edition_varia_multiple += [(ol_book_dict.get('edition_varia_normalized') or '').strip() for ol_book_dict in aarecord['ol']]
        edition_varia_multiple += [(isbndb.get('edition_varia_normalized') or '').strip() for isbndb in aarecord['isbndb']]
        edition_varia_multiple += [oclc['aa_oclc_derived']['edition_varia_normalized'] for oclc in aarecord['oclc']]
        if aarecord['file_unified_data']['edition_varia_best'] == '':
            aarecord['file_unified_data']['edition_varia_best'] = max(edition_varia_multiple, key=len)
        aarecord['file_unified_data']['edition_varia_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(edition_varia_multiple) if s != aarecord['file_unified_data']['edition_varia_best']]
        if len(aarecord['file_unified_data']['edition_varia_additional']) == 0:
            del aarecord['file_unified_data']['edition_varia_additional']

        year_multiple_raw = [
            ((aarecord['lgrsnf_book'] or {}).get('year') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('year') or '').strip(),
            ((lgli_single_edition or {}).get('year') or '').strip(),
            ((lgli_single_edition or {}).get('issue_year_number') or '').strip(),
            ((aarecord['aac_zlib3_book'] or {}).get('year') or '').strip(),
            ((aarecord['zlib_book'] or {}).get('year') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('year') or '').strip(),
        ]
        # Filter out years in for which we surely don't have books (famous last words..)
        year_multiple = [(year if year.isdigit() and int(year) >= 1600 and int(year) < 2100 else '') for year in year_multiple_raw]
        aarecord['file_unified_data']['year_best'] = max(year_multiple, key=len)
        year_multiple += [(edition.get('year_normalized') or '').strip() for edition in lgli_all_editions]
        year_multiple += [(ol_book_dict.get('year_normalized') or '').strip() for ol_book_dict in aarecord['ol']]
        year_multiple += [(isbndb.get('year_normalized') or '').strip() for isbndb in aarecord['isbndb']]
        for oclc in aarecord['oclc']:
            year_multiple += oclc['aa_oclc_derived']['year_multiple']
        for year in year_multiple:
            # If a year appears in edition_varia_best, then use that, for consistency.
            if year != '' and year in aarecord['file_unified_data']['edition_varia_best']:
                aarecord['file_unified_data']['year_best'] = year
        if aarecord['file_unified_data']['year_best'] == '':
            aarecord['file_unified_data']['year_best'] = max(year_multiple, key=len)
        aarecord['file_unified_data']['year_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(year_multiple) if s != aarecord['file_unified_data']['year_best']]
        if len(aarecord['file_unified_data']['year_additional']) == 0:
            del aarecord['file_unified_data']['year_additional']

        comments_multiple = [
            ((aarecord['lgrsnf_book'] or {}).get('commentary') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('commentary') or '').strip(),
            ' -- '.join(filter(len, [((aarecord['lgrsnf_book'] or {}).get('library') or '').strip(), (aarecord['lgrsnf_book'] or {}).get('issue', '').strip()])),
            ' -- '.join(filter(len, [((aarecord['lgrsfic_book'] or {}).get('library') or '').strip(), (aarecord['lgrsfic_book'] or {}).get('issue', '').strip()])),
            ' -- '.join(filter(len, [*((aarecord['lgli_file'] or {}).get('descriptions_mapped') or {}).get('descriptions_mapped.library', []), *(aarecord['lgli_file'] or {}).get('descriptions_mapped', {}).get('descriptions_mapped.library_issue', [])])),
            ((lgli_single_edition or {}).get('commentary') or '').strip(),
            ((lgli_single_edition or {}).get('editions_add_info') or '').strip(),
            ((lgli_single_edition or {}).get('commentary') or '').strip(),
            *[note.strip() for note in (((lgli_single_edition or {}).get('descriptions_mapped') or {}).get('descriptions_mapped.notes') or [])],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('combined_comments') or '').strip(),
        ]
        aarecord['file_unified_data']['comments_best'] = max(comments_multiple, key=len)
        comments_multiple += [(edition.get('comments_normalized') or '').strip() for edition in lgli_all_editions]
        for edition in lgli_all_editions:
            comments_multiple.append((edition.get('editions_add_info') or '').strip())
            comments_multiple.append((edition.get('commentary') or '').strip())
            for note in (edition.get('descriptions_mapped') or {}).get('descriptions_mapped.notes', []):
                comments_multiple.append(note.strip())
        for ol_book_dict in aarecord['ol']:
            for comment in ol_book_dict.get('comments_normalized') or []:
                comments_multiple.append(comment.strip())
        if aarecord['file_unified_data']['comments_best'] == '':
            aarecord['file_unified_data']['comments_best'] = max(comments_multiple, key=len)
        aarecord['file_unified_data']['comments_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(comments_multiple) if s != aarecord['file_unified_data']['comments_best']]
        if len(aarecord['file_unified_data']['comments_additional']) == 0:
            del aarecord['file_unified_data']['comments_additional']

        stripped_description_multiple = [
            ((aarecord['lgrsnf_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((aarecord['lgrsfic_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((lgli_single_edition or {}).get('stripped_description') or '').strip()[0:5000],
            ((aarecord['aac_zlib3_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((aarecord['zlib_book'] or {}).get('stripped_description') or '').strip()[0:5000],
        ]
        aarecord['file_unified_data']['stripped_description_best'] = max(stripped_description_multiple, key=len)
        stripped_description_multiple += [(edition.get('stripped_description') or '').strip()[0:5000] for edition in lgli_all_editions]
        stripped_description_multiple += [ol_book_dict['stripped_description'].strip()[0:5000] for ol_book_dict in aarecord['ol']]
        stripped_description_multiple += [(isbndb['json'].get('synopsis') or '').strip()[0:5000] for isbndb in aarecord['isbndb']]
        stripped_description_multiple += [(isbndb['json'].get('overview') or '').strip()[0:5000] for isbndb in aarecord['isbndb']]
        for oclc in aarecord['oclc']:
            stripped_description_multiple += oclc['aa_oclc_derived']['stripped_description_multiple']
        if aarecord['file_unified_data']['stripped_description_best'] == '':
            aarecord['file_unified_data']['stripped_description_best'] = max(stripped_description_multiple, key=len)
        ia_descr = (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('stripped_description_and_references') or '').strip()[0:5000]
        if len(ia_descr) > 0:
            stripped_description_multiple += [ia_descr]
            aarecord['file_unified_data']['stripped_description_best'] = (aarecord['file_unified_data']['stripped_description_best'] + '\n\n' + ia_descr).strip()
        aarecord['file_unified_data']['stripped_description_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(stripped_description_multiple) if s != aarecord['file_unified_data']['stripped_description_best']]
        if len(aarecord['file_unified_data']['stripped_description_additional']) == 0:
            del aarecord['file_unified_data']['stripped_description_additional']

        aarecord['file_unified_data']['language_codes'] = combine_bcp47_lang_codes([
            ((aarecord['lgrsnf_book'] or {}).get('language_codes') or []),
            ((aarecord['lgrsfic_book'] or {}).get('language_codes') or []),
            ((lgli_single_edition or {}).get('language_codes') or []),
            ((aarecord['aac_zlib3_book'] or {}).get('language_codes') or []),
            ((aarecord['zlib_book'] or {}).get('language_codes') or []),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('language_codes') or []),
        ])
        if len(aarecord['file_unified_data']['language_codes']) == 0:
            aarecord['file_unified_data']['language_codes'] = combine_bcp47_lang_codes([(edition.get('language_codes') or []) for edition in lgli_all_editions])
        if len(aarecord['file_unified_data']['language_codes']) == 0:
            aarecord['file_unified_data']['language_codes'] = combine_bcp47_lang_codes([(ol_book_dict.get('language_codes') or []) for ol_book_dict in aarecord['ol']])
        if len(aarecord['file_unified_data']['language_codes']) == 0:
            aarecord['file_unified_data']['language_codes'] = combine_bcp47_lang_codes([(isbndb.get('language_codes') or []) for isbndb in aarecord['isbndb']])
        if len(aarecord['file_unified_data']['language_codes']) == 0:
            aarecord['file_unified_data']['language_codes'] = combine_bcp47_lang_codes([oclc['aa_oclc_derived']['language_codes'] for oclc in aarecord['oclc']])
        if len(aarecord['file_unified_data']['language_codes']) == 0:
            for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
                potential_code = get_bcp47_lang_codes_parse_substr(isbnlib.info(canonical_isbn13))
                if potential_code != '':
                    aarecord['file_unified_data']['language_codes'] = [potential_code]
                    break

        # detected_language_codes_probs = []
        # for item in language_detection:
        #     for code in get_bcp47_lang_codes(item.lang):
        #         detected_language_codes_probs.append(f"{code}: {item.prob}")
        # aarecord['file_unified_data']['detected_language_codes_probs'] = ", ".join(detected_language_codes_probs)

        aarecord['file_unified_data']['most_likely_language_code'] = ''
        if len(aarecord['file_unified_data']['language_codes']) > 0:
            aarecord['file_unified_data']['most_likely_language_code'] = aarecord['file_unified_data']['language_codes'][0]
        elif len(aarecord['file_unified_data']['stripped_description_best']) > 20:
            language_detect_string = " ".join(title_multiple) + " ".join(stripped_description_multiple)
            try:
                language_detection_data = ftlangdetect.detect(language_detect_string)
                if language_detection_data['score'] > 0.5: # Somewhat arbitrary cutoff
                    language_detection = language_detection_data['lang']
                    aarecord['file_unified_data']['most_likely_language_code'] = get_bcp47_lang_codes(language_detection)[0]
            except:
                pass

        # Duplicated from above, but with more fields now.
        aarecord['file_unified_data']['identifiers_unified'] = allthethings.utils.merge_unified_fields([
            ((aarecord['lgrsnf_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['aac_zlib3_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['zlib_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgli_file'] or {}).get('identifiers_unified') or {}),
            *[(edition['identifiers_unified'].get('identifiers_unified') or {}) for edition in lgli_all_editions],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('identifiers_unified') or {}),
            *[isbndb['identifiers_unified'] for isbndb in aarecord['isbndb']],
            *[ol_book_dict['identifiers_unified'] for ol_book_dict in aarecord['ol']],
            *[scihub_doi['identifiers_unified'] for scihub_doi in aarecord['scihub_doi']],
            *[oclc['aa_oclc_derived']['identifiers_unified'] for oclc in aarecord['oclc']],
        ])
        aarecord['file_unified_data']['classifications_unified'] = allthethings.utils.merge_unified_fields([
            ((aarecord['lgrsnf_book'] or {}).get('classifications_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('classifications_unified') or {}),
            ((aarecord['aac_zlib3_book'] or {}).get('classifications_unified') or {}),
            ((aarecord['zlib_book'] or {}).get('classifications_unified') or {}),
            *[(edition.get('classifications_unified') or {}) for edition in lgli_all_editions],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('classifications_unified') or {}),
            *[isbndb['classifications_unified'] for isbndb in aarecord['isbndb']],
            *[ol_book_dict['classifications_unified'] for ol_book_dict in aarecord['ol']],
            *[scihub_doi['classifications_unified'] for scihub_doi in aarecord['scihub_doi']],
        ])

        aarecord['file_unified_data']['problems'] = []
        if ((aarecord['lgrsnf_book'] or {}).get('visible') or '') != '':
            aarecord['file_unified_data']['problems'].append({ 'type': 'lgrsnf_visible', 'descr': ((aarecord['lgrsnf_book'] or {}).get('visible') or ''), 'better_md5': ((aarecord['lgrsnf_book'] or {}).get('generic') or '').lower() })
        if ((aarecord['lgrsfic_book'] or {}).get('visible') or '') != '':
            aarecord['file_unified_data']['problems'].append({ 'type': 'lgrsfic_visible', 'descr': ((aarecord['lgrsfic_book'] or {}).get('visible') or ''), 'better_md5': ((aarecord['lgrsfic_book'] or {}).get('generic') or '').lower() })
        if ((aarecord['lgli_file'] or {}).get('visible') or '') != '':
            aarecord['file_unified_data']['problems'].append({ 'type': 'lgli_visible', 'descr': ((aarecord['lgli_file'] or {}).get('visible') or ''), 'better_md5': ((aarecord['lgli_file'] or {}).get('generic') or '').lower() })
        if ((aarecord['lgli_file'] or {}).get('broken') or '') in [1, "1", "y", "Y"]:
            aarecord['file_unified_data']['problems'].append({ 'type': 'lgli_broken', 'descr': ((aarecord['lgli_file'] or {}).get('broken') or ''), 'better_md5': ((aarecord['lgli_file'] or {}).get('generic') or '').lower() })
        if (aarecord['zlib_book'] and (aarecord['zlib_book']['in_libgen'] or False) == False and (aarecord['zlib_book']['pilimi_torrent'] or '') == ''):
            aarecord['file_unified_data']['problems'].append({ 'type': 'zlib_missing', 'descr': '', 'better_md5': '' })

        aarecord['file_unified_data']['content_type'] = 'book_unknown'
        if aarecord['lgli_file'] is not None:
            if aarecord['lgli_file']['libgen_topic'] == 'l':
                aarecord['file_unified_data']['content_type'] = 'book_nonfiction'
            if aarecord['lgli_file']['libgen_topic'] == 'f':
                aarecord['file_unified_data']['content_type'] = 'book_fiction'
            if aarecord['lgli_file']['libgen_topic'] == 'r':
                aarecord['file_unified_data']['content_type'] = 'book_fiction'
            if aarecord['lgli_file']['libgen_topic'] == 'a':
                aarecord['file_unified_data']['content_type'] = 'journal_article'
            if aarecord['lgli_file']['libgen_topic'] == 's':
                aarecord['file_unified_data']['content_type'] = 'standards_document'
            if aarecord['lgli_file']['libgen_topic'] == 'm':
                aarecord['file_unified_data']['content_type'] = 'magazine'
            if aarecord['lgli_file']['libgen_topic'] == 'c':
                aarecord['file_unified_data']['content_type'] = 'book_comic'
        if aarecord['lgrsnf_book'] and (not aarecord['lgrsfic_book']):
            aarecord['file_unified_data']['content_type'] = 'book_nonfiction'
        if (not aarecord['lgrsnf_book']) and aarecord['lgrsfic_book']:
            aarecord['file_unified_data']['content_type'] = 'book_fiction'
        ia_content_type = (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('content_type') or 'book_unknown')
        if (aarecord['file_unified_data']['content_type'] == 'book_unknown') and (ia_content_type != 'book_unknown'):
            aarecord['file_unified_data']['content_type'] = ia_content_type
        if (aarecord['file_unified_data']['content_type'] == 'book_unknown') and (len(aarecord['scihub_doi']) > 0):
            aarecord['file_unified_data']['content_type'] = 'journal_article'
        if (aarecord['file_unified_data']['content_type'] == 'book_unknown') and (len(aarecord['oclc']) > 0):
            for oclc in aarecord['oclc']:
                if (aarecord_id_split[0] == 'oclc') or (oclc['aa_oclc_derived']['content_type'] != 'other'):
                    aarecord['file_unified_data']['content_type'] = oclc['aa_oclc_derived']['content_type']
                    break

        if aarecord['lgrsnf_book'] is not None:
            aarecord['lgrsnf_book'] = {
                'id': aarecord['lgrsnf_book']['id'],
                'md5': aarecord['lgrsnf_book']['md5'],
            }
        if aarecord['lgrsfic_book'] is not None:
            aarecord['lgrsfic_book'] = {
                'id': aarecord['lgrsfic_book']['id'],
                'md5': aarecord['lgrsfic_book']['md5'],
            }
        if aarecord['lgli_file'] is not None:
            aarecord['lgli_file'] = {
                'f_id': aarecord['lgli_file']['f_id'],
                'md5': aarecord['lgli_file']['md5'],
                'libgen_topic': aarecord['lgli_file']['libgen_topic'],
                'libgen_id': aarecord['lgli_file']['libgen_id'],
                'fiction_id': aarecord['lgli_file']['fiction_id'],
                'fiction_rus_id': aarecord['lgli_file']['fiction_rus_id'],
                'comics_id': aarecord['lgli_file']['comics_id'],
                'scimag_id': aarecord['lgli_file']['scimag_id'],
                'standarts_id': aarecord['lgli_file']['standarts_id'],
                'magz_id': aarecord['lgli_file']['magz_id'],
                'scimag_archive_path': aarecord['lgli_file']['scimag_archive_path'],
            }
        if aarecord['zlib_book'] is not None:
            aarecord['zlib_book'] = {
                'zlibrary_id': aarecord['zlib_book']['zlibrary_id'],
                'md5': aarecord['zlib_book']['md5'],
                'md5_reported': aarecord['zlib_book']['md5_reported'],
                'filesize': aarecord['zlib_book']['filesize'],
                'filesize_reported': aarecord['zlib_book']['filesize_reported'],
                'in_libgen': aarecord['zlib_book']['in_libgen'],
                'pilimi_torrent': aarecord['zlib_book']['pilimi_torrent'],
            }
        if aarecord['aac_zlib3_book'] is not None:
            aarecord['aac_zlib3_book'] = {
                'zlibrary_id': aarecord['aac_zlib3_book']['zlibrary_id'],
                'md5': aarecord['aac_zlib3_book']['md5'],
                'md5_reported': aarecord['aac_zlib3_book']['md5_reported'],
                'filesize_reported': aarecord['aac_zlib3_book']['filesize_reported'],
                'file_data_folder': aarecord['aac_zlib3_book']['file_data_folder'],
                'record_aacid': aarecord['aac_zlib3_book']['record_aacid'],
                'file_aacid': aarecord['aac_zlib3_book']['file_aacid'],
            }
        if aarecord['aa_lgli_comics_2022_08_file'] is not None:
            aarecord ['aa_lgli_comics_2022_08_file'] = {
                'path': aarecord['aa_lgli_comics_2022_08_file']['path'],
                'md5': aarecord['aa_lgli_comics_2022_08_file']['md5'],
                'filesize': aarecord['aa_lgli_comics_2022_08_file']['filesize'],
            }
        if aarecord['ia_record'] is not None:
            aarecord['ia_record'] = {
                'ia_id': aarecord['ia_record']['ia_id'],
                'has_thumb': aarecord['ia_record']['has_thumb'],
                'aa_ia_file': {
                    'type': aarecord['ia_record']['aa_ia_file']['type'],
                    'filesize': aarecord['ia_record']['aa_ia_file']['filesize'],
                    'extension': aarecord['ia_record']['aa_ia_file']['extension'],
                    'ia_id': aarecord['ia_record']['aa_ia_file']['ia_id'],
                    'aacid': aarecord['ia_record']['aa_ia_file'].get('aacid'),
                    'data_folder': aarecord['ia_record']['aa_ia_file'].get('data_folder'),
                } if (aarecord['ia_record'].get('aa_ia_file') is not None) else None,
                'aa_ia_derived': {
                    'printdisabled_only': aarecord['ia_record']['aa_ia_derived']['printdisabled_only'],
                }
            }
        aarecord['isbndb'] = aarecord.get('isbndb') or []
        for index, item in enumerate(aarecord['isbndb']):
            aarecord['isbndb'][index] = {
                'isbn13': aarecord['isbndb'][index]['isbn13'],
            }
        aarecord['ol'] = aarecord.get('ol') or []
        for index, item in enumerate(aarecord['ol']):
            aarecord['ol'][index] = {
                'ol_edition': aarecord['ol'][index]['ol_edition'],
            }
        aarecord['scihub_doi'] = aarecord.get('scihub_doi') or []
        for index, item in enumerate(aarecord['scihub_doi']):
            aarecord['scihub_doi'][index] = {
                'doi': aarecord['scihub_doi'][index]['doi'],
            }
        aarecord['oclc'] = aarecord.get('oclc') or []
        for index, item in enumerate(aarecord['oclc']):
            aarecord['oclc'][index] = {
                'oclc_id': aarecord['oclc'][index]['oclc_id'],
            }

        # Even though `additional` is only for computing real-time stuff,
        # we'd like to cache some fields for in the search results.
        with force_locale('en'):
            additional = get_additional_for_aarecord(aarecord)
            aarecord['file_unified_data']['has_aa_downloads'] = additional['has_aa_downloads']
            aarecord['file_unified_data']['has_aa_exclusive_downloads'] = additional['has_aa_exclusive_downloads']

        initial_search_text = "\n".join(list(dict.fromkeys([
            aarecord['file_unified_data']['title_best'][:1000],
            aarecord['file_unified_data']['author_best'][:1000],
            aarecord['file_unified_data']['edition_varia_best'][:1000],
            aarecord['file_unified_data']['publisher_best'][:1000],
            aarecord['file_unified_data']['original_filename_best_name_only'][:1000],
            aarecord['id'][:1000],
            # TODO: Add description maybe?
        ])))
        split_search_text = set(initial_search_text.split())
        normalized_search_terms = initial_search_text.replace('.', ' ').replace(':', ' ').replace('_', ' ').replace('/', ' ').replace('\\', ' ')
        filtered_normalized_search_terms = ' '.join([term for term in normalized_search_terms.split() if term not in split_search_text])
        more_search_text = "\n".join([
            aarecord['file_unified_data']['extension_best'],
            *[f"{key}:{item}" for key, items in aarecord['file_unified_data']['identifiers_unified'].items() for item in items],
            *[f"{key}:{item}" for key, items in aarecord['file_unified_data']['classifications_unified'].items() for item in items],
            aarecord_id,
        ])
        search_text = f"{initial_search_text}\n\n{filtered_normalized_search_terms}\n\n{more_search_text}"

        aarecord['search_only_fields'] = {
            'search_filesize': aarecord['file_unified_data']['filesize_best'],
            'search_year': aarecord['file_unified_data']['year_best'],
            'search_extension': aarecord['file_unified_data']['extension_best'],
            'search_content_type': aarecord['file_unified_data']['content_type'],
            'search_most_likely_language_code': aarecord['file_unified_data']['most_likely_language_code'],
            'search_isbn13': (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []),
            'search_doi': (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []),
            'search_text': search_text,
            'search_access_types': [
                *(['external_download'] if any([((aarecord.get(field) is not None) and (type(aarecord[field]) != list or len(aarecord[field]) > 0)) for field in ['lgrsnf_book', 'lgrsfic_book', 'lgli_file', 'zlib_book', 'aac_zlib3_book', 'scihub_doi']]) else []),
                *(['external_borrow'] if (aarecord.get('ia_record') and (not aarecord['ia_record']['aa_ia_derived']['printdisabled_only'])) else []),
                *(['external_borrow_printdisabled'] if (aarecord.get('ia_record') and (aarecord['ia_record']['aa_ia_derived']['printdisabled_only'])) else []),
                *(['aa_download'] if aarecord['file_unified_data']['has_aa_downloads'] == 1 else []),
                *(['meta_explore'] if aarecord_id_split[0] in ['isbn', 'ol', 'oclc'] else []),
            ],
            'search_record_sources': list(set([
                *(['lgrs']      if aarecord['lgrsnf_book'] is not None else []),
                *(['lgrs']      if aarecord['lgrsfic_book'] is not None else []),
                *(['lgli']      if aarecord['lgli_file'] is not None else []),
                *(['zlib']      if aarecord['zlib_book'] is not None else []),
                *(['zlib']      if aarecord['aac_zlib3_book'] is not None else []),
                *(['lgli']      if aarecord['aa_lgli_comics_2022_08_file'] is not None else []),
                *(['ia']        if aarecord['ia_record'] is not None else []),
                *(['scihub']    if len(aarecord['scihub_doi']) > 0 else []),
                *(['isbndb']    if (aarecord_id_split[0] == 'isbn' and len(aarecord['isbndb'] or []) > 0) else []),
                *(['ol']        if (aarecord_id_split[0] == 'ol' and len(aarecord['ol'] or []) > 0) else []),
                *(['oclc']      if (aarecord_id_split[0] == 'oclc' and len(aarecord['oclc'] or []) > 0) else []),
            ])),
        }

        # At the very end
        aarecord['search_only_fields']['search_score_base'] = float(aarecord_score_base(aarecord))
        aarecord['search_only_fields']['search_score_base_rank'] = aarecord['search_only_fields']['search_score_base']

    return aarecords

def get_md5_problem_type_mapping():
    return { 
        "lgrsnf_visible":  gettext("common.md5_problem_type_mapping.lgrsnf_visible"),
        "lgrsfic_visible": gettext("common.md5_problem_type_mapping.lgrsfic_visible"),
        "lgli_visible":    gettext("common.md5_problem_type_mapping.lgli_visible"),
        "lgli_broken":     gettext("common.md5_problem_type_mapping.lgli_broken"),
        "zlib_missing":    gettext("common.md5_problem_type_mapping.zlib_missing"),
    }

def get_md5_content_type_mapping(display_lang):
    with force_locale(display_lang):
        return {
            "book_unknown":       gettext("common.md5_content_type_mapping.book_unknown"),
            "book_nonfiction":    gettext("common.md5_content_type_mapping.book_nonfiction"),
            "book_fiction":       gettext("common.md5_content_type_mapping.book_fiction"),
            "journal_article":    gettext("common.md5_content_type_mapping.journal_article"),
            "standards_document": gettext("common.md5_content_type_mapping.standards_document"),
            "magazine":           gettext("common.md5_content_type_mapping.magazine"),
            "book_comic":         gettext("common.md5_content_type_mapping.book_comic"),
            "musical_score":      "Musical score", # TODO:TRANSLATE
            "other":              "Other", # TODO:TRANSLATE
        }

def get_access_types_mapping(display_lang):
    with force_locale(display_lang):
        return {
            "aa_download": gettext("common.access_types_mapping.aa_download"),
            "external_download": gettext("common.access_types_mapping.external_download"),
            "external_borrow": gettext("common.access_types_mapping.external_borrow"),
            "external_borrow_printdisabled": gettext("common.access_types_mapping.external_borrow_printdisabled"),
            "meta_explore": gettext("common.access_types_mapping.meta_explore"),
        }

def get_record_sources_mapping(display_lang):
    with force_locale(display_lang):
        return {
            "lgrs": gettext("common.record_sources_mapping.lgrs"),
            "lgli": gettext("common.record_sources_mapping.lgli"),
            "zlib": gettext("common.record_sources_mapping.zlib"),
            "ia": gettext("common.record_sources_mapping.ia"),
            "isbndb": gettext("common.record_sources_mapping.isbndb"),
            "ol": gettext("common.record_sources_mapping.ol"),
            "scihub": gettext("common.record_sources_mapping.scihub"),
            "oclc": "OCLC (WorldCat)", # TODO:TRANSLATE
        }

def format_filesize(num):
    if num < 100000:
        return f"0.1MB"
    elif num < 1000000:
        return f"{num/1000000:3.1f}MB"
    else:
        for unit in ["", "KB", "MB", "GB", "TB", "PB", "EB", "ZB"]:
            if abs(num) < 1000.0:
                return f"{num:3.1f}{unit}"
            num /= 1000.0
        return f"{num:.1f}YB"

def add_partner_servers(path, modifier, aarecord, additional):
    additional['has_aa_downloads'] = 1
    targeted_seconds = 90
    if modifier == 'aa_exclusive':
        targeted_seconds = 180
        additional['has_aa_exclusive_downloads'] = 1
    if modifier == 'scimag':
        targeted_seconds = 3
    # When changing the domains, don't forget to change md5_fast_download and md5_slow_download.
    for _ in range(len(allthethings.utils.FAST_DOWNLOAD_DOMAINS)):
        additional['fast_partner_urls'].append((gettext("common.md5.servers.fast_partner", number=len(additional['fast_partner_urls'])+1), '/fast_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/' + str(len(additional['fast_partner_urls'])), gettext("common.md5.servers.no_browser_verification") if len(additional['fast_partner_urls']) == 0 else ''))
    for _ in range(len(allthethings.utils.SLOW_DOWNLOAD_DOMAINS)):
        additional['slow_partner_urls'].append((gettext("common.md5.servers.slow_partner", number=len(additional['slow_partner_urls'])+1), '/slow_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/' + str(len(additional['slow_partner_urls'])), gettext("common.md5.servers.browser_verification_unlimited", a_browser=' href="/browser_verification" ') if len(additional['slow_partner_urls']) == 0 else ''))
    additional['partner_url_paths'].append({ 'path': path, 'targeted_seconds': targeted_seconds })

def max_length_with_word_boundary(sentence, max_len):
    str_split = sentence.split(' ')
    output_index = 0
    output_total = 0
    for item in str_split:
        item = item.strip()
        len_item = len(item)+1 # Also count a trailing space
        if output_total+len_item-1 > max_len: # But don't count the very last trailing space here
            break
        output_index += 1
        output_total += len_item
    if output_index == 0:
        return sentence[0:max_len].strip()
    else:
        return ' '.join(str_split[0:output_index]).strip()

def get_additional_for_aarecord(aarecord):
    aarecord_id_split = aarecord['id'].split(':', 1)

    additional = {}
    additional['path'] = '/' + aarecord_id_split[0].replace('/isbn/', '/isbndb/') + '/' + aarecord_id_split[1]
    additional['most_likely_language_name'] = (get_display_name_for_lang(aarecord['file_unified_data'].get('most_likely_language_code', None) or '', allthethings.utils.get_base_lang_code(get_locale())) if aarecord['file_unified_data'].get('most_likely_language_code', None) else '')

    additional['codes'] = []
    for key, values in aarecord['file_unified_data'].get('identifiers_unified', {}).items():
        for value in values:
            masked_isbn = ''
            if key in ['isbn10', 'isbn13']:
                masked_isbn = isbnlib.mask(value)

            additional['codes'].append({
                'key': key,
                'value': value,
                'masked_isbn': masked_isbn,
                'type': 'identifier',
                'info': allthethings.utils.UNIFIED_IDENTIFIERS.get(key) or {},
            })
    for key, values in aarecord['file_unified_data'].get('classifications_unified', {}).items():
        for value in values:
            additional['codes'].append({
                'key': key,
                'value': value,
                'type': 'classification',
                'info': allthethings.utils.UNIFIED_CLASSIFICATIONS.get(key) or {},
            })
    CODES_PRIORITY = ['isbn13', 'isbn10', 'doi', 'issn', 'udc', 'oclc', 'ol', 'ocaid', 'asin']
    additional['codes'].sort(key=lambda item: (CODES_PRIORITY.index(item['key']) if item['key'] in CODES_PRIORITY else 100))

    additional['top_box'] = {
        'meta_information': [item for item in [
                aarecord['file_unified_data'].get('title_best', None) or '',
                aarecord['file_unified_data'].get('author_best', None) or '',
                (aarecord['file_unified_data'].get('stripped_description_best', None) or '')[0:100],
                aarecord['file_unified_data'].get('publisher_best', None) or '',
                aarecord['file_unified_data'].get('edition_varia_best', None) or '',
                aarecord['file_unified_data'].get('original_filename_best_name_only', None) or '',
            ] if item != ''],
        'cover_url': (aarecord['file_unified_data'].get('cover_url_best', None) or '').replace('https://covers.zlibcdn2.com/', 'https://static.1lib.sk/'),
        'top_row': ", ".join([item for item in [
                additional['most_likely_language_name'],
                aarecord['file_unified_data'].get('extension_best', None) or '',
                format_filesize(aarecord['file_unified_data'].get('filesize_best', None) or 0) if aarecord['file_unified_data'].get('filesize_best', None) else '',
                aarecord['file_unified_data'].get('original_filename_best_name_only', None) or '',
                aarecord_id_split[1] if aarecord_id_split[0] in ['ia', 'ol'] else '',
                f"ISBNdb {aarecord_id_split[1]}" if aarecord_id_split[0] == 'isbn' else '',
                f"OCLC {aarecord_id_split[1]}" if aarecord_id_split[0] == 'oclc' else '',
            ] if item != '']),
        'title': aarecord['file_unified_data'].get('title_best', None) or '',
        'publisher_and_edition': ", ".join([item for item in [
                aarecord['file_unified_data'].get('publisher_best', None) or '',
                aarecord['file_unified_data'].get('edition_varia_best', None) or '',
            ] if item != '']),
        'author': aarecord['file_unified_data'].get('author_best', None) or '',
        'description': aarecord['file_unified_data'].get('stripped_description_best', None) or '',
    }

    filename_info = [item for item in [
            max_length_with_word_boundary(aarecord['file_unified_data'].get('title_best', None) or aarecord['file_unified_data'].get('original_filename_best_name_only', None) or '', 60),
            max_length_with_word_boundary(aarecord['file_unified_data'].get('author_best', None) or '', 60),
            max_length_with_word_boundary(aarecord['file_unified_data'].get('edition_varia_best', None) or '', 60),
            max_length_with_word_boundary(aarecord['file_unified_data'].get('publisher_best', None) or '', 60),
        ] if item != '']
    filename_slug = max_length_with_word_boundary(" -- ".join(filename_info), 150)
    if filename_slug.endswith(' --'):
        filename_slug = filename_slug[0:-len(' --')]
    filename_extension = aarecord['file_unified_data'].get('extension_best', None) or ''    
    filename_code = ''
    for code in additional['codes']:
        if code['key'] in ['isbn13', 'isbn10', 'doi', 'issn']:
            filename_code = f" -- {code['value']}"
            break
    additional['filename'] = urllib.parse.quote(f"{filename_slug}{filename_code} -- {aarecord['id'].split(':', 1)[1]} -- Anna’s Archive.{filename_extension}", safe='')

    additional['download_urls'] = []
    additional['fast_partner_urls'] = []
    additional['slow_partner_urls'] = []
    additional['partner_url_paths'] = []
    additional['has_aa_downloads'] = 0
    additional['has_aa_exclusive_downloads'] = 0
    shown_click_get = False
    linked_dois = set()

    for scihub_doi in aarecord.get('scihub_doi') or []:
        doi = scihub_doi['doi']
        additional['download_urls'].append((gettext('page.md5.box.download.scihub', doi=doi), f"https://sci-hub.ru/{doi}", ""))
        linked_dois.add(doi)
    if (aarecord.get('ia_record') is not None) and (aarecord['ia_record'].get('aa_ia_file') is not None):
        ia_id = aarecord['ia_record']['aa_ia_file']['ia_id']
        extension = aarecord['ia_record']['aa_ia_file']['extension']
        ia_file_type = aarecord['ia_record']['aa_ia_file']['type']
        if ia_file_type == 'acsm':
            directory = 'other'
            if bool(re.match(r"^[a-z]", ia_id)):
                directory = ia_id[0]
            partner_path = f"u/annas-archive-ia-2023-06-acsm/{directory}/{ia_id}.{extension}"
        elif ia_file_type == 'lcpdf':
            directory = 'other'
            if ia_id.startswith('per_c'):
                directory = 'per_c'
            elif ia_id.startswith('per_w'):
                directory = 'per_w'
            elif ia_id.startswith('per_'):
                directory = 'per_'
            elif bool(re.match(r"^[a-z]", ia_id)):
                directory = ia_id[0]
            partner_path = f"u/annas-archive-ia-2023-06-lcpdf/{directory}/{ia_id}.{extension}"
        elif ia_file_type == 'ia2_acsmpdf':
            partner_path = make_temp_anon_aac_path("o/ia2_acsmpdf_files", aarecord['ia_record']['aa_ia_file']['aacid'], aarecord['ia_record']['aa_ia_file']['data_folder'])
        else:
            raise Exception(f"Unknown ia_record file type: {ia_file_type}")
        add_partner_servers(partner_path, 'aa_exclusive', aarecord, additional)
    if aarecord.get('aa_lgli_comics_2022_08_file') is not None:
        if aarecord['aa_lgli_comics_2022_08_file']['path'].startswith('libgen_comics/comics'):
            stripped_path = urllib.parse.quote(aarecord['aa_lgli_comics_2022_08_file']['path'][len('libgen_comics/'):])
            partner_path = f"a/comics_2022_08/{stripped_path}"
            add_partner_servers(partner_path, 'aa_exclusive', aarecord, additional)
        if aarecord['aa_lgli_comics_2022_08_file']['path'].startswith('libgen_comics/repository/'):
            stripped_path = urllib.parse.quote(aarecord['aa_lgli_comics_2022_08_file']['path'][len('libgen_comics/repository/'):])
            partner_path = f"a/c_2022_12_thousand_dirs/{stripped_path}"
            add_partner_servers(partner_path, 'aa_exclusive', aarecord, additional)
        if aarecord['aa_lgli_comics_2022_08_file']['path'].startswith('libgen_magz/repository/'):
            stripped_path = urllib.parse.quote(aarecord['aa_lgli_comics_2022_08_file']['path'][len('libgen_magz/repository/'):])
            partner_path = f"a/c_2022_12_thousand_dirs_magz/{stripped_path}"
            add_partner_servers(partner_path, 'aa_exclusive', aarecord, additional)
    if aarecord.get('lgrsnf_book') is not None:
        lgrsnf_thousands_dir = (aarecord['lgrsnf_book']['id'] // 1000) * 1000
        if lgrsnf_thousands_dir <= 3730000:
            lgrsnf_path = f"e/lgrsnf/{lgrsnf_thousands_dir}/{aarecord['lgrsnf_book']['md5'].lower()}"
            add_partner_servers(lgrsnf_path, '', aarecord, additional)

        additional['download_urls'].append((gettext('page.md5.box.download.lgrsnf'), f"http://library.lol/main/{aarecord['lgrsnf_book']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if aarecord.get('lgrsfic_book') is not None:
        lgrsfic_thousands_dir = (aarecord['lgrsfic_book']['id'] // 1000) * 1000
        if lgrsfic_thousands_dir <= 2715000:
            lgrsfic_path = f"e/lgrsfic/{lgrsfic_thousands_dir}/{aarecord['lgrsfic_book']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
            add_partner_servers(lgrsfic_path, '', aarecord, additional)

        additional['download_urls'].append((gettext('page.md5.box.download.lgrsfic'), f"http://library.lol/fiction/{aarecord['lgrsfic_book']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if aarecord.get('lgli_file') is not None:
        lglific_id = aarecord['lgli_file']['fiction_id']
        if lglific_id > 0:
            lglific_thousands_dir = (lglific_id // 1000) * 1000
            if lglific_thousands_dir >= 2201000 and lglific_thousands_dir <= 4259000:
                lglific_path = f"e/lglific/{lglific_thousands_dir}/{aarecord['lgli_file']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
                add_partner_servers(lglific_path, '', aarecord, additional)
        scimag_id = aarecord['lgli_file']['scimag_id']
        if scimag_id > 0 and scimag_id <= 87599999: # 87637042 seems the max now in the libgenli db
            scimag_tenmillion_dir = (scimag_id // 10000000)
            scimag_filename = urllib.parse.quote(aarecord['lgli_file']['scimag_archive_path'].replace('\\', '/'))
            scimag_path = f"i/scimag/{scimag_tenmillion_dir}/{scimag_filename}"
            add_partner_servers(scimag_path, 'scimag', aarecord, additional)

        additional['download_urls'].append((gettext('page.md5.box.download.lgli'), f"http://libgen.li/ads.php?md5={aarecord['lgli_file']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if len(aarecord.get('ipfs_infos') or []) > 0:
        additional['download_urls'].append((gettext('page.md5.box.download.ipfs_gateway', num=1), f"https://cloudflare-ipfs.com/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename']}", gettext('page.md5.box.download.ipfs_gateway_extra')))
        additional['download_urls'].append((gettext('page.md5.box.download.ipfs_gateway', num=2), f"https://ipfs.io/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename']}", ""))
        additional['download_urls'].append((gettext('page.md5.box.download.ipfs_gateway', num=3), f"https://gateway.pinata.cloud/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename']}", ""))
    if aarecord.get('zlib_book') is not None and len(aarecord['zlib_book']['pilimi_torrent'] or '') > 0:
        zlib_path = make_temp_anon_zlib_path(aarecord['zlib_book']['zlibrary_id'], aarecord['zlib_book']['pilimi_torrent'])
        add_partner_servers(zlib_path, 'aa_exclusive' if (len(additional['fast_partner_urls']) == 0) else '', aarecord, additional)
    if aarecord.get('aac_zlib3_book') is not None:
        zlib_path = make_temp_anon_aac_path("o/zlib3_files", aarecord['aac_zlib3_book']['file_aacid'], aarecord['aac_zlib3_book']['file_data_folder'])
        add_partner_servers(zlib_path, 'aa_exclusive' if (len(additional['fast_partner_urls']) == 0) else '', aarecord, additional)
    if aarecord.get('zlib_book') is not None:
        # additional['download_urls'].append((gettext('page.md5.box.download.zlib_tor'), f"http://loginzlib2vrak5zzpcocc3ouizykn6k5qecgj2tzlnab5wcbqhembyd.onion/md5/{aarecord['zlib_book']['md5_reported'].lower()}", gettext('page.md5.box.download.zlib_tor_extra')))
        additional['download_urls'].append(("Z-Library", f"https://1lib.sk/md5/{aarecord['zlib_book']['md5_reported'].lower()}", ""))
    if aarecord.get('aac_zlib3_book') is not None:
        # additional['download_urls'].append((gettext('page.md5.box.download.zlib_tor'), f"http://loginzlib2vrak5zzpcocc3ouizykn6k5qecgj2tzlnab5wcbqhembyd.onion/md5/{aarecord['aac_zlib3_book']['md5_reported'].lower()}", gettext('page.md5.box.download.zlib_tor_extra')))
        additional['download_urls'].append(("Z-Library", f"https://1lib.sk/md5/{aarecord['aac_zlib3_book']['md5_reported'].lower()}", ""))
    if aarecord.get('ia_record') is not None:
        ia_id = aarecord['ia_record']['ia_id']
        printdisabled_only = aarecord['ia_record']['aa_ia_derived']['printdisabled_only']
        additional['download_urls'].append((gettext('page.md5.box.download.ia_borrow'), f"https://archive.org/details/{ia_id}", gettext('page.md5.box.download.print_disabled_only') if printdisabled_only else ''))
    for doi in (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []):
        if doi not in linked_dois:
            additional['download_urls'].append((gettext('page.md5.box.download.scihub', doi=doi), f"https://sci-hub.ru/{doi}", gettext('page.md5.box.download.scihub_maybe')))
    if aarecord_id_split[0] == 'md5':
        additional['download_urls'].append((gettext('page.md5.box.download.bulk_torrents'), "/datasets", gettext('page.md5.box.download.experts_only')))
    if aarecord_id_split[0] == 'isbn':
        additional['download_urls'].append((gettext('page.md5.box.download.aa_isbn'), f'/search?q="isbn13:{aarecord_id_split[1]}"', ""))
        additional['download_urls'].append((gettext('page.md5.box.download.other_isbn'), f"https://en.wikipedia.org/wiki/Special:BookSources?isbn={aarecord_id_split[1]}", ""))
        if len(aarecord.get('isbndb') or []) > 0:
            additional['download_urls'].append((gettext('page.md5.box.download.original_isbndb'), f"https://isbndb.com/book/{aarecord_id_split[1]}", ""))
    if aarecord_id_split[0] == 'ol':
        additional['download_urls'].append((gettext('page.md5.box.download.aa_openlib'), f'/search?q="ol:{aarecord_id_split[1]}"', ""))
        if len(aarecord.get('ol') or []) > 0:
            additional['download_urls'].append((gettext('page.md5.box.download.original_openlib'), f"https://openlibrary.org/books/{aarecord_id_split[1]}", ""))
    if aarecord_id_split[0] == 'oclc':
        # TODO:TRANSLATE
        additional['download_urls'].append(("Search Anna’s Archive for OCLC (WorldCat) number", f'/search?q="oclc:{aarecord_id_split[1]}"', ""))
        # TODO:TRANSLATE
        additional['download_urls'].append(("Find original record in WorldCat", f"https://worldcat.org/title/{aarecord_id_split[1]}", ""))
    additional['download_urls'] = additional['slow_partner_urls'] + additional['download_urls']

    scidb_info = allthethings.utils.scidb_info(aarecord, additional)
    if scidb_info is not None:
        additional['fast_partner_urls'] = [(gettext('page.md5.box.download.scidb'), f"/scidb/{scidb_info['doi']}", gettext('common.md5.servers.no_browser_verification'))] + additional['fast_partner_urls']
        additional['download_urls'] = [(gettext('page.md5.box.download.scidb'), f"/scidb/{scidb_info['doi']}", "")] + additional['download_urls']

    return additional

def add_additional_to_aarecord(aarecord):
    return { **aarecord, 'additional': get_additional_for_aarecord(aarecord) }


@page.get("/md5/<string:md5_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def md5_page(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]
    return render_aarecord(f"md5:{canonical_md5}")

@page.get("/ia/<string:ia_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def ia_page(ia_input):
    with Session(engine) as session:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        count = cursor.execute('SELECT md5 FROM aa_ia_2023_06_files WHERE ia_id = %(ia_input)s LIMIT 1', { "ia_input": ia_input })
        if count > 0:
            md5 = cursor.fetchone()['md5']
            return redirect(f"/md5/{md5}", code=301)

        return render_aarecord(f"ia:{ia_input}")

@page.get("/isbn/<string:isbn_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def isbn_page(isbn_input):
    return redirect(f"/isbndb/{isbn_input}", code=302)

@page.get("/isbndb/<string:isbn_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def isbndb_page(isbn_input):
    return render_aarecord(f"isbn:{isbn_input}")

@page.get("/ol/<string:ol_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def ol_page(ol_input):
    return render_aarecord(f"ol:{ol_input}")

@page.get("/doi/<path:doi_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def doi_page(doi_input):
    return render_aarecord(f"doi:{doi_input}")

@page.get("/oclc/<path:oclc_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def oclc_page(oclc_input):
    return render_aarecord(f"oclc:{oclc_input}")

def render_aarecord(record_id):
    with Session(engine) as session:
        ids = [record_id]
        if not allthethings.utils.validate_aarecord_ids(ids):
            return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=record_id)

        aarecords = get_aarecords_elasticsearch(ids)

        if len(aarecords) == 0:
            return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=record_id)

        aarecord = aarecords[0]

        render_fields = {
            "header_active": "home/search",
            "aarecord_id": aarecord['id'],
            "aarecord_id_split": aarecord['id'].split(':', 1),
            "aarecord": aarecord,
            "md5_problem_type_mapping": get_md5_problem_type_mapping(),
            "md5_report_type_mapping": allthethings.utils.get_md5_report_type_mapping()
        }
        return render_template("page/aarecord.html", **render_fields)

@page.get("/scidb/")
@page.post("/scidb/")
@allthethings.utils.no_cache()
def scidb_redirect_page():
    doi_input = request.args.get("doi", "").strip()
    return redirect(f"/scidb/{doi_input}", code=302)

@page.get("/scidb/<path:doi_input>")
@page.post("/scidb/<path:doi_input>")
@allthethings.utils.no_cache()
def scidb_page(doi_input):
    doi_input = doi_input.strip()

    if not doi_input.startswith('10.'):
        if '10.' in doi_input:
            return redirect(f"/scidb/{doi_input[doi_input.find('10.'):].strip()}", code=302)    
        return redirect(f"/search?q={doi_input}", code=302)

    if allthethings.utils.doi_is_isbn(doi_input):
        return redirect(f'/search?q="doi:{doi_input}"', code=302)

    fast_scidb = False
    verified = False
    if str(request.args.get("scidb_verified") or "") == "1":
        verified = True
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is not None:
        with Session(mariapersist_engine) as mariapersist_session:
            account_fast_download_info = allthethings.utils.get_account_fast_download_info(mariapersist_session, account_id)
            if account_fast_download_info is not None:
                fast_scidb = True
                verified = True
    if not verified:
        return redirect(f"/scidb/{doi_input}?scidb_verified=1", code=302)

    with Session(engine) as session:
        try:
            search_results_raw = es.search(
                index="aarecords",
                size=50,
                query={ "term": { "search_only_fields.search_doi": doi_input } },
                timeout=ES_TIMEOUT_PRIMARY,
            )
        except Exception as err:
            return redirect(f"/search?q=doi:{doi_input}", code=302)
        aarecords = [add_additional_to_aarecord(aarecord['_source']) for aarecord in search_results_raw['hits']['hits']]
        aarecords_and_infos = [(aarecord, allthethings.utils.scidb_info(aarecord)) for aarecord in aarecords if allthethings.utils.scidb_info(aarecord) is not None]
        aarecords_and_infos.sort(key=lambda aarecord_and_info: aarecord_and_info[1]['priority'])

        if len(aarecords_and_infos) == 0:
            return redirect(f"/search?q=doi:{doi_input}", code=302)

        aarecord, scidb_info = aarecords_and_infos[0]

        pdf_url = None
        download_url = None
        path_info = scidb_info['path_info']
        if path_info:
            domain = random.choice(allthethings.utils.SLOW_DOWNLOAD_DOMAINS)
            targeted_seconds_multiplier = 1.0
            minimum = 30
            maximum = 200
            if fast_scidb:
                minimum = 400
                maximum = 800
            speed = compute_download_speed(path_info['targeted_seconds']*targeted_seconds_multiplier, aarecord['file_unified_data']['filesize_best'], minimum, maximum)
            pdf_url = 'https://' + domain + '/' + allthethings.utils.make_anon_download_uri(False, speed, path_info['path'], aarecord['additional']['filename'], domain)
            download_url = 'https://' + domain + '/' + allthethings.utils.make_anon_download_uri(True, speed, path_info['path'], aarecord['additional']['filename'], domain)
        
        render_fields = {
            "header_active": "home/search",
            "aarecord_id": aarecord['id'],
            "aarecord_id_split": aarecord['id'].split(':', 1),
            "aarecord": aarecord,
            "doi_input": doi_input,
            "pdf_url": pdf_url,
            "download_url": download_url,
            "scihub_link": scidb_info['scihub_link'],
        }
        return render_template("page/scidb.html", **render_fields)

@page.get("/db/aarecord/<path:aarecord_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def md5_json(aarecord_id):
    with Session(engine) as session:
        with Session(engine) as session:
            aarecords = get_aarecords_elasticsearch([aarecord_id])
            if len(aarecords) == 0:
                return "{}", 404
            
            aarecord_comments = {
                "id": ("before", ["File from the combined collections of Anna's Archive.",
                                   "More details at https://annas-archive.org/datasets",
                                   allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
                "lgrsnf_book": ("before", ["Source data at: https://annas-archive.org/db/lgrs/nf/<id>.json"]),
                "lgrsfic_book": ("before", ["Source data at: https://annas-archive.org/db/lgrs/fic/<id>.json"]),
                "lgli_file": ("before", ["Source data at: https://annas-archive.org/db/lgli/file/<f_id>.json"]),
                "zlib_book": ("before", ["Source data at: https://annas-archive.org/db/zlib/<zlibrary_id>.json"]),
                "aac_zlib3_book": ("before", ["Source data at: https://annas-archive.org/db/aac_zlib3/<zlibrary_id>.json"]),
                "ia_record": ("before", ["Source data at: https://annas-archive.org/db/ia/<ia_id>.json"]),
                "isbndb": ("before", ["Source data at: https://annas-archive.org/db/isbndb/<isbn13>.json"]),
                "ol": ("before", ["Source data at: https://annas-archive.org/db/ol/<ol_edition>.json"]),
                "scihub_doi": ("before", ["Source data at: https://annas-archive.org/db/scihub_doi/<doi>.json"]),
                "aa_lgli_comics_2022_08_file": ("before", ["File from the Libgen.li comics backup by Anna's Archive",
                                                           "See https://annas-archive.org/datasets/libgen_li",
                                                           "No additional source data beyond what is shown here."]),
                "file_unified_data": ("before", ["Combined data by Anna's Archive from the various source collections, attempting to get pick the best field where possible."]),
                "ipfs_infos": ("before", ["Data about the IPFS files."]),
                "search_only_fields": ("before", ["Data that is used during searching."]),
                "additional": ("before", ["Data that is derived at a late stage, and not stored in the search index."]),
            }
            aarecord = add_comments_to_dict(aarecords[0], aarecord_comments)

            aarecord['additional'].pop('fast_partner_urls')
            aarecord['additional'].pop('slow_partner_urls')

            return nice_json(aarecord), {'Content-Type': 'text/json; charset=utf-8'}


@page.get("/fast_download/<string:md5_input>/<int:path_index>/<int:domain_index>")
@allthethings.utils.no_cache()
def md5_fast_download(md5_input, path_index, domain_index):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]) or canonical_md5 != md5_input:
        return redirect(f"/md5/{md5_input}", code=302)
    with Session(engine) as session:
        aarecords = get_aarecords_elasticsearch([f"md5:{canonical_md5}"])
        if len(aarecords) == 0:
            return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=md5_input)
        aarecord = aarecords[0]
        try:
            domain = allthethings.utils.FAST_DOWNLOAD_DOMAINS[domain_index]
            path_info = aarecord['additional']['partner_url_paths'][path_index]
        except:
            return redirect(f"/md5/{md5_input}", code=302)
        url = 'https://' + domain + '/' + allthethings.utils.make_anon_download_uri(False, 20000, path_info['path'], aarecord['additional']['filename'], domain)

    account_id = allthethings.utils.get_account_id(request.cookies)
    with Session(mariapersist_engine) as mariapersist_session:
        account_fast_download_info = allthethings.utils.get_account_fast_download_info(mariapersist_session, account_id)
        if account_fast_download_info is None:
            return redirect(f"/fast_download_not_member", code=302)

        if canonical_md5 not in account_fast_download_info['recently_downloaded_md5s']:
            if account_fast_download_info['downloads_left'] <= 0:
                return redirect(f"/fast_download_no_more", code=302)

            data_md5 = bytes.fromhex(canonical_md5)
            data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
            mariapersist_session.connection().execute(text('INSERT INTO mariapersist_fast_download_access (md5, ip, account_id) VALUES (:md5, :ip, :account_id)').bindparams(md5=data_md5, ip=data_ip, account_id=account_id))
            mariapersist_session.commit()

    return render_template(
        "page/partner_download.html",
        header_active="search",
        url=url,
        slow_download=False,
    )

def compute_download_speed(targeted_seconds, filesize, minimum, maximum):
    return min(maximum, max(minimum, int(filesize/1000/targeted_seconds)))

@page.get("/slow_download/<string:md5_input>/<int:path_index>/<int:domain_index>")
@allthethings.utils.no_cache()
def md5_slow_download(md5_input, path_index, domain_index):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
    account_id = allthethings.utils.get_account_id(request.cookies)

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]) or canonical_md5 != md5_input:
        return redirect(f"/md5/{md5_input}", code=302)
    with Session(engine) as session:
        with Session(mariapersist_engine) as mariapersist_session:
            aarecords = get_aarecords_elasticsearch([f"md5:{canonical_md5}"])
            if len(aarecords) == 0:
                return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=md5_input)
            aarecord = aarecords[0]
            try:
                domain = allthethings.utils.SLOW_DOWNLOAD_DOMAINS[domain_index]
                path_info = aarecord['additional']['partner_url_paths'][path_index]
            except:
                return redirect(f"/md5/{md5_input}", code=302)

            # cursor = mariapersist_session.connection().connection.cursor(pymysql.cursors.DictCursor)
            # cursor.execute('SELECT COUNT(DISTINCT md5) AS count FROM mariapersist_slow_download_access WHERE timestamp > (NOW() - INTERVAL 24 HOUR) AND SUBSTRING(ip, 1, 8) = %(data_ip)s LIMIT 1', { "data_ip": data_ip })
            # download_count_from_ip = cursor.fetchone()['count']
            minimum = 20
            maximum = 300
            targeted_seconds_multiplier = 1.0
            warning = False
            # if download_count_from_ip > 500:
            #     targeted_seconds_multiplier = 3.0
            #     minimum = 10
            #     maximum = 50
            #     warning = True
            # elif download_count_from_ip > 300:
            #     targeted_seconds_multiplier = 2.0
            #     minimum = 15
            #     maximum = 100
            #     warning = True
            # elif download_count_from_ip > 150:
            #     targeted_seconds_multiplier = 1.5
            #     minimum = 20
            #     maximum = 150
            #     warning = False

            speed = compute_download_speed(path_info['targeted_seconds']*targeted_seconds_multiplier, aarecord['file_unified_data']['filesize_best'], minimum, maximum)

            url = 'https://' + domain + '/' + allthethings.utils.make_anon_download_uri(True, speed, path_info['path'], aarecord['additional']['filename'], domain)

            data_md5 = bytes.fromhex(canonical_md5)
            mariapersist_session.connection().execute(text('INSERT IGNORE INTO mariapersist_slow_download_access (md5, ip, account_id) VALUES (:md5, :ip, :account_id)').bindparams(md5=data_md5, ip=data_ip, account_id=account_id))
            mariapersist_session.commit()

            return render_template(
                "page/partner_download.html",
                header_active="search",
                url=url,
                slow_download=True,
                warning=warning
            )

def search_query_aggs(search_index_long):
    aggs = {
        "search_content_type": { "terms": { "field": "search_only_fields.search_content_type", "size": 200 } },
        "search_extension": { "terms": { "field": "search_only_fields.search_extension", "size": 9 } },
        "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "size": 100 } },
        "search_record_sources": { "terms": { "field": "search_only_fields.search_record_sources", "size": 100 } }
    }
    if search_index_long != "aarecords_metadata":
        aggs["search_most_likely_language_code"] = { "terms": { "field": "search_only_fields.search_most_likely_language_code", "size": 50 } }
    return aggs

@functools.cache
def all_search_aggs(display_lang, search_index_long):
    search_results_raw = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[search_index_long].search(index=search_index_long, size=0, aggs=search_query_aggs(search_index_long), timeout=ES_TIMEOUT_ALL_AGG)

    all_aggregations = {}
    # Unfortunately we have to special case the "unknown language", which is currently represented with an empty string `bucket['key'] != ''`, otherwise this gives too much trouble in the UI.
    all_aggregations['search_most_likely_language_code'] = []
    if 'search_most_likely_language_code' in search_results_raw['aggregations']:
        for bucket in search_results_raw['aggregations']['search_most_likely_language_code']['buckets']:
            if bucket['key'] == '':
                all_aggregations['search_most_likely_language_code'].append({ 'key': '_empty', 'label': get_display_name_for_lang('', display_lang), 'doc_count': bucket['doc_count'] })
            else:
                all_aggregations['search_most_likely_language_code'].append({ 'key': bucket['key'], 'label': get_display_name_for_lang(bucket['key'], display_lang), 'doc_count': bucket['doc_count'] })
        all_aggregations['search_most_likely_language_code'].sort(key=lambda bucket: bucket['doc_count'] + (1000000000 if bucket['key'] == display_lang else 0), reverse=True)

    content_type_buckets = list(search_results_raw['aggregations']['search_content_type']['buckets'])
    md5_content_type_mapping = get_md5_content_type_mapping(display_lang)
    all_aggregations['search_content_type'] = [{ 'key': bucket['key'], 'label': md5_content_type_mapping[bucket['key']], 'doc_count': bucket['doc_count'] } for bucket in content_type_buckets]
    content_type_keys_present = set([bucket['key'] for bucket in content_type_buckets])
    # for key, label in md5_content_type_mapping.items():
    #     if key not in content_type_keys_present:
    #         all_aggregations['search_content_type'].append({ 'key': key, 'label': label, 'doc_count': 0 })
    search_content_type_sorting = ['book_nonfiction', 'book_fiction', 'book_unknown', 'journal_article']
    all_aggregations['search_content_type'].sort(key=lambda bucket: (search_content_type_sorting.index(bucket['key']) if bucket['key'] in search_content_type_sorting else 99999, -bucket['doc_count']))

    # Similarly to the "unknown language" issue above, we have to filter for empty-string extensions, since it gives too much trouble.
    all_aggregations['search_extension'] = []
    for bucket in search_results_raw['aggregations']['search_extension']['buckets']:
        if bucket['key'] == '':
            all_aggregations['search_extension'].append({ 'key': '_empty', 'label': 'unknown', 'doc_count': bucket['doc_count'] })
        else:
            all_aggregations['search_extension'].append({ 'key': bucket['key'], 'label': bucket['key'], 'doc_count': bucket['doc_count'] })

    access_types_buckets = list(search_results_raw['aggregations']['search_access_types']['buckets'])
    access_types_mapping = get_access_types_mapping(display_lang)
    all_aggregations['search_access_types'] = [{ 'key': bucket['key'], 'label': access_types_mapping[bucket['key']], 'doc_count': bucket['doc_count'] } for bucket in access_types_buckets]
    content_type_keys_present = set([bucket['key'] for bucket in access_types_buckets])
    # for key, label in access_types_mapping.items():
    #     if key not in content_type_keys_present:
    #         all_aggregations['search_access_types'].append({ 'key': key, 'label': label, 'doc_count': 0 })
    search_access_types_sorting = list(access_types_mapping.keys())
    all_aggregations['search_access_types'].sort(key=lambda bucket: (search_access_types_sorting.index(bucket['key']) if bucket['key'] in search_access_types_sorting else 99999, -bucket['doc_count']))

    record_sources_buckets = list(search_results_raw['aggregations']['search_record_sources']['buckets'])
    record_sources_mapping = get_record_sources_mapping(display_lang)
    all_aggregations['search_record_sources'] = [{ 'key': bucket['key'], 'label': record_sources_mapping[bucket['key']], 'doc_count': bucket['doc_count'] } for bucket in record_sources_buckets]
    content_type_keys_present = set([bucket['key'] for bucket in record_sources_buckets])
    # for key, label in record_sources_mapping.items():
    #     if key not in content_type_keys_present:
    #         all_aggregations['search_record_sources'].append({ 'key': key, 'label': label, 'doc_count': 0 })

    es_stat = { 'name': 'all_search_aggs//' + search_index_long, 'took': search_results_raw.get('took'), 'timed_out': search_results_raw.get('timed_out') }

    return (all_aggregations, es_stat)


@page.get("/search")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def search_page():
    had_es_timeout = False
    had_primary_es_timeout = False
    es_stats = []

    search_input = request.args.get("q", "").strip()
    filter_values = {
        'search_most_likely_language_code': [val.strip()[0:15] for val in request.args.getlist("lang")],
        'search_content_type': [val.strip()[0:25] for val in request.args.getlist("content")],
        'search_extension': [val.strip()[0:10] for val in request.args.getlist("ext")],
        'search_access_types': [val.strip()[0:50] for val in request.args.getlist("acc")],
        'search_record_sources': [val.strip()[0:20] for val in request.args.getlist("src")],
    }
    sort_value = request.args.get("sort", "").strip()
    search_index_short = request.args.get("index", "").strip()
    if search_index_short not in allthethings.utils.SEARCH_INDEX_SHORT_LONG_MAPPING:
        search_index_short = ""
    search_index_long = allthethings.utils.SEARCH_INDEX_SHORT_LONG_MAPPING[search_index_short]
    if search_index_short == 'digital_lending':
        filter_values['search_extension'] = []

    if bool(re.match(r"^[a-fA-F\d]{32}$", search_input)):
        return redirect(f"/md5/{search_input}", code=302)

    potential_isbn = search_input.replace('-', '')
    if search_input != potential_isbn and (isbnlib.is_isbn13(potential_isbn) or isbnlib.is_isbn10(potential_isbn)):
        return redirect(f"/search?q={potential_isbn}", code=302)

    ol_page = None
    if bool(re.match(r"^OL\d+M$", search_input)):
        ol_page = search_input
    doi_page = None
    potential_doi = normalize_doi(search_input)
    if potential_doi != '':
        doi_page = potential_doi
    isbn_page = None
    canonical_isbn13 = allthethings.utils.normalize_isbn(search_input)
    if canonical_isbn13 != '':
        isbn_page = canonical_isbn13

    post_filter = []
    for key, values in filter_values.items():
        if values != []:
            post_filter.append({ "terms": { f"search_only_fields.{key}": [value if value != '_empty' else '' for value in values] } })

    custom_search_sorting = []
    if sort_value == "newest":
        custom_search_sorting = [{ "search_only_fields.search_year": "desc" }]
    if sort_value == "oldest":
        custom_search_sorting = [{ "search_only_fields.search_year": "asc" }]
    if sort_value == "largest":
        custom_search_sorting = [{ "search_only_fields.search_filesize": "desc" }]
    if sort_value == "smallest":
        custom_search_sorting = [{ "search_only_fields.search_filesize": "asc" }]

    search_query = {
        "bool": {
            "should": [
                {
                    "bool": {
                        "should": [
                            { "rank_feature": { "field": "search_only_fields.search_score_base_rank", "boost": 10000.0 } },
                            { 
                                "constant_score": {
                                    "filter": { "term": { "search_only_fields.search_most_likely_language_code": { "value": allthethings.utils.get_base_lang_code(get_locale()) } } },
                                    "boost": 50000.0,
                                },
                            },
                        ],
                        "must": [
                            { "match_phrase": { "search_only_fields.search_text": { "query": search_input } } },
                        ],
                    },
                },
            ],
            "must": [
                {
                    "bool": {
                        "should": [
                            { "rank_feature": { "field": "search_only_fields.search_score_base_rank", "boost": 10000.0/100000.0 } },
                            {
                                "constant_score": {
                                    "filter": { "term": { "search_only_fields.search_most_likely_language_code": { "value": allthethings.utils.get_base_lang_code(get_locale()) } } },
                                    "boost": 50000.0/100000.0,
                                },
                            },
                        ],
                        "must": [
                            {
                                "simple_query_string": {
                                    "query": search_input, "fields": ["search_only_fields.search_text"],
                                    "default_operator": "and",
                                    "boost": 1/100000.0,
                                },
                            },
                        ],
                    },
                },
            ],
        },
    }

    max_display_results = 200
    max_additional_display_results = 50

    search_results_raw = {}
    try:
        search_results_raw = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[search_index_long].search(
            index=search_index_long, 
            size=max_display_results, 
            query=search_query,
            aggs=search_query_aggs(search_index_long),
            post_filter={ "bool": { "filter": post_filter } },
            sort=custom_search_sorting+['_score'],
            track_total_hits=False,
            timeout=ES_TIMEOUT_PRIMARY,
        )
    except Exception as err:
        had_es_timeout = True
        had_primary_es_timeout = True
    if search_results_raw.get('timed_out'):
        had_es_timeout = True
        had_primary_es_timeout = True
    es_stats.append({ 'name': 'search1_primary', 'took': search_results_raw.get('took'), 'timed_out': search_results_raw.get('timed_out') })

    display_lang = allthethings.utils.get_base_lang_code(get_locale())
    all_aggregations, all_aggregations_es_stat = all_search_aggs(display_lang, search_index_long)
    es_stats.append(all_aggregations_es_stat)
    es_handle = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[search_index_long]

    doc_counts = {}
    doc_counts['search_most_likely_language_code'] = {}
    doc_counts['search_content_type'] = {}
    doc_counts['search_extension'] = {}
    doc_counts['search_access_types'] = {}
    doc_counts['search_record_sources'] = {}
    if search_input == '':
        for bucket in all_aggregations['search_most_likely_language_code']:
            doc_counts['search_most_likely_language_code'][bucket['key']] = bucket['doc_count']
        for bucket in all_aggregations['search_content_type']:
            doc_counts['search_content_type'][bucket['key']] = bucket['doc_count']
        for bucket in all_aggregations['search_extension']:
            doc_counts['search_extension'][bucket['key']] = bucket['doc_count']
        for bucket in all_aggregations['search_access_types']:
            doc_counts['search_access_types'][bucket['key']] = bucket['doc_count']
        for bucket in all_aggregations['search_record_sources']:
            doc_counts['search_record_sources'][bucket['key']] = bucket['doc_count']
    elif 'aggregations' in search_results_raw:
        if 'search_most_likely_language_code' in search_results_raw['aggregations']:
            for bucket in search_results_raw['aggregations']['search_most_likely_language_code']['buckets']:
                doc_counts['search_most_likely_language_code'][bucket['key'] if bucket['key'] != '' else '_empty'] = bucket['doc_count']
        for bucket in search_results_raw['aggregations']['search_content_type']['buckets']:
            doc_counts['search_content_type'][bucket['key']] = bucket['doc_count']
        for bucket in search_results_raw['aggregations']['search_extension']['buckets']:
            doc_counts['search_extension'][bucket['key'] if bucket['key'] != '' else '_empty'] = bucket['doc_count']
        for bucket in search_results_raw['aggregations']['search_access_types']['buckets']:
            doc_counts['search_access_types'][bucket['key']] = bucket['doc_count']
        for bucket in search_results_raw['aggregations']['search_record_sources']['buckets']:
            doc_counts['search_record_sources'][bucket['key']] = bucket['doc_count']

    aggregations = {}
    aggregations['search_most_likely_language_code'] = [{
            **bucket,
            'doc_count': doc_counts['search_most_likely_language_code'].get(bucket['key'], 0),
            'selected':  (bucket['key'] in filter_values['search_most_likely_language_code']),
        } for bucket in all_aggregations['search_most_likely_language_code']]
    aggregations['search_content_type'] = [{
            **bucket,
            'doc_count': doc_counts['search_content_type'].get(bucket['key'], 0),
            'selected':  (bucket['key'] in filter_values['search_content_type']),
        } for bucket in all_aggregations['search_content_type']]
    aggregations['search_extension'] = [{
            **bucket,
            'doc_count': doc_counts['search_extension'].get(bucket['key'], 0),
            'selected':  (bucket['key'] in filter_values['search_extension']),
        } for bucket in all_aggregations['search_extension']]
    aggregations['search_access_types'] = [{
            **bucket,
            'doc_count': doc_counts['search_access_types'].get(bucket['key'], 0),
            'selected':  (bucket['key'] in filter_values['search_access_types']),
        } for bucket in all_aggregations['search_access_types']]
    aggregations['search_record_sources'] = [{
            **bucket,
            'doc_count': doc_counts['search_record_sources'].get(bucket['key'], 0),
            'selected':  (bucket['key'] in filter_values['search_record_sources']),
        } for bucket in all_aggregations['search_record_sources']]

    # Only sort languages, for the other lists we want consistency.
    aggregations['search_most_likely_language_code'] = sorted(aggregations['search_most_likely_language_code'], key=lambda bucket: bucket['doc_count'] + (1000000000 if bucket['key'] == display_lang else 0), reverse=True)

    search_aarecords = []
    if 'hits' in search_results_raw:
        search_aarecords = [add_additional_to_aarecord(aarecord_raw['_source']) for aarecord_raw in search_results_raw['hits']['hits'] if aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]

    max_search_aarecords_reached = False
    max_additional_search_aarecords_reached = False
    additional_search_aarecords = []

    if (len(search_aarecords) < max_display_results) and (not had_es_timeout):
        # For partial matches, first try our original query again but this time without filters.
        seen_ids = set([aarecord['id'] for aarecord in search_aarecords])
        search_results_raw = {}
        try:
            search_results_raw = es_handle.search(
                index=search_index_long, 
                size=len(seen_ids)+max_additional_display_results, # This way, we'll never filter out more than "max_display_results" results because we have seen them already., 
                query=search_query,
                sort=custom_search_sorting+['_score'],
                track_total_hits=False,
                timeout=ES_TIMEOUT,
            )
        except Exception as err:
            had_es_timeout = True
        if search_results_raw.get('timed_out'):
            had_es_timeout = True
        es_stats.append({ 'name': 'search2', 'took': search_results_raw.get('took'), 'timed_out': search_results_raw.get('timed_out') })
        if len(seen_ids)+len(search_results_raw['hits']['hits']) >= max_additional_display_results:
            max_additional_search_aarecords_reached = True
        additional_search_aarecords = [add_additional_to_aarecord(aarecord_raw['_source']) for aarecord_raw in search_results_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]

        # Then do an "OR" query, but this time with the filters again.
        if (len(search_aarecords) + len(additional_search_aarecords) < max_display_results) and (not had_es_timeout):
            seen_ids = seen_ids.union(set([aarecord['id'] for aarecord in additional_search_aarecords]))
            search_results_raw = {}
            try:
                search_results_raw = es_handle.search(
                    index=search_index_long,
                    size=len(seen_ids)+max_additional_display_results, # This way, we'll never filter out more than "max_display_results" results because we have seen them already.
                    # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                    query={"bool": { "must": { "match": { "search_only_fields.search_text": { "query": search_input } } }, "filter": post_filter } },
                    sort=custom_search_sorting+['_score'],
                    track_total_hits=False,
                    timeout=ES_TIMEOUT,
                )
            except Exception as err:
                had_es_timeout = True
            if search_results_raw.get('timed_out'):
                had_es_timeout = True
            es_stats.append({ 'name': 'search3', 'took': search_results_raw.get('took'), 'timed_out': search_results_raw.get('timed_out') })
            if len(seen_ids)+len(search_results_raw['hits']['hits']) >= max_additional_display_results:
                max_additional_search_aarecords_reached = True
            additional_search_aarecords += [add_additional_to_aarecord(aarecord_raw['_source']) for aarecord_raw in search_results_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]

            # If we still don't have enough, do another OR query but this time without filters.
            if (len(search_aarecords) + len(additional_search_aarecords) < max_display_results) and not had_es_timeout:
                seen_ids = seen_ids.union(set([aarecord['id'] for aarecord in additional_search_aarecords]))
                search_results_raw = {}
                try:
                    search_results_raw = es_handle.search(
                        index=search_index_long,
                        size=len(seen_ids)+max_additional_display_results, # This way, we'll never filter out more than "max_display_results" results because we have seen them already.
                        # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                        query={"bool": { "must": { "match": { "search_only_fields.search_text": { "query": search_input } } } } },
                        sort=custom_search_sorting+['_score'],
                        track_total_hits=False,
                        timeout=ES_TIMEOUT,
                    )
                except Exception as err:
                    had_es_timeout = True
                if search_results_raw.get('timed_out'):
                    had_es_timeout = True
                es_stats.append({ 'name': 'search4', 'took': search_results_raw.get('took'), 'timed_out': search_results_raw.get('timed_out') })
                if (len(seen_ids)+len(search_results_raw['hits']['hits']) >= max_additional_display_results) and (not had_es_timeout):
                    max_additional_search_aarecords_reached = True
                additional_search_aarecords += [add_additional_to_aarecord(aarecord_raw['_source']) for aarecord_raw in search_results_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]
    else:
        max_search_aarecords_reached = True

    # had_fatal_es_timeout = had_es_timeout and len(search_aarecords) == 0
    
    search_dict = {}
    search_dict['search_aarecords'] = search_aarecords[0:max_display_results]
    search_dict['additional_search_aarecords'] = additional_search_aarecords[0:max_additional_display_results]
    search_dict['max_search_aarecords_reached'] = max_search_aarecords_reached
    search_dict['max_additional_search_aarecords_reached'] = max_additional_search_aarecords_reached
    search_dict['aggregations'] = aggregations
    search_dict['sort_value'] = sort_value
    search_dict['search_index_short'] = search_index_short
    search_dict['es_stats'] = es_stats
    search_dict['had_primary_es_timeout'] = had_primary_es_timeout
    # search_dict['had_fatal_es_timeout'] = had_fatal_es_timeout

    # status = 404 if had_fatal_es_timeout else 200 # So we don't cache
    status = 200

    r = make_response((render_template(
            "page/search.html",
            header_active="home/search",
            search_input=search_input,
            search_dict=search_dict,
            redirect_pages={
                'ol_page': ol_page,
                'doi_page': doi_page,
                'isbn_page': isbn_page,
            }
        ), status))
    if had_primary_es_timeout:
        r.headers.add('Cache-Control', 'no-cache')
    return r
