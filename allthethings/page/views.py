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
from allthethings.extensions import engine, es, babel, mariapersist_engine, ZlibBook, ZlibIsbn, IsbndbIsbns, LibgenliEditions, LibgenliEditionsAddDescr, LibgenliEditionsToFiles, LibgenliElemDescr, LibgenliFiles, LibgenliFilesAddDescr, LibgenliPublishers, LibgenliSeries, LibgenliSeriesAddDescr, LibgenrsDescription, LibgenrsFiction, LibgenrsFictionDescription, LibgenrsFictionHashes, LibgenrsHashes, LibgenrsTopics, LibgenrsUpdated, OlBase, AaLgliComics202208Files, AaIa202306Metadata, AaIa202306Files, MariapersistSmallFiles
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

ES_TIMEOUT = "5s"

# Retrieved from https://openlibrary.org/config/edition.json on 2023-07-02
ol_edition_json = json.load(open(os.path.dirname(os.path.realpath(__file__)) + '/ol_edition.json'))
ol_classifications = {}
for classification in ol_edition_json['classifications']:
    if 'website' in classification:
        classification['website'] = classification['website'].split(' ')[0] # sometimes there's a suffix in text..
    ol_classifications[classification['name']] = classification
ol_classifications['lc_classifications']['website'] = 'https://en.wikipedia.org/wiki/Library_of_Congress_Classification'
ol_classifications['dewey_decimal_class']['website'] = 'https://en.wikipedia.org/wiki/List_of_Dewey_Decimal_classes'
ol_identifiers = {}
for identifier in ol_edition_json['identifiers']:
    ol_identifiers[identifier['name']] = identifier

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
# * http://localhost:8000/isbn/9789514596933
# * http://localhost:8000/isbn/9780000000439
# * http://localhost:8000/isbn/9780001055506
# * http://localhost:8000/isbn/9780316769174
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

def make_temp_anon_aac_zlib3_path(file_aac_id, data_folder):
    date = data_folder.split('__')[3][0:8]
    return f"o/zlib3_files/{date}/{data_folder}/{file_aac_id}"

def strip_description(description):
    return re.sub(r'<[^<]+?>', r' ', re.sub(r'<a.+?href="([^"]+)"[^>]*>', r'(\1) ', description.replace('</p>', '\n\n').replace('</P>', '\n\n').replace('<br>', '\n').replace('<BR>', '\n')))

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
                lang = ''
        # We have a bunch of weird data that gets interpreted as "Egyptian Sign Language" when it's
        # clearly all just Spanish..
        if lang == "esl":
            lang = "es"
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def home_page():
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
        aarecords = get_aarecords_elasticsearch(session, popular_ids)
        aarecords.sort(key=lambda aarecord: popular_ids.index(aarecord['id']))

        return render_template(
            "page/home.html",
            header_active="home",
            aarecords=aarecords,
        )

@page.get("/login")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def login_page():
    return redirect(f"/account", code=301)
    # return render_template("page/login.html", header_active="account")

@page.get("/about")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def about_page():
    return render_template("page/about.html", header_active="home/about")

@page.get("/security")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def security_page():
    return render_template("page/security.html", header_active="home/security")

@page.get("/mobile")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def mobile_page():
    return render_template("page/mobile.html", header_active="home/mobile")

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

        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT metadata FROM annas_archive_meta__aacid__zlib3_records ORDER BY aacid DESC LIMIT 1')
        zlib3_record = cursor.fetchone()
        zlib_date = orjson.loads(zlib3_record['metadata'])['date_modified'] if zlib3_record is not None else ''

        stats_data_es = dict(es.msearch(
            request_timeout=20,
            max_concurrent_searches=10,
            max_concurrent_shard_requests=10,
            searches=[
                # { "index": "aarecords", "request_cache": False },
                { "index": "aarecords" },
                { "track_total_hits": True, "size": 0, "aggs": { "total_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } } },
                # { "index": "aarecords", "request_cache": False },
                { "index": "aarecords" },
                {
                    "track_total_hits": True,
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
                    "size": 0,
                    "query": { "term": { "search_only_fields.search_content_type": { "value": "journal_article" } } },
                    "aggs": { "search_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } },
                },
                # { "index": "aarecords", "request_cache": False },
                { "index": "aarecords" },
                {
                    "track_total_hits": True,
                    "size": 0,
                    "query": { "term": { "search_only_fields.search_content_type": { "value": "journal_article" } } },
                    "aggs": { "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } } },
                },
                # { "index": "aarecords", "request_cache": False },
                { "index": "aarecords" },
                {
                    "track_total_hits": True,
                    "size": 0,
                    "aggs": { "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } } },
                },
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

    return {
        'stats_by_group': stats_by_group,
        'libgenrs_date': libgenrs_date,
        'libgenli_date': libgenli_date,
        'openlib_date': openlib_date,
        'zlib_date': zlib_date,
        'ia_date': '2023-06-28',
        'isbndb_date': '2022-09-01',
        'isbn_country_date': '2022-02-11',
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

@page.get("/datasets/isbn_ranges")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def datasets_isbn_ranges_page():
    return render_template("page/datasets_isbn_ranges.html", header_active="home/datasets", stats_data=get_stats_data())

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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def torrents_page():
    with mariapersist_engine.connect() as conn:
        small_files = conn.execute(select(MariapersistSmallFiles.created, MariapersistSmallFiles.file_path, MariapersistSmallFiles.metadata).where(MariapersistSmallFiles.file_path.like("torrents/managed_by_aa/%")).order_by(MariapersistSmallFiles.created.asc()).limit(10000)).all()

        small_file_dicts_grouped = collections.defaultdict(list)
        for small_file in small_files:
            # if orjson.loads(small_file.metadata).get('by_script') == 1:
            #     continue
            group = small_file.file_path.split('/')[2]
            filename = small_file.file_path.split('/')[3]
            if 'zlib3' in filename:
                group = 'zlib'
            small_file_dicts_grouped[group].append(dict(small_file))

        return render_template(
            "page/torrents.html",
            header_active="home/torrents",
            small_file_dicts_grouped=small_file_dicts_grouped,
        )

@page.get("/torrents.json")
@allthethings.utils.no_cache()
def torrents_json_page():
    with mariapersist_engine.connect() as conn:
        small_files = conn.execute(select(MariapersistSmallFiles.created, MariapersistSmallFiles.file_path, MariapersistSmallFiles.metadata).where(MariapersistSmallFiles.file_path.like("torrents/managed_by_aa/%")).order_by(MariapersistSmallFiles.created.asc()).limit(10000)).all()
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
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT data FROM mariapersist_small_files WHERE file_path LIKE CONCAT("torrents/managed_by_aa/annas_archive_meta__aacid/annas_archive_meta__aacid__", %(collection)s, "%%") ORDER BY created DESC LIMIT 1', { "collection": collection })
        file = cursor.fetchone()
        if file is None:
            return "File not found", 404
        return send_file(io.BytesIO(file['data']), as_attachment=True, download_name=f'{collection}.torrent')

@page.get("/small_file/<path:file_path>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def small_file_page(file_path):
    with mariapersist_engine.connect() as conn:
        file = conn.execute(select(MariapersistSmallFiles.data).where(MariapersistSmallFiles.file_path == file_path).limit(10000)).first()
        if file is None:
            return "File not found", 404
        return send_file(io.BytesIO(file.data), as_attachment=True, download_name=file_path.split('/')[-1])


zlib_book_dict_comments = {
    **allthethings.utils.COMMON_DICT_COMMENTS,
    "zlibrary_id": ("before", ["This is a file from the Z-Library collection of Anna's Archive.",
                      "More details at https://annas-archive.org/datasets/zlib",
                      "The source URL is http://zlibrary24tuxziyiyfr7zd46ytefdqbqd2axkmxm4o5374ptpc52fad.onion/md5/<md5_reported>",
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
        allthethings.utils.add_isbns_unified(zlib_book_dict, [record.isbn for record in zlib_book.isbns])

        zlib_book_dicts.append(add_comments_to_dict(zlib_book_dict, zlib_book_dict_comments))
    return zlib_book_dicts

def get_aac_zlib3_book_dicts(session, key, values):
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
    seen_ia_ids = set()
    ia_entries = []
    try:
        base_query = select(AaIa202306Metadata, AaIa202306Files).join(AaIa202306Files, AaIa202306Files.ia_id == AaIa202306Metadata.ia_id, isouter=True)
        if key.lower() in ['md5']:
            # TODO: we should also consider matching on libgen_md5, but we used to do that before and it had bad SQL performance,
            # when combined in a single query, so we'd have to split it up.
            ia_entries = session.execute(
                base_query.where(getattr(AaIa202306Files, 'md5').in_(values))
            ).unique().all()
        else:
            ia_entries = session.execute(
                base_query.where(getattr(AaIa202306Metadata, key).in_(values))
            ).unique().all()
    except Exception as err:
        print(f"Error in get_ia_record_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    ia_record_dicts = []
    for ia_record, ia_file in ia_entries:
        ia_record_dict = ia_record.to_dict()

        # TODO: When querying by ia_id we can match multiple files. For now we just pick the first one.
        if ia_record_dict['ia_id'] in seen_ia_ids:
            continue
        seen_ia_ids.add(ia_record_dict['ia_id'])

        ia_record_dict['aa_ia_file'] = None
        if ia_file and ia_record_dict['libgen_md5'] is None: # If there's a Libgen MD5, then we do NOT serve our IA file.
            ia_record_dict['aa_ia_file'] = ia_file.to_dict()
            ia_record_dict['aa_ia_file']['extension'] = 'pdf'
        ia_record_dict['json'] = orjson.loads(ia_record_dict['json'])

        ia_record_dict['aa_ia_derived'] = {}
        ia_record_dict['aa_ia_derived']['original_filename'] = ia_record_dict['ia_id'] + '.pdf'
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
        for date in ia_record_dict['aa_ia_derived']['all_dates']:
            potential_year = re.search(r"(\d\d\d\d)", date)
            if potential_year is not None:
                ia_record_dict['aa_ia_derived']['year'] = potential_year[0]

        ia_record_dict['aa_ia_derived']['content_type'] = 'book_unknown'
        if ia_record_dict['ia_id'].split('_')[0] in ['sim', 'per'] or extract_list_from_ia_json_field(ia_record_dict, 'pub_type') in ["Government Documents", "Historical Journals", "Law Journals", "Magazine", "Magazines", "Newspaper", "Scholarly Journals", "Trade Journals"]:
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
            allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'openlibrary', item)
        for item in extract_list_from_ia_json_field(ia_record_dict, 'item'):
            allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'lccn', item)

        isbns = extract_list_from_ia_json_field(ia_record_dict, 'isbn')
        for urn in extract_list_from_ia_json_field(ia_record_dict, 'external-identifier'):
            if urn.startswith('urn:oclc:record:'):
                allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'oclcworldcat', urn[len('urn:oclc:record:'):])
            elif urn.startswith('urn:oclc:'):
                allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'oclcworldcat', urn[len('urn:oclc:'):])
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


@page.get("/ol/<string:ol_book_id>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def ol_book_page(ol_book_id):
    ol_book_id = ol_book_id[0:20]

    with engine.connect() as conn:
        ol_book = conn.execute(select(OlBase).where(OlBase.ol_key == f"/books/{ol_book_id}").limit(1)).first()

        if ol_book is None:
            return render_template("page/ol_book.html", header_active="search", ol_book_id=ol_book_id), 404

        ol_book_dict = dict(ol_book)
        ol_book_dict['json'] = orjson.loads(ol_book_dict['json'])

        ol_book_dict['work'] = None
        if 'works' in ol_book_dict['json'] and len(ol_book_dict['json']['works']) > 0:
            ol_work = conn.execute(select(OlBase).where(OlBase.ol_key == ol_book_dict['json']['works'][0]['key']).limit(1)).first()
            if ol_work:
                ol_book_dict['work'] = dict(ol_work)
                ol_book_dict['work']['json'] = orjson.loads(ol_book_dict['work']['json'])

        unredirected_ol_authors = []
        if 'authors' in ol_book_dict['json'] and len(ol_book_dict['json']['authors']) > 0:
            unredirected_ol_authors = conn.execute(select(OlBase).where(OlBase.ol_key.in_([author['key'] for author in ol_book_dict['json']['authors']])).limit(10)).all()
        elif ol_book_dict['work'] and 'authors' in ol_book_dict['work']['json']:
            author_keys = [author['author']['key'] for author in ol_book_dict['work']['json']['authors'] if 'author' in author]
            if len(author_keys) > 0:
                unredirected_ol_authors = conn.execute(select(OlBase).where(OlBase.ol_key.in_(author_keys)).limit(10)).all()
        ol_authors = []
        # TODO: Batch them up.
        for unredirected_ol_author in unredirected_ol_authors:
            if unredirected_ol_author.type == '/type/redirect':
                json = orjson.loads(unredirected_ol_author.json)
                if 'location' not in json:
                    continue
                ol_author = conn.execute(select(OlBase).where(OlBase.ol_key == json['location']).limit(1)).first()
                ol_authors.append(ol_author)
            else:
                ol_authors.append(unredirected_ol_author)

        ol_book_dict['authors'] = []
        for author in ol_authors:
            author_dict = dict(author)
            author_dict['json'] = orjson.loads(author_dict['json'])
            ol_book_dict['authors'].append(author_dict)

        allthethings.utils.init_identifiers_and_classification_unified(ol_book_dict)
        allthethings.utils.add_isbns_unified(ol_book_dict, (ol_book_dict['json'].get('isbn_10') or []) + (ol_book_dict['json'].get('isbn_13') or []))
        for item in (ol_book_dict['json'].get('lc_classifications') or []):
            allthethings.utils.add_classification_unified(ol_book_dict, allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['lc_classifications'], item)
        for item in (ol_book_dict['json'].get('dewey_decimal_class') or []):
            allthethings.utils.add_classification_unified(ol_book_dict, allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_decimal_class'], item)
        for item in (ol_book_dict['json'].get('dewey_number') or []):
            allthethings.utils.add_classification_unified(ol_book_dict, allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_number'], item)
        for classification_type, items in (ol_book_dict['json'].get('classifications') or {}).items():
            if classification_type not in allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING:
                # TODO: Do a scrape / review of all classification types in OL.
                print(f"Warning: missing classification_type: {classification_type}")
                continue
            for item in items:
                allthethings.utils.add_classification_unified(ol_book_dict, allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[classification_type], item)
        if ol_book_dict['work']:
            allthethings.utils.init_identifiers_and_classification_unified(ol_book_dict['work'])
            for item in (ol_book_dict['work']['json'].get('lc_classifications') or []):
                allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['lc_classifications'], item)
            for item in (ol_book_dict['work']['json'].get('dewey_decimal_class') or []):
                allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_decimal_class'], item)
            for item in (ol_book_dict['work']['json'].get('dewey_number') or []):
                allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING['dewey_number'], item)
            for classification_type, items in (ol_book_dict['work']['json'].get('classifications') or {}).items():
                if classification_type not in allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING:
                    # TODO: Do a scrape / review of all classification types in OL.
                    print(f"Warning: missing classification_type: {classification_type}")
                    continue
                for item in items:
                    allthethings.utils.add_classification_unified(ol_book_dict['work'], allthethings.utils.OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[classification_type], item)
        for item in (ol_book_dict['json'].get('lccn') or []):
            allthethings.utils.add_identifier_unified(ol_book_dict, allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING['lccn'], item)
        for item in (ol_book_dict['json'].get('oclc_numbers') or []):
            allthethings.utils.add_identifier_unified(ol_book_dict, allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING['oclc_numbers'], item)
        for identifier_type, items in (ol_book_dict['json'].get('identifiers') or {}).items():
            if identifier_type not in allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING:
                # TODO: Do a scrape / review of all identifier types in OL.
                print(f"Warning: missing identifier_type: {identifier_type}")
                continue
            for item in items:
                allthethings.utils.add_identifier_unified(ol_book_dict, allthethings.utils.OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING[identifier_type], item)

        ol_book_dict['languages_normalized'] = [(ol_languages.get(language['key']) or {'name':language['key']})['name'] for language in (ol_book_dict['json'].get('languages') or [])]
        ol_book_dict['translated_from_normalized'] = [(ol_languages.get(language['key']) or {'name':language['key']})['name'] for language in (ol_book_dict['json'].get('translated_from') or [])]

        ol_book_top = {
            'title': '',
            'subtitle': '',
            'authors': '',
            'description': '',
            'cover': f"https://covers.openlibrary.org/b/olid/{ol_book_id}-M.jpg",
        }

        if len(ol_book_top['title'].strip()) == 0 and 'title' in ol_book_dict['json']:
            if 'title_prefix' in ol_book_dict['json']:
                ol_book_top['title'] = ol_book_dict['json']['title_prefix'] + " " + ol_book_dict['json']['title']
            else:
                ol_book_top['title'] = ol_book_dict['json']['title']
        if len(ol_book_top['title'].strip()) == 0 and ol_book_dict['work'] and 'title' in ol_book_dict['work']['json']:
            ol_book_top['title'] = ol_book_dict['work']['json']['title']
        if len(ol_book_top['title'].strip()) == 0:
            ol_book_top['title'] = '(no title)'

        if len(ol_book_top['subtitle'].strip()) == 0 and 'subtitle' in ol_book_dict['json']:
            ol_book_top['subtitle'] = ol_book_dict['json']['subtitle']
        if len(ol_book_top['subtitle'].strip()) == 0 and ol_book_dict['work'] and 'subtitle' in ol_book_dict['work']['json']:
            ol_book_top['subtitle'] = ol_book_dict['work']['json']['subtitle']

        if len(ol_book_top['authors'].strip()) == 0 and 'by_statement' in ol_book_dict['json']:
            ol_book_top['authors'] = ol_book_dict['json']['by_statement'].replace(' ; ', '; ').strip()
            if ol_book_top['authors'][-1] == '.':
                ol_book_top['authors'] = ol_book_top['authors'][0:-1]
        if len(ol_book_top['authors'].strip()) == 0:
            ol_book_top['authors'] = ",".join([author['json']['name'] for author in ol_book_dict['authors'] if 'name' in author['json']])
        if len(ol_book_top['authors'].strip()) == 0:
            ol_book_top['authors'] = '(no authors)'

        if len(ol_book_top['description'].strip()) == 0 and 'description' in ol_book_dict['json']:
            if type(ol_book_dict['json']['description']) == str:
                ol_book_top['description'] = ol_book_dict['json']['description']
            else:
                ol_book_top['description'] = ol_book_dict['json']['description']['value']
        if len(ol_book_top['description'].strip()) == 0 and ol_book_dict['work'] and 'description' in ol_book_dict['work']['json']:
            if type(ol_book_dict['work']['json']['description']) == str:
                ol_book_top['description'] = ol_book_dict['work']['json']['description']
            else:
                ol_book_top['description'] = ol_book_dict['work']['json']['description']['value']
        if len(ol_book_top['description'].strip()) == 0 and 'first_sentence' in ol_book_dict['json']:
            if type(ol_book_dict['json']['first_sentence']) == str:
                ol_book_top['description'] = ol_book_dict['json']['first_sentence']
            else:
                ol_book_top['description'] = ol_book_dict['json']['first_sentence']['value']
        if len(ol_book_top['description'].strip()) == 0 and ol_book_dict['work'] and 'first_sentence' in ol_book_dict['work']['json']:
            if type(ol_book_dict['work']['json']['first_sentence']) == str:
                ol_book_top['description'] = ol_book_dict['work']['json']['first_sentence']
            else:
                ol_book_top['description'] = ol_book_dict['work']['json']['first_sentence']['value']

        if len(ol_book_dict['json'].get('covers') or []) > 0:
            ol_book_top['cover'] = f"https://covers.openlibrary.org/b/id/{ol_book_dict['json']['covers'][0]}-M.jpg"
        elif ol_book_dict['work'] and len(ol_book_dict['work']['json'].get('covers') or []) > 0:
            ol_book_top['cover'] = f"https://covers.openlibrary.org/b/id/{ol_book_dict['work']['json']['covers'][0]}-M.jpg"

        return render_template(
            "page/ol_book.html",
            header_active="search",
            ol_book_id=ol_book_id,
            ol_book_dict=ol_book_dict,
            ol_book_dict_json=nice_json(ol_book_dict),
            ol_book_top=ol_book_top,
            ol_classifications=ol_classifications,
            ol_identifiers=ol_identifiers,
            ol_languages=ol_languages,
        )

def get_aa_lgli_comics_2022_08_file_dicts(session, key, values):
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
                        allthethings.utils.add_identifier_unified(edition_dict, key, value)
            for key, values in edition_dict['descriptions_mapped'].items():
                if key in allthethings.utils.LGLI_CLASSIFICATIONS:
                    for value in values:
                        allthethings.utils.add_classification_unified(edition_dict, key, value)
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
        potential_doi_scimag_archive_path = lgli_file_dict['scimag_archive_path'].replace('\\', '/')
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

def get_isbn_dicts(session, canonical_isbn13s):
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
            "doi": isbnlib.doi(canonical_isbn13),
            "info": isbnlib.info(canonical_isbn13),
            "mask": isbn13_mask,
            "mask_split": isbn13_mask.split('-'),
        }
        if isbn_dict['isbn10']:
            isbn_dict['mask10'] = isbnlib.mask(isbn_dict['isbn10'])

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

        for isbndb_dict in isbn_dict['isbndb']:
            isbndb_dict['language_codes'] = get_bcp47_lang_codes(isbndb_dict['json'].get('language') or '')
            isbndb_dict['languages_and_codes'] = [(get_display_name_for_lang(lang_code, allthethings.utils.get_full_lang_code(get_locale())), lang_code) for lang_code in isbndb_dict['language_codes']]
            isbndb_dict['edition_varia_normalized'] = ", ".join([item for item in [
                str(isbndb_dict['json'].get('edition') or '').strip(),
                str(isbndb_dict['json'].get('date_published') or '').split('T')[0].strip(),
            ] if item != ''])
            isbndb_dict['title_normalized'] = max([isbndb_dict['json'].get('title') or '', isbndb_dict['json'].get('title_long') or ''], key=len)
            isbndb_dict['year_normalized'] = ''
            potential_year = re.search(r"(\d\d\d\d)", str(isbndb_dict['json'].get('date_published') or '').split('T')[0])
            if potential_year is not None:
                isbndb_dict['year_normalized'] = potential_year[0]

        isbn_dicts.append(isbn_dict)

    return isbn_dicts


@page.get("/isbn/<string:isbn_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def isbn_page(isbn_input):
    isbn_input = isbn_input[0:20]

    canonical_isbn13 = allthethings.utils.normalize_isbn(isbn_input)
    if canonical_isbn13 == '':
        # TODO, check if a different prefix would help, like in
        # https://github.com/inventaire/isbn3/blob/d792973ac0e13a48466d199b39326c96026b7fc3/lib/audit.js
        return render_template("page/isbn.html", header_active="search", isbn_input=isbn_input)

    if canonical_isbn13 != isbn_input:
        return redirect(f"/isbn/{canonical_isbn13}", code=301)

    with Session(engine) as session:
        isbn13_mask = isbnlib.mask(canonical_isbn13)
        isbn_dict = get_isbn_dicts(session, [canonical_isbn13])[0]
        isbn_dict['additional'] = {}

        barcode_svg = ''
        try:
            barcode_bytesio = io.BytesIO()
            barcode.ISBN13(canonical_isbn13, writer=barcode.writer.SVGWriter()).write(barcode_bytesio)
            barcode_bytesio.seek(0)
            isbn_dict['additional']['barcode_svg'] = barcode_bytesio.read().decode('utf-8').replace('fill:white', 'fill:transparent').replace(canonical_isbn13, '')
        except Exception as err:
            print(f"Error generating barcode: {err}")
    
        if len(isbn_dict['isbndb']) > 0:
            isbn_dict['additional']['top_box'] = {
                'cover_url': isbn_dict['isbndb'][0]['json'].get('image') or '',
                'top_row': isbn_dict['isbndb'][0]['languages_and_codes'][0][0] if len(isbn_dict['isbndb'][0]['languages_and_codes']) > 0 else '',
                'title': isbn_dict['isbndb'][0]['title_normalized'],
                'publisher_and_edition': ", ".join([item for item in [
                    str(isbn_dict['isbndb'][0]['json'].get('publisher') or '').strip(),
                    str(isbn_dict['isbndb'][0]['json'].get('edition_varia_normalized') or '').strip(),
                ] if item != '']),
                'author': ', '.join(isbn_dict['isbndb'][0]['json'].get('authors') or []),
                'description': '\n\n'.join([strip_description(isbn_dict['isbndb'][0]['json'].get('synopsis') or ''), strip_description(isbn_dict['isbndb'][0]['json'].get('overview') or '')]).strip(),
            }

        # TODO: sort the results again by best matching language. But we should maybe also look at other matches like title, author, etc, in case we have mislabeled ISBNs.
        # Get the language codes from the first match.
        # language_codes_probs = {}
        # if len(isbn_dict['isbndb']) > 0:
        #     for lang_code in isbn_dict['isbndb'][0]['language_codes']:
        #         language_codes_probs[lang_code] = 1.0

        search_results_raw = es.search(
            index="aarecords",
            size=100,
            query={ "term": { "search_only_fields.search_isbn13": canonical_isbn13 } },
            sort={ "search_only_fields.search_score_base": "desc" },
            timeout=ES_TIMEOUT,
        )
        search_aarecords = [add_additional_to_aarecord(aarecord['_source']) for aarecord in search_results_raw['hits']['hits']]
        isbn_dict['additional']['search_aarecords'] = search_aarecords
        
        return render_template(
            "page/isbn.html",
            header_active="search",
            isbn_input=isbn_input,
            isbn_dict=isbn_dict,
            isbn_dict_json=nice_json(isbn_dict),
        )

@page.get("/doi/<path:doi_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def doi_page(doi_input):
    doi_input = normalize_doi(doi_input[0:100])

    if doi_input == '':
        return render_template("page/doi.html", header_active="search", doi_input=doi_input), 404

    search_results_raw = es.search(
        index="aarecords",
        size=100,
        query={ "term": { "search_only_fields.search_doi": doi_input } },
        sort={ "search_only_fields.search_score_base": "desc" },
        timeout=ES_TIMEOUT,
    )
    search_aarecords = [add_additional_to_aarecord(aarecord['_source']) for aarecord in search_results_raw['hits']['hits']]

    doi_dict = {}
    doi_dict['search_aarecords'] = search_aarecords
    
    return render_template(
        "page/doi.html",
        header_active="search",
        doi_input=doi_input,
        doi_dict=doi_dict,
        doi_dict_json=nice_json(doi_dict),
    )

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

def get_aarecords_elasticsearch(session, aarecord_ids):
    if not allthethings.utils.validate_aarecord_ids(aarecord_ids):
        raise Exception("Invalid aarecord_ids")

    # Filter out bad data
    aarecord_ids = [val for val in aarecord_ids if val not in search_filtered_bad_aarecord_ids]

    if len(aarecord_ids) == 0:
        return []

    # Uncomment the following line to use MySQL directly; useful for local development.
    # return [add_additional_to_aarecord(aarecord) for aarecord in get_aarecords_mysql(session, aarecord_ids)]

    search_results_raw = es.mget(index="aarecords", ids=aarecord_ids)
    return [add_additional_to_aarecord(aarecord_raw['_source']) for aarecord_raw in search_results_raw['docs'] if aarecord_raw['found'] and (aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids)]


def get_random_aarecord_elasticsearch():
    """
    Returns a random aarecord from Elasticsearch.
    Uses `random_score`. See: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-function-score-query.html#function-random
    """
    search_results_raw = es.search(
        index="aarecords",
        size=1,
        query={
            "function_score": {
                "query": {
                    "bool": {
                        "must": {
                            "match_all": {}
                        },
                        "must_not": [
                            {
                                "ids": { "values": search_filtered_bad_aarecord_ids }
                            }
                        ]
                    }
                },
                "random_score": {},
            },
        },
        timeout=ES_TIMEOUT,
    )

    first_hit = search_results_raw['hits']['hits'][0]
    return first_hit


def aarecord_score_base(aarecord):
    if len(aarecord['file_unified_data'].get('problems') or []) > 0:
        return 0.01

    score = 10000.0
    # Filesize of >0.5MB is overriding everything else.
    if (aarecord['file_unified_data'].get('filesize_best') or 0) > 500000:
        score += 1000.0
    # If we're not confident about the language, demote.
    if len(aarecord['file_unified_data'].get('language_codes') or []) == 0:
        score -= 2.0
    # Bump English a little bit regardless of the user's language
    if (aarecord['search_only_fields']['search_most_likely_language_code'] == 'en'):
        score += 5.0
    if (aarecord['file_unified_data'].get('extension_best') or '') in ['epub', 'pdf']:
        score += 10.0
    if len(aarecord['file_unified_data'].get('cover_url_best') or '') > 0:
        score += 3.0
    if (aarecord['file_unified_data'].get('has_aa_downloads') or 0) > 0:
        score += 5.0
    # Don't bump IA too much.
    if ((aarecord['file_unified_data'].get('has_aa_exclusive_downloads') or 0) > 0) and (aarecord['search_only_fields']['search_record_sources'] != ['ia']):
        score += 3.0
    if len(aarecord['file_unified_data'].get('title_best') or '') > 0:
        score += 10.0
    if len(aarecord['file_unified_data'].get('author_best') or '') > 0:
        score += 1.0
    if len(aarecord['file_unified_data'].get('publisher_best') or '') > 0:
        score += 1.0
    if len(aarecord['file_unified_data'].get('edition_varia_best') or '') > 0:
        score += 1.0
    score += min(5.0, 1.0*len(aarecord['file_unified_data'].get('identifiers_unified') or []))
    if len(aarecord['file_unified_data'].get('content_type') or '') in ['journal_article', 'standards_document', 'book_comic', 'magazine']:
        # For now demote non-books quite a bit, since they can drown out books.
        # People can filter for them directly.
        score -= 70.0
    if len(aarecord['file_unified_data'].get('stripped_description_best') or '') > 0:
        score += 1.0
    return score

def get_aarecords_mysql(session, aarecord_ids):
    if not allthethings.utils.validate_aarecord_ids(aarecord_ids):
        raise Exception("Invalid aarecord_ids")

    # Filter out bad data
    aarecord_ids = [val for val in aarecord_ids if val not in search_filtered_bad_aarecord_ids]

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

    # First pass, so we can fetch more dependencies.
    aarecords = []
    canonical_isbn13s = []
    for aarecord_id in aarecord_ids:
        aarecord = {}
        aarecord['id'] = aarecord_id
        aarecord['path'] = '/' + aarecord_id.replace(':', '/')
        aarecord['lgrsnf_book'] = lgrsnf_book_dicts.get(aarecord_id)
        aarecord['lgrsfic_book'] = lgrsfic_book_dicts.get(aarecord_id)
        aarecord['lgli_file'] = lgli_file_dicts.get(aarecord_id)
        if aarecord.get('lgli_file'):
            aarecord['lgli_file']['editions'] = aarecord['lgli_file']['editions'][0:5]
        aarecord['zlib_book'] = zlib_book_dicts1.get(aarecord_id) or zlib_book_dicts2.get(aarecord_id)
        aarecord['aac_zlib3_book'] = aac_zlib3_book_dicts1.get(aarecord_id) or aac_zlib3_book_dicts2.get(aarecord_id)
        aarecord['aa_lgli_comics_2022_08_file'] = aa_lgli_comics_2022_08_file_dicts.get(aarecord_id)
        aarecord['ia_record'] = ia_record_dicts.get(aarecord_id)

        lgli_all_editions = aarecord['lgli_file']['editions'] if aarecord.get('lgli_file') else []

        aarecord['file_unified_data'] = {}
        aarecord['file_unified_data']['identifiers_unified'] = allthethings.utils.merge_unified_fields([
            ((aarecord['lgrsnf_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['aac_zlib3_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['zlib_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgli_file'] or {}).get('identifiers_unified') or {}),
            *[(edition['identifiers_unified'].get('identifiers_unified') or {}) for edition in lgli_all_editions],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('identifiers_unified') or {}),
        ])
        for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
            canonical_isbn13s.append(canonical_isbn13)

        aarecords.append(aarecord)

    isbn_dicts = {item['ean13']: item for item in get_isbn_dicts(session, canonical_isbn13s)}

    # Second pass
    for aarecord in aarecords:
        aarecord_id = aarecord['id']
        lgli_single_edition = aarecord['lgli_file']['editions'][0] if len((aarecord.get('lgli_file') or {}).get('editions') or []) == 1 else None
        lgli_all_editions = aarecord['lgli_file']['editions'] if aarecord.get('lgli_file') else []

        isbndb_all = []
        for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
            for isbndb in isbn_dicts[canonical_isbn13]['isbndb']:
                isbndb_all.append(isbndb)
        if len(isbndb_all) > 5:
            isbndb_all = []

        aarecord['indexes'] = ['aarecords']
        if aarecord['ia_record'] is not None:
            aarecord['indexes'].append('aarecords_online_borrow')

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
            ((aarecord['lgli_file'] or {}).get('scimag_archive_path') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('original_filename') or '').strip(),
        ]
        original_filename_multiple_processed = sort_by_length_and_filter_subsequences_with_longest_string(original_filename_multiple)
        aarecord['file_unified_data']['original_filename_best'] = min(original_filename_multiple_processed, key=len) if len(original_filename_multiple_processed) > 0 else ''
        aarecord['file_unified_data']['original_filename_additional'] = [s for s in original_filename_multiple_processed if s != aarecord['file_unified_data']['original_filename_best']]
        aarecord['file_unified_data']['original_filename_best_name_only'] =  re.split(r'[\\/]', aarecord['file_unified_data']['original_filename_best'])[-1]

        # Select the cover_url_normalized in order of what is likely to be the best one: ia, zlib, lgrsnf, lgrsfic, lgli.
        zlib_cover = ((aarecord['zlib_book'] or {}).get('cover_url') or '').strip()
        cover_url_multiple = [
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('cover_url') or '').strip(),
            # Put the zlib_cover at the beginning if it starts with the right prefix.
            # zlib_cover.strip() if zlib_cover.startswith('https://covers.zlibcdn2.com') else '',
            ((aarecord['lgrsnf_book'] or {}).get('cover_url_normalized') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('cover_url_normalized') or '').strip(),
            ((aarecord['lgli_file'] or {}).get('cover_url_guess_normalized') or '').strip(),
            # Otherwie put it at the end.
            # '' if zlib_cover.startswith('https://covers.zlibcdn2.com') else zlib_cover.strip(),
            # Temporarily always put it at the end because their servers are down.
            zlib_cover.strip()
        ]
        cover_url_multiple_processed = list(dict.fromkeys(filter(len, cover_url_multiple)))
        aarecord['file_unified_data']['cover_url_best'] = (cover_url_multiple_processed + [''])[0]
        aarecord['file_unified_data']['cover_url_additional'] = [s for s in cover_url_multiple_processed if s != aarecord['file_unified_data']['cover_url_best']]

        extension_multiple = [
            (((aarecord['ia_record'] or {}).get('aa_ia_file') or {}).get('extension') or '').strip(),
            ((aarecord['aac_zlib3_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['zlib_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgrsnf_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgrsfic_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgli_file'] or {}).get('extension') or '').strip().lower(),
        ]
        if "epub" in extension_multiple:
            aarecord['file_unified_data']['extension_best'] = "epub"
        elif "pdf" in extension_multiple:
            aarecord['file_unified_data']['extension_best'] = "pdf"
        else:
            aarecord['file_unified_data']['extension_best'] = max(extension_multiple, key=len)
        aarecord['file_unified_data']['extension_additional'] = [s for s in dict.fromkeys(filter(len, extension_multiple)) if s != aarecord['file_unified_data']['extension_best']]

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
        zlib_book_filesize = (aarecord['zlib_book'] or {}).get('filesize') or 0
        if zlib_book_filesize > 0:
            # If we have a zlib_book with a `filesize`, then that is leading, since we measured it ourselves.
            aarecord['file_unified_data']['filesize_best'] = zlib_book_filesize
        aarecord['file_unified_data']['filesize_additional'] = [s for s in dict.fromkeys(filter(lambda fz: fz > 0, filesize_multiple)) if s != aarecord['file_unified_data']['filesize_best']]

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
        title_multiple += [(isbndb.get('title_normalized') or '').strip() for isbndb in isbndb_all]
        if aarecord['file_unified_data']['title_best'] == '':
            aarecord['file_unified_data']['title_best'] = max(title_multiple, key=len)
        aarecord['file_unified_data']['title_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(title_multiple) if s != aarecord['file_unified_data']['title_best']]

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
        author_multiple += [", ".join(isbndb['json'].get('authors') or []) for isbndb in isbndb_all]
        if aarecord['file_unified_data']['author_best'] == '':
            aarecord['file_unified_data']['author_best'] = max(author_multiple, key=len)
        aarecord['file_unified_data']['author_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(author_multiple) if s != aarecord['file_unified_data']['author_best']]

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
        publisher_multiple += [(isbndb['json'].get('publisher') or '').strip() for isbndb in isbndb_all]
        if aarecord['file_unified_data']['publisher_best'] == '':
            aarecord['file_unified_data']['publisher_best'] = max(publisher_multiple, key=len)
        aarecord['file_unified_data']['publisher_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(publisher_multiple) if s != aarecord['file_unified_data']['publisher_best']]

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
        edition_varia_multiple += [(isbndb.get('edition_varia_normalized') or '').strip() for isbndb in isbndb_all]
        if aarecord['file_unified_data']['edition_varia_best'] == '':
            aarecord['file_unified_data']['edition_varia_best'] = max(edition_varia_multiple, key=len)
        aarecord['file_unified_data']['edition_varia_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(edition_varia_multiple) if s != aarecord['file_unified_data']['edition_varia_best']]

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
        year_multiple += [(isbndb.get('year_normalized') or '').strip() for isbndb in isbndb_all]
        for year in year_multiple:
            # If a year appears in edition_varia_best, then use that, for consistency.
            if year != '' and year in aarecord['file_unified_data']['edition_varia_best']:
                aarecord['file_unified_data']['year_best'] = year
        if aarecord['file_unified_data']['year_best'] == '':
            aarecord['file_unified_data']['year_best'] = max(year_multiple, key=len)
        aarecord['file_unified_data']['year_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(year_multiple) if s != aarecord['file_unified_data']['year_best']]

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
        if aarecord['file_unified_data']['comments_best'] == '':
            aarecord['file_unified_data']['comments_best'] = max(comments_multiple, key=len)
        aarecord['file_unified_data']['comments_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(comments_multiple) if s != aarecord['file_unified_data']['comments_best']]

        stripped_description_multiple = [
            ((aarecord['lgrsnf_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((aarecord['lgrsfic_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((lgli_single_edition or {}).get('stripped_description') or '').strip()[0:5000],
            ((aarecord['aac_zlib3_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((aarecord['zlib_book'] or {}).get('stripped_description') or '').strip()[0:5000],
        ]
        aarecord['file_unified_data']['stripped_description_best'] = max(stripped_description_multiple, key=len)
        stripped_description_multiple += [(edition.get('stripped_description') or '').strip()[0:5000] for edition in lgli_all_editions]
        stripped_description_multiple += [(isbndb['json'].get('synposis') or '').strip()[0:5000] for isbndb in isbndb_all]
        stripped_description_multiple += [(isbndb['json'].get('overview') or '').strip()[0:5000] for isbndb in isbndb_all]
        if aarecord['file_unified_data']['stripped_description_best'] == '':
            aarecord['file_unified_data']['stripped_description_best'] = max(stripped_description_multiple, key=len)
        ia_descr = (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('stripped_description_and_references') or '').strip()[0:5000]
        if len(ia_descr) > 0:
            stripped_description_multiple += [ia_descr]
            aarecord['file_unified_data']['stripped_description_best'] += '\n\n' + ia_descr
        aarecord['file_unified_data']['stripped_description_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(stripped_description_multiple) if s != aarecord['file_unified_data']['stripped_description_best']]

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
            aarecord['file_unified_data']['language_codes'] = combine_bcp47_lang_codes([(isbndb.get('language_codes') or []) for isbndb in isbndb_all])

        language_detection = ''
        if len(aarecord['file_unified_data']['stripped_description_best']) > 20:
            language_detect_string = " ".join(title_multiple) + " ".join(stripped_description_multiple)
            try:
                language_detection_data = ftlangdetect.detect(language_detect_string)
                if language_detection_data['score'] > 0.5: # Somewhat arbitrary cutoff
                    language_detection = language_detection_data['lang']
            except:
                pass

        # detected_language_codes_probs = []
        # for item in language_detection:
        #     for code in get_bcp47_lang_codes(item.lang):
        #         detected_language_codes_probs.append(f"{code}: {item.prob}")
        # aarecord['file_unified_data']['detected_language_codes_probs'] = ", ".join(detected_language_codes_probs)

        aarecord['file_unified_data']['most_likely_language_code'] = ''
        if len(aarecord['file_unified_data']['language_codes']) > 0:
            aarecord['file_unified_data']['most_likely_language_code'] = aarecord['file_unified_data']['language_codes'][0]
        elif len(language_detection) > 0:
            aarecord['file_unified_data']['most_likely_language_code'] = get_bcp47_lang_codes(language_detection)[0]

        aarecord['file_unified_data']['classifications_unified'] = allthethings.utils.merge_unified_fields([
            ((aarecord['lgrsnf_book'] or {}).get('classifications_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('classifications_unified') or {}),
            ((aarecord['aac_zlib3_book'] or {}).get('classifications_unified') or {}),
            ((aarecord['zlib_book'] or {}).get('classifications_unified') or {}),
            *[(edition.get('classifications_unified') or {}) for edition in lgli_all_editions],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('classifications_unified') or {}),
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
            aarecord ['ia_record'] = {
                'ia_id': aarecord['ia_record']['ia_id'],
                'has_thumb': aarecord['ia_record']['has_thumb'],
                'aa_ia_file': {
                    'type': aarecord['ia_record']['aa_ia_file']['type'],
                    'filesize': aarecord['ia_record']['aa_ia_file']['filesize'],
                    'extension': aarecord['ia_record']['aa_ia_file']['extension'],
                    'ia_id': aarecord['ia_record']['aa_ia_file']['ia_id'],
                },
            }

        # Even though `additional` is only for computing real-time stuff,
        # we'd like to cache some fields for in the search results.
        with force_locale('en'):
            additional = get_additional_for_aarecord(aarecord)
            aarecord['file_unified_data']['has_aa_downloads'] = additional['has_aa_downloads']
            aarecord['file_unified_data']['has_aa_exclusive_downloads'] = additional['has_aa_exclusive_downloads']

        aarecord['search_only_fields'] = {
            'search_filesize': aarecord['file_unified_data']['filesize_best'],
            'search_year': aarecord['file_unified_data']['year_best'],
            'search_extension': aarecord['file_unified_data']['extension_best'],
            'search_content_type': aarecord['file_unified_data']['content_type'],
            'search_most_likely_language_code': aarecord['file_unified_data']['most_likely_language_code'],
            'search_isbn13': (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []),
            'search_doi': (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []),
            'search_text': "\n".join(list(dict.fromkeys([
                aarecord['file_unified_data']['title_best'][:1000],
                aarecord['file_unified_data']['title_best'][:1000].replace('.', '. ').replace('_', ' ').replace('/', ' ').replace('\\', ' '),
                aarecord['file_unified_data']['author_best'][:1000],
                aarecord['file_unified_data']['author_best'][:1000].replace('.', '. ').replace('_', ' ').replace('/', ' ').replace('\\', ' '),
                aarecord['file_unified_data']['edition_varia_best'][:1000],
                aarecord['file_unified_data']['edition_varia_best'][:1000].replace('.', '. ').replace('_', ' ').replace('/', ' ').replace('\\', ' '),
                aarecord['file_unified_data']['publisher_best'][:1000],
                aarecord['file_unified_data']['publisher_best'][:1000].replace('.', '. ').replace('_', ' ').replace('/', ' ').replace('\\', ' '),
                aarecord['file_unified_data']['original_filename_best_name_only'][:1000],
                aarecord['file_unified_data']['original_filename_best_name_only'][:1000].replace('.', '. ').replace('_', ' ').replace('/', ' ').replace('\\', ' '),
                aarecord['file_unified_data']['extension_best'],
                *[f"{item} {key}:{item}" for key, items in aarecord['file_unified_data']['identifiers_unified'].items() for item in items],
                *[f"{item} {key}:{item}" for key, items in aarecord['file_unified_data']['classifications_unified'].items() for item in items],
                aarecord_id,
            ]))),
            'search_access_types': [
                *(['external_download'] if any([field in aarecord for field in ['lgrsnf_book', 'lgrsfic_book', 'lgli_file', 'zlib_book', 'aac_zlib3_book']]) else []),
                *(['external_borrow'] if any([field in aarecord for field in ['ia_record']]) else []),
                *(['aa_download'] if aarecord['file_unified_data']['has_aa_downloads'] == 1 else []),
            ],
            'search_record_sources': list(set([
                *(['lgrs'] if aarecord['lgrsnf_book'] is not None else []),
                *(['lgrs'] if aarecord['lgrsfic_book'] is not None else []),
                *(['lgli'] if aarecord['lgli_file'] is not None else []),
                *(['zlib'] if aarecord['zlib_book'] is not None else []),
                *(['zlib'] if aarecord['aac_zlib3_book'] is not None else []),
                *(['lgli'] if aarecord['aa_lgli_comics_2022_08_file'] is not None else []),
                *(['ia']   if aarecord['ia_record'] is not None else []),
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
            # Virtual field, only in searches:
            "book_any":           gettext("common.md5_content_type_mapping.book_any"),
        }
md5_content_type_book_any_subtypes = ["book_unknown","book_fiction","book_nonfiction"]

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
    targeted_seconds = 60
    if modifier == 'aa_exclusive':
        targeted_seconds = 180
        additional['has_aa_exclusive_downloads'] = 1
    if modifier == 'scimag':
        targeted_seconds = 3
    # When changing the domains, don't forget to change md5_fast_download and md5_slow_download.
    additional['fast_partner_urls'].append((gettext("common.md5.servers.fast_partner", number=len(additional['fast_partner_urls'])+1), '/fast_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/0', gettext("common.md5.servers.no_browser_verification") if len(additional['fast_partner_urls']) == 0 else ''))
    additional['fast_partner_urls'].append((gettext("common.md5.servers.fast_partner", number=len(additional['fast_partner_urls'])+1), '/fast_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/1', ''))
    additional['slow_partner_urls'].append((gettext("common.md5.servers.slow_partner", number=len(additional['slow_partner_urls'])+1), '/slow_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/0', gettext("common.md5.servers.browser_verification_unlimited", a_browser='href="/browser_verification"') if len(additional['slow_partner_urls']) == 0 else ''))
    additional['slow_partner_urls'].append((gettext("common.md5.servers.slow_partner", number=len(additional['slow_partner_urls'])+1), '/slow_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/1', ''))
    additional['slow_partner_urls'].append((gettext("common.md5.servers.slow_partner", number=len(additional['slow_partner_urls'])+1), '/slow_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/2', ''))
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
    additional = {}
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
    CODES_PRIORITY = ['isbn13', 'isbn10', 'doi', 'issn', 'udc', 'oclcworldcat', 'openlibrary', 'ocaid', 'asin']
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
                format_filesize(aarecord['file_unified_data'].get('filesize_best', None) or 0),
                aarecord['file_unified_data'].get('original_filename_best_name_only', None) or '',
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
            max_length_with_word_boundary(aarecord['file_unified_data'].get('title_best', None) or aarecord['file_unified_data'].get('original_filename_best_name_only', None) or '', 100),
            max_length_with_word_boundary(aarecord['file_unified_data'].get('author_best', None) or '', 100),
            max_length_with_word_boundary(aarecord['file_unified_data'].get('edition_varia_best', None) or '', 100),
            max_length_with_word_boundary(aarecord['file_unified_data'].get('publisher_best', None) or '', 100),
        ] if item != '']
    filename_slug = max_length_with_word_boundary(" -- ".join(filename_info), 200)
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
    if aarecord.get('ia_record') is not None:
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
        else:
            raise Exception("Unknown ia_record file type: {ia_file_type}")
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
        zlib_path = make_temp_anon_aac_zlib3_path(aarecord['aac_zlib3_book']['file_aacid'], aarecord['aac_zlib3_book']['file_data_folder'])
        add_partner_servers(zlib_path, 'aa_exclusive' if (len(additional['fast_partner_urls']) == 0) else '', aarecord, additional)
    for doi in (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []):
        additional['download_urls'].append((gettext('page.md5.box.download.scihub', doi=doi), f"https://sci-hub.ru/{doi}", gettext('page.md5.box.download.scihub_maybe')))
    if aarecord.get('zlib_book') is not None:
        additional['download_urls'].append((gettext('page.md5.box.download.zlib_tor'), f"http://zlibrary24tuxziyiyfr7zd46ytefdqbqd2axkmxm4o5374ptpc52fad.onion/md5/{aarecord['zlib_book']['md5_reported'].lower()}", gettext('page.md5.box.download.zlib_tor_extra')))
    if aarecord.get('aac_zlib3_book') is not None:
        additional['download_urls'].append((gettext('page.md5.box.download.zlib_tor'), f"http://zlibrary24tuxziyiyfr7zd46ytefdqbqd2axkmxm4o5374ptpc52fad.onion/md5/{aarecord['aac_zlib3_book']['md5_reported'].lower()}", gettext('page.md5.box.download.zlib_tor_extra')))
    if aarecord.get('ia_record') is not None:
        ia_id = aarecord['ia_record']['aa_ia_file']['ia_id']
        additional['download_urls'].append((gettext('page.md5.box.download.ia_borrow'), f"https://archive.org/details/{ia_id}", ''))
    additional['download_urls'].append((gettext('page.md5.box.download.bulk_torrents'), "/datasets", gettext('page.md5.box.download.experts_only')))
    additional['download_urls'] = additional['slow_partner_urls'] + additional['download_urls']
    return additional

def add_additional_to_aarecord(aarecord):
    return { **aarecord, 'additional': get_additional_for_aarecord(aarecord) }


@page.get("/md5/<string:md5_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def md5_page(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        return render_template("page/md5.html", header_active="search", md5_input=md5_input)

    if canonical_md5 != md5_input:
        return redirect(f"/md5/{canonical_md5}", code=301)

    with Session(engine) as session:
        aarecords = get_aarecords_elasticsearch(session, [f"md5:{canonical_md5}"])

        if len(aarecords) == 0:
            return render_template("page/md5.html", header_active="search", md5_input=md5_input)

        aarecord = aarecords[0]

        render_fields = {
            "header_active": "search",
            "md5_input": md5_input,
            "aarecord": aarecord,
            "md5_content_type_mapping": get_md5_content_type_mapping(allthethings.utils.get_base_lang_code(get_locale())),
            "md5_problem_type_mapping": get_md5_problem_type_mapping(),
            "md5_report_type_mapping": allthethings.utils.get_md5_report_type_mapping()
        }
        
        return render_template("page/md5.html", **render_fields)

@page.get("/db/aarecord/md5:<string:md5_input>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def md5_json(md5_input):
    with Session(engine) as session:
        md5_input = md5_input[0:50]
        canonical_md5 = md5_input.strip().lower()[0:32]
        if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
            return "{}", 404

        with Session(engine) as session:
            aarecords = get_aarecords_elasticsearch(session, [f"md5:{canonical_md5}"])
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
        aarecords = get_aarecords_elasticsearch(session, [f"md5:{canonical_md5}"])
        if len(aarecords) == 0:
            return render_template("page/md5.html", header_active="search", md5_input=md5_input)
        aarecord = aarecords[0]
        try:
            domain = ['momot.in', 'momot.rs'][domain_index]
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
            aarecords = get_aarecords_elasticsearch(session, [f"md5:{canonical_md5}"])
            if len(aarecords) == 0:
                return render_template("page/md5.html", header_active="search", md5_input=md5_input)
            aarecord = aarecords[0]
            try:
                domain = ['momot.rs', 'ktxr.rs', 'nrzr.li'][domain_index]
                path_info = aarecord['additional']['partner_url_paths'][path_index]
            except:
                return redirect(f"/md5/{md5_input}", code=302)

            cursor = mariapersist_session.connection().connection.cursor(pymysql.cursors.DictCursor)
            cursor.execute('SELECT COUNT(DISTINCT md5) AS count FROM mariapersist_slow_download_access WHERE timestamp > (NOW() - INTERVAL 24 HOUR) AND SUBSTRING(ip, 1, 8) = %(data_ip)s LIMIT 1', { "data_ip": data_ip })
            download_count_from_ip = cursor.fetchone()['count']
            minimum = 40
            maximum = 300
            targeted_seconds_multiplier = 1.0
            warning = False
            if download_count_from_ip > 500:
                targeted_seconds_multiplier = 3.0
                minimum = 20
                maximum = 50
                warning = True
            elif download_count_from_ip > 300:
                targeted_seconds_multiplier = 2.0
                minimum = 20
                maximum = 100
                warning = True
            elif download_count_from_ip > 150:
                targeted_seconds_multiplier = 1.5
                minimum = 20
                maximum = 150
                warning = False

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


# TODO: Remove search_most_likely_language_code == 'en' when we do a refresh, since this is now baked
# into the base score.
sort_search_aarecords_script = """
float score = params.boost + $('search_only_fields.search_score_base', 0);

score += _score / 100.0;

if (params.lang_code == $('search_only_fields.search_most_likely_language_code', '')) {
    score += 15.0;
}
if (params.lang_code == 'ca' && $('search_only_fields.search_most_likely_language_code', '') == 'es') {
    score += 10.0;
}
if (params.lang_code == 'bg' && $('search_only_fields.search_most_likely_language_code', '') == 'ru') {
    score += 10.0;
}
if ($('search_only_fields.search_most_likely_language_code', '') == 'en') {
    score += 5.0;
}

return score;
"""


search_query_aggs = {
    "search_most_likely_language_code": {
      "terms": { "field": "search_only_fields.search_most_likely_language_code", "size": 100 } 
    },
    "search_content_type": {
      "terms": { "field": "search_only_fields.search_content_type", "size": 200 } 
    },
    "search_extension": {
      "terms": { "field": "search_only_fields.search_extension", "size": 20 } 
    },
}

@functools.cache
def all_search_aggs(display_lang):
    search_results_raw = es.search(index="aarecords", size=0, aggs=search_query_aggs, timeout=ES_TIMEOUT)

    all_aggregations = {}
    # Unfortunately we have to special case the "unknown language", which is currently represented with an empty string `bucket['key'] != ''`, otherwise this gives too much trouble in the UI.
    all_aggregations['search_most_likely_language_code'] = []
    for bucket in search_results_raw['aggregations']['search_most_likely_language_code']['buckets']:
        if bucket['key'] == '':
            all_aggregations['search_most_likely_language_code'].append({ 'key': '_empty', 'label': get_display_name_for_lang('', display_lang), 'doc_count': bucket['doc_count'] })
        else:
            all_aggregations['search_most_likely_language_code'].append({ 'key': bucket['key'], 'label': get_display_name_for_lang(bucket['key'], display_lang), 'doc_count': bucket['doc_count'] })
    # We don't have browser_lang_codes for now..
    # total_doc_count = sum([record['doc_count'] for record in all_aggregations['search_most_likely_language_code']])
    # all_aggregations['search_most_likely_language_code'] = sorted(all_aggregations['search_most_likely_language_code'], key=lambda bucket: bucket['doc_count'] + (1000000000 if bucket['key'] in browser_lang_codes and bucket['doc_count'] >= total_doc_count//100 else 0), reverse=True)

    content_type_buckets = list(search_results_raw['aggregations']['search_content_type']['buckets'])
    md5_content_type_mapping = get_md5_content_type_mapping(display_lang)
    book_any_total = sum([bucket['doc_count'] for bucket in content_type_buckets if bucket['key'] in md5_content_type_book_any_subtypes])
    content_type_buckets.append({'key': 'book_any', 'doc_count': book_any_total})
    all_aggregations['search_content_type'] = [{ 'key': bucket['key'], 'label': md5_content_type_mapping[bucket['key']], 'doc_count': bucket['doc_count'] } for bucket in content_type_buckets]
    content_type_keys_present = set([bucket['key'] for bucket in content_type_buckets])
    for key, label in md5_content_type_mapping.items():
        if key not in content_type_keys_present:
            all_aggregations['search_content_type'].append({ 'key': key, 'label': label, 'doc_count': 0 })
    all_aggregations['search_content_type'] = sorted(all_aggregations['search_content_type'], key=lambda bucket: bucket['doc_count'], reverse=True)

    # Similarly to the "unknown language" issue above, we have to filter for empty-string extensions, since it gives too much trouble.
    all_aggregations['search_extension'] = []
    for bucket in search_results_raw['aggregations']['search_extension']['buckets']:
        if bucket['key'] == '':
            all_aggregations['search_extension'].append({ 'key': '_empty', 'label': 'unknown', 'doc_count': bucket['doc_count'] })
        else:
            all_aggregations['search_extension'].append({ 'key': bucket['key'], 'label': bucket['key'], 'doc_count': bucket['doc_count'] })

    return all_aggregations


@page.get("/random_book")
@allthethings.utils.no_cache()
def random_book():
    """
    Gets a random record from the elastic search index and redirects to the page for that book.
    If no record is found, redirects to the search page.
    """
    random_aarecord = get_random_aarecord_elasticsearch()
    if random_aarecord is not None:
        return redirect(random_aarecord['_source']['path'], code=301)

    return redirect("/search", code=302)


@page.get("/search")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24*30)
def search_page():
    search_input = request.args.get("q", "").strip()
    filter_values = {
        'search_most_likely_language_code': request.args.get("lang", "").strip()[0:15],
        'search_content_type': request.args.get("content", "").strip()[0:25],
        'search_extension': request.args.get("ext", "").strip()[0:10],
    }
    sort_value = request.args.get("sort", "").strip()

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
    for filter_key, filter_value in filter_values.items():
        if filter_value != '':
            if filter_key == 'search_content_type' and filter_value == 'book_any':
                post_filter.append({ "terms": { f"search_only_fields.search_content_type": md5_content_type_book_any_subtypes } })
            elif filter_value == '_empty':
                post_filter.append({ "term": { f"search_only_fields.{filter_key}": '' } })
            else:
                post_filter.append({ "term": { f"search_only_fields.{filter_key}": filter_value } })

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
            "should": [{
                "script_score": {
                    "query": { "match_phrase": { "search_only_fields.search_text": { "query": search_input } } },
                    "script": {
                        "source": sort_search_aarecords_script,
                        "params": { "lang_code": allthethings.utils.get_base_lang_code(get_locale()), "boost": 100000 }
                    }
                }
            }],
            "must": [{
                "script_score": {
                    "query": { "simple_query_string": {"query": search_input, "fields": ["search_only_fields.search_text"], "default_operator": "and"} },
                    "script": {
                        "source": sort_search_aarecords_script,
                        "params": { "lang_code": allthethings.utils.get_base_lang_code(get_locale()), "boost": 0 }
                    }
                }
            }]
        }
    }

    max_display_results = 200
    max_additional_display_results = 50

    search_results_raw = es.search(
        index="aarecords", 
        size=max_display_results, 
        query=search_query,
        aggs=search_query_aggs,
        post_filter={ "bool": { "filter": post_filter } },
        sort=custom_search_sorting+['_score'],
        track_total_hits=False,
        timeout=ES_TIMEOUT,
    )

    all_aggregations = all_search_aggs(allthethings.utils.get_base_lang_code(get_locale()))

    doc_counts = {}
    doc_counts['search_most_likely_language_code'] = {}
    doc_counts['search_content_type'] = {}
    doc_counts['search_extension'] = {}
    if search_input == '':
        for bucket in all_aggregations['search_most_likely_language_code']:
            doc_counts['search_most_likely_language_code'][bucket['key']] = bucket['doc_count']
        for bucket in all_aggregations['search_content_type']:
            doc_counts['search_content_type'][bucket['key']] = bucket['doc_count']
        for bucket in all_aggregations['search_extension']:
            doc_counts['search_extension'][bucket['key']] = bucket['doc_count']
    else:
        for bucket in search_results_raw['aggregations']['search_most_likely_language_code']['buckets']:
            doc_counts['search_most_likely_language_code'][bucket['key'] if bucket['key'] != '' else '_empty'] = bucket['doc_count']
        # Special casing for "book_any":
        doc_counts['search_content_type']['book_any'] = 0
        for bucket in search_results_raw['aggregations']['search_content_type']['buckets']:
            doc_counts['search_content_type'][bucket['key']] = bucket['doc_count']
            if bucket['key'] in md5_content_type_book_any_subtypes:
                doc_counts['search_content_type']['book_any'] += bucket['doc_count']
        for bucket in search_results_raw['aggregations']['search_extension']['buckets']:
            doc_counts['search_extension'][bucket['key'] if bucket['key'] != '' else '_empty'] = bucket['doc_count']

    aggregations = {}
    aggregations['search_most_likely_language_code'] = [{
            **bucket,
            'doc_count': doc_counts['search_most_likely_language_code'].get(bucket['key'], 0),
            'selected':  (bucket['key'] == filter_values['search_most_likely_language_code']),
        } for bucket in all_aggregations['search_most_likely_language_code']]
    aggregations['search_content_type'] = [{
            **bucket,
            'doc_count': doc_counts['search_content_type'].get(bucket['key'], 0),
            'selected':  (bucket['key'] == filter_values['search_content_type']),
        } for bucket in all_aggregations['search_content_type']]
    aggregations['search_extension'] = [{
            **bucket,
            'doc_count': doc_counts['search_extension'].get(bucket['key'], 0),
            'selected':  (bucket['key'] == filter_values['search_extension']),
        } for bucket in all_aggregations['search_extension']]

    aggregations['search_most_likely_language_code'] = sorted(aggregations['search_most_likely_language_code'], key=lambda bucket: bucket['doc_count'], reverse=True)
    aggregations['search_content_type']              = sorted(aggregations['search_content_type'],              key=lambda bucket: bucket['doc_count'], reverse=True)
    aggregations['search_extension']                 = sorted(aggregations['search_extension'],                 key=lambda bucket: bucket['doc_count'], reverse=True)

    search_aarecords = [add_additional_to_aarecord(aarecord_raw['_source']) for aarecord_raw in search_results_raw['hits']['hits'] if aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]

    max_search_aarecords_reached = False
    max_additional_search_aarecords_reached = False
    additional_search_aarecords = []

    if len(search_aarecords) < max_display_results:
        # For partial matches, first try our original query again but this time without filters.
        seen_ids = set([aarecord['id'] for aarecord in search_aarecords])
        search_results_raw = es.search(
            index="aarecords", 
            size=len(seen_ids)+max_additional_display_results, # This way, we'll never filter out more than "max_display_results" results because we have seen them already., 
            query=search_query,
            sort=custom_search_sorting+['_score'],
            track_total_hits=False,
            timeout=ES_TIMEOUT,
        )
        if len(seen_ids)+len(search_results_raw['hits']['hits']) >= max_additional_display_results:
            max_additional_search_aarecords_reached = True
        additional_search_aarecords = [add_additional_to_aarecord(aarecord_raw['_source']) for aarecord_raw in search_results_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]

        # Then do an "OR" query, but this time with the filters again.
        if len(search_aarecords) + len(additional_search_aarecords) < max_display_results:
            seen_ids = seen_ids.union(set([aarecord['id'] for aarecord in additional_search_aarecords]))
            search_results_raw = es.search(
                index="aarecords",
                size=len(seen_ids)+max_additional_display_results, # This way, we'll never filter out more than "max_display_results" results because we have seen them already.
                # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                query={"bool": { "must": { "match": { "search_only_fields.search_text": { "query": search_input } } }, "filter": post_filter } },
                sort=custom_search_sorting+['_score'],
                track_total_hits=False,
                timeout=ES_TIMEOUT,
            )
            if len(seen_ids)+len(search_results_raw['hits']['hits']) >= max_additional_display_results:
                max_additional_search_aarecords_reached = True
            additional_search_aarecords += [add_additional_to_aarecord(aarecord_raw['_source']) for aarecord_raw in search_results_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]

            # If we still don't have enough, do another OR query but this time without filters.
            if len(search_aarecords) + len(additional_search_aarecords) < max_display_results:
                seen_ids = seen_ids.union(set([aarecord['id'] for aarecord in additional_search_aarecords]))
                search_results_raw = es.search(
                    index="aarecords",
                    size=len(seen_ids)+max_additional_display_results, # This way, we'll never filter out more than "max_display_results" results because we have seen them already.
                    # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                    query={"bool": { "must": { "match": { "search_only_fields.search_text": { "query": search_input } } } } },
                    sort=custom_search_sorting+['_score'],
                    track_total_hits=False,
                    timeout=ES_TIMEOUT,
                )
                if len(seen_ids)+len(search_results_raw['hits']['hits']) >= max_additional_display_results:
                    max_additional_search_aarecords_reached = True
                additional_search_aarecords += [add_additional_to_aarecord(aarecord_raw['_source']) for aarecord_raw in search_results_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]
    else:
        max_search_aarecords_reached = True

    
    search_dict = {}
    search_dict['search_aarecords'] = search_aarecords[0:max_display_results]
    search_dict['additional_search_aarecords'] = additional_search_aarecords[0:max_additional_display_results]
    search_dict['max_search_aarecords_reached'] = max_search_aarecords_reached
    search_dict['max_additional_search_aarecords_reached'] = max_additional_search_aarecords_reached
    search_dict['aggregations'] = aggregations
    search_dict['sort_value'] = sort_value

    return render_template(
        "page/search.html",
        header_active="search",
        search_input=search_input,
        search_dict=search_dict,
        redirect_pages={
            'ol_page': ol_page,
            'doi_page': doi_page,
            'isbn_page': isbn_page,
        }
    )
