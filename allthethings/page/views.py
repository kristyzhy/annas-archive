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
import cachetools
import time
import sentence_transformers
import struct
import natsort

from flask import g, Blueprint, __version__, render_template, make_response, redirect, request, send_file
from allthethings.extensions import engine, es, es_aux, babel, mariapersist_engine, ZlibBook, ZlibIsbn, IsbndbIsbns, LibgenliEditions, LibgenliEditionsAddDescr, LibgenliEditionsToFiles, LibgenliElemDescr, LibgenliFiles, LibgenliFilesAddDescr, LibgenliPublishers, LibgenliSeries, LibgenliSeriesAddDescr, LibgenrsDescription, LibgenrsFiction, LibgenrsFictionDescription, LibgenrsFictionHashes, LibgenrsHashes, LibgenrsTopics, LibgenrsUpdated, OlBase, AaIa202306Metadata, AaIa202306Files, Ia2Records, Ia2AcsmpdfFiles, MariapersistSmallFiles
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
    "md5:ffdbec06986b84f24fc786d89ce46528",
    "md5:ca10d6b2ee5c758955ff468591ad67d9",
]

ES_TIMEOUT_PRIMARY = "1s"
ES_TIMEOUT_ALL_AGG = "20s"
ES_TIMEOUT = "500ms"

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
# * http://localhost:8000/db/lgrsnf/288054.json
# * http://localhost:8000/db/lgrsnf/3175616.json
# * http://localhost:8000/db/lgrsnf/2933905.json
# * http://localhost:8000/db/lgrsnf/1125703.json
# * http://localhost:8000/db/lgrsnf/59.json
# * http://localhost:8000/db/lgrsnf/1195487.json
# * http://localhost:8000/db/lgrsnf/1360257.json
# * http://localhost:8000/db/lgrsnf/357571.json
# * http://localhost:8000/db/lgrsnf/2425562.json
# * http://localhost:8000/db/lgrsnf/3354081.json
# * http://localhost:8000/db/lgrsnf/3357578.json
# * http://localhost:8000/db/lgrsnf/3357145.json
# * http://localhost:8000/db/lgrsnf/2040423.json
# * http://localhost:8000/db/lgrsfic/1314135.json
# * http://localhost:8000/db/lgrsfic/25761.json
# * http://localhost:8000/db/lgrsfic/2443846.json
# * http://localhost:8000/db/lgrsfic/2473252.json
# * http://localhost:8000/db/lgrsfic/2340232.json
# * http://localhost:8000/db/lgrsfic/1122239.json
# * http://localhost:8000/db/lgrsfic/6862.json
# * http://localhost:8000/db/lgli/100.json
# * http://localhost:8000/db/lgli/1635550.json
# * http://localhost:8000/db/lgli/94069002.json
# * http://localhost:8000/db/lgli/40122.json
# * http://localhost:8000/db/lgli/21174.json
# * http://localhost:8000/db/lgli/91051161.json
# * http://localhost:8000/db/lgli/733269.json
# * http://localhost:8000/db/lgli/156965.json
# * http://localhost:8000/db/lgli/10000000.json
# * http://localhost:8000/db/lgli/933304.json
# * http://localhost:8000/db/lgli/97559799.json
# * http://localhost:8000/db/lgli/3756440.json
# * http://localhost:8000/db/lgli/91128129.json
# * http://localhost:8000/db/lgli/44109.json
# * http://localhost:8000/db/lgli/2264591.json
# * http://localhost:8000/db/lgli/151611.json
# * http://localhost:8000/db/lgli/1868248.json
# * http://localhost:8000/db/lgli/1761341.json
# * http://localhost:8000/db/lgli/4031847.json
# * http://localhost:8000/db/lgli/2827612.json
# * http://localhost:8000/db/lgli/2096298.json
# * http://localhost:8000/db/lgli/96751802.json
# * http://localhost:8000/db/lgli/5064830.json
# * http://localhost:8000/db/lgli/1747221.json
# * http://localhost:8000/db/lgli/1833886.json
# * http://localhost:8000/db/lgli/3908879.json
# * http://localhost:8000/db/lgli/41752.json
# * http://localhost:8000/db/lgli/97768237.json
# * http://localhost:8000/db/lgli/4031335.json
# * http://localhost:8000/db/lgli/1842179.json
# * http://localhost:8000/db/lgli/97562793.json
# * http://localhost:8000/db/lgli/4029864.json
# * http://localhost:8000/db/lgli/2834701.json
# * http://localhost:8000/db/lgli/97562143.json
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
    return re.sub(r'<[^<]+?>', r' ', re.sub(r'<a.+?href="([^"]+)"[^>]*>', r'(\1) ', description.replace('</p>', '\n\n').replace('</P>', '\n\n').replace('<br>', '\n').replace('<BR>', '\n').replace('.', '. ').replace(',', ', '))).strip()

def nice_json(some_dict):
    json_str = orjson.dumps(some_dict, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS, default=str).decode('utf-8')
    # Triple-slashes means it shouldn't be put on the previous line.
    return re.sub(r'[ \n]*"//(?!/)', ' "//', json_str, flags=re.MULTILINE)


# A mapping of countries to languages, for those countries that have a clear single spoken language.
# Courtesy of a friendly LLM.. beware of hallucinations!
country_lang_mapping = { "Albania": "Albanian", "Algeria": "Arabic", "Andorra": "Catalan", "Argentina": "Spanish", "Armenia": "Armenian", 
"Azerbaijan": "Azerbaijani", "Bahrain": "Arabic", "Bangladesh": "Bangla", "Belarus": "Belorussian", "Benin": "French", 
"Bhutan": "Dzongkha", "Brazil": "Portuguese", "Brunei Darussalam": "Malay", "Bulgaria": "Bulgarian", "Cambodia": "Khmer", 
"Caribbean Community": "English", "Chile": "Spanish", "China": "Mandarin", "Colombia": "Spanish", "Costa Rica": "Spanish", 
"Croatia": "Croatian", "Cuba": "Spanish", "Cur": "Papiamento", "Cyprus": "Greek", "Denmark": "Danish", 
"Dominican Republic": "Spanish", "Ecuador": "Spanish", "Egypt": "Arabic", "El Salvador": "Spanish", "Estonia": "Estonian", 
"Finland": "Finnish", "France": "French", "Gambia": "English", "Georgia": "Georgian", "Ghana": "English", "Greece": "Greek", 
"Guatemala": "Spanish", "Honduras": "Spanish", "Hungary": "Hungarian", "Iceland": "Icelandic", "Indonesia": "Bahasa Indonesia", 
"Iran": "Persian", "Iraq": "Arabic", "Israel": "Hebrew", "Italy": "Italian", "Japan": "Japanese", "Jordan": "Arabic", 
"Kazakhstan": "Kazak", "Kuwait": "Arabic", "Latvia": "Latvian", "Lebanon": "Arabic", "Libya": "Arabic", "Lithuania": "Lithuanian", 
"Malaysia": "Malay", "Maldives": "Dhivehi", "Mexico": "Spanish", "Moldova": "Moldovan", "Mongolia": "Mongolian", 
"Myanmar": "Burmese", "Namibia": "English", "Nepal": "Nepali", "Netherlands": "Dutch", "Nicaragua": "Spanish", 
"North Macedonia": "Macedonian", "Norway": "Norwegian", "Oman": "Arabic", "Pakistan": "Urdu", "Palestine": "Arabic", 
"Panama": "Spanish", "Paraguay": "Spanish", "Peru": "Spanish", "Philippines": "Filipino", "Poland": "Polish", "Portugal": "Portuguese", 
"Qatar": "Arabic", "Romania": "Romanian", "Saudi Arabia": "Arabic", "Slovenia": "Slovenian", "South Pacific": "English", "Spain": "Spanish", 
"Srpska": "Serbian", "Sweden": "Swedish", "Thailand": "Thai", "Turkey": "Turkish", "Ukraine": "Ukrainian", 
"United Arab Emirates": "Arabic", "United States": "English", "Uruguay": "Spanish", "Venezuela": "Spanish", "Vietnam": "Vietnamese" }

@functools.cache
def get_e5_small_model():
    return sentence_transformers.SentenceTransformer("intfloat/multilingual-e5-small")

@functools.cache
def get_bcp47_lang_codes_parse_substr(substr):
    lang = ''
    try:
        lang = str(langcodes.standardize_tag(langcodes.get(substr), macro=True))
    except langcodes.tag_parser.LanguageTagError:
        for country_name, language_name in country_lang_mapping.items():
            if country_name.lower() in substr.lower():
                try:
                    lang = str(langcodes.standardize_tag(langcodes.find(language_name), macro=True))
                except LookupError:
                    pass
                break
        if lang == '':
            try:
                lang = str(langcodes.standardize_tag(langcodes.find(substr), macro=True))
            except LookupError:
                # In rare cases, disambiguate by saying that `substr` is written in English
                try:
                    lang = str(langcodes.standardize_tag(langcodes.find(substr, language='en'), macro=True))
                except LookupError:
                    lang = ''
    # Further specification is unnecessary for most languages, except Traditional Chinese.
    if ('-' in lang) and (lang != 'zh-Hant'):
        lang = lang.split('-', 1)[0]
    # We have a bunch of weird data that gets interpreted as "Egyptian Sign Language" when it's
    # clearly all just Spanish..
    if lang == 'esl':
        lang = 'es'
    # Seems present within ISBNdb, and just means "en".
    if lang == 'us':
        lang = 'en'
    # "urdu" not being converted to "ur" seems to be a bug in langcodes?
    if lang == 'urdu':
        lang = 'ur'
    if lang in ['und', 'mul']:
        lang = ''
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def login_page():
    return redirect(f"/account", code=301)
    # return render_template("page/login.html", header_active="account")

@page.get("/about")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def security_page():
    return render_template("page/security.html", header_active="home/security")

@page.get("/mobile")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def mobile_page():
    return render_template("page/mobile.html", header_active="home/mobile")

# @page.get("/wechat")
# @allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
# def wechat_page():
#     return render_template("page/wechat.html", header_active="home/wechat")

@page.get("/llm")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def llm_page():
    return render_template("page/llm.html", header_active="home/llm")

@page.get("/mirrors")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def mirrors_page():
    return render_template("page/mirrors.html", header_active="home/mirrors")

@page.get("/browser_verification")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def browser_verification_page():
    return render_template("page/browser_verification.html", header_active="home/search")

@cachetools.cached(cache=cachetools.TTLCache(maxsize=30000, ttl=24*60*60))
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
        cursor.execute('SELECT metadata FROM annas_archive_meta__aacid__zlib3_records ORDER BY primary_id DESC, aacid DESC LIMIT 1')
        zlib3_record = cursor.fetchone()
        zlib_date = orjson.loads(zlib3_record['metadata'])['date_modified'] if zlib3_record is not None else ''

        stats_data_es = dict(es.msearch(
            request_timeout=30,
            max_concurrent_searches=10,
            max_concurrent_shard_requests=10,
            searches=[
                # { "index": allthethings.utils.all_virtshards_for_index("aarecords")+allthethings.utils.all_virtshards_for_index("aarecords_journals"), "request_cache": False },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords")+allthethings.utils.all_virtshards_for_index("aarecords_journals") },
                { "track_total_hits": True, "timeout": "20s", "size": 0, "aggs": { "total_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } } },
                # { "index": allthethings.utils.all_virtshards_for_index("aarecords"), "request_cache": False },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords") },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": {
                        "search_record_sources": {
                            "terms": { "field": "search_only_fields.search_record_sources" },
                            "aggs": {
                                "search_filesize": { "sum": { "field": "search_only_fields.search_filesize" } },
                                "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } },
                                "search_bulk_torrents": { "terms": { "field": "search_only_fields.search_bulk_torrents", "include": "has_bulk_torrents" } },
                            },
                        },
                    },
                },
                # { "index": allthethings.utils.all_virtshards_for_index("aarecords_journals"), "request_cache": False },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords_journals") },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": { "search_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } },
                },
                # { "index": allthethings.utils.all_virtshards_for_index("aarecords_journals"), "request_cache": False },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords_journals") },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": {
                        "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } },
                        "search_bulk_torrents": { "terms": { "field": "search_only_fields.search_bulk_torrents", "include": "has_bulk_torrents" } },
                    },
                },
                # { "index": allthethings.utils.all_virtshards_for_index("aarecords")+allthethings.utils.all_virtshards_for_index("aarecords_journals"), "request_cache": False },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords")+allthethings.utils.all_virtshards_for_index("aarecords_journals") },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": {
                        "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } },
                        "search_bulk_torrents": { "terms": { "field": "search_only_fields.search_bulk_torrents", "include": "has_bulk_torrents" } },
                    },
                },
            ],
        ))
        stats_data_es_aux = dict(es_aux.msearch(
            request_timeout=30,
            max_concurrent_searches=10,
            max_concurrent_shard_requests=10,
            searches=[
                # { "index": allthethings.utils.all_virtshards_for_index("aarecords_digital_lending"), "request_cache": False },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords_digital_lending") },
                { "track_total_hits": True, "timeout": "20s", "size": 0, "aggs": { "total_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } } },
            ],
        ))
        responses_without_timed_out = [response for response in (stats_data_es['responses'] + stats_data_es_aux['responses']) if 'timed_out' not in response]
        if len(responses_without_timed_out) > 0:
            raise Exception(f"One of the 'get_stats_data' responses didn't have 'timed_out' field in it: {responses_without_timed_out=}")
        if any([response['timed_out'] for response in (stats_data_es['responses'] + stats_data_es_aux['responses'])]):
            # WARNING: don't change this message because we match on 'timed out' below
            raise Exception("One of the 'get_stats_data' responses timed out")

        # print(f'{orjson.dumps(stats_data_es)=}')

        stats_by_group = {}
        for bucket in stats_data_es['responses'][1]['aggregations']['search_record_sources']['buckets']:
            stats_by_group[bucket['key']] = {
                'count': bucket['doc_count'],
                'filesize': bucket['search_filesize']['value'],
                'aa_count': bucket['search_access_types']['buckets'][0]['doc_count'],
                'torrent_count': bucket['search_bulk_torrents']['buckets'][0]['doc_count'] if len(bucket['search_bulk_torrents']['buckets']) > 0 else 0,
            }
        stats_by_group['journals'] = {
            'count': stats_data_es['responses'][2]['hits']['total']['value'],
            'filesize': stats_data_es['responses'][2]['aggregations']['search_filesize']['value'],
            'aa_count': stats_data_es['responses'][3]['aggregations']['search_access_types']['buckets'][0]['doc_count'],
            'torrent_count': stats_data_es['responses'][3]['aggregations']['search_bulk_torrents']['buckets'][0]['doc_count'] if len(stats_data_es['responses'][3]['aggregations']['search_bulk_torrents']['buckets']) > 0 else 0,
        }
        stats_by_group['total'] = {
            'count': stats_data_es['responses'][0]['hits']['total']['value'],
            'filesize': stats_data_es['responses'][0]['aggregations']['total_filesize']['value'],
            'aa_count': stats_data_es['responses'][4]['aggregations']['search_access_types']['buckets'][0]['doc_count'],
            'torrent_count': stats_data_es['responses'][4]['aggregations']['search_bulk_torrents']['buckets'][0]['doc_count'] if len(stats_data_es['responses'][4]['aggregations']['search_bulk_torrents']['buckets']) > 0 else 0,
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
        'duxiu_date': '~2023',
        'isbndb_date': '2022-09-01',
        'isbn_country_date': '2022-02-11',
        'oclc_date': '2023-10-01',
    }

def torrent_group_data_from_file_path(file_path):
    group = file_path.split('/')[2]
    aac_meta_group = None
    aac_meta_prefix = 'torrents/managed_by_aa/annas_archive_meta__aacid/annas_archive_meta__aacid__'
    if file_path.startswith(aac_meta_prefix):
        aac_meta_group = file_path[len(aac_meta_prefix):].split('__', 1)[0]
        group = aac_meta_group
    aac_data_prefix = 'torrents/managed_by_aa/annas_archive_data__aacid/annas_archive_data__aacid__'
    if file_path.startswith(aac_data_prefix):
        group = file_path[len(aac_data_prefix):].split('__', 1)[0]
    if 'zlib3' in file_path:
        group = 'zlib'
    if '_ia2_' in file_path:
        group = 'ia'
    if 'duxiu' in file_path:
        group = 'duxiu'

    return { 'group': group, 'aac_meta_group': aac_meta_group }

@cachetools.cached(cache=cachetools.TTLCache(maxsize=1024, ttl=30*60))
def get_torrents_data():
    with mariapersist_engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        # cursor.execute('SELECT mariapersist_small_files.created, mariapersist_small_files.file_path, mariapersist_small_files.metadata, s.metadata AS scrape_metadata, s.created AS scrape_created FROM mariapersist_small_files LEFT JOIN (SELECT mariapersist_torrent_scrapes.* FROM mariapersist_torrent_scrapes INNER JOIN (SELECT file_path, MAX(created) AS max_created FROM mariapersist_torrent_scrapes GROUP BY file_path) s2 ON (mariapersist_torrent_scrapes.file_path = s2.file_path AND mariapersist_torrent_scrapes.created = s2.max_created)) s USING (file_path) WHERE mariapersist_small_files.file_path LIKE "torrents/managed_by_aa/%" GROUP BY mariapersist_small_files.file_path ORDER BY created ASC, scrape_created DESC LIMIT 50000')
        cursor.execute('SELECT created, file_path, metadata FROM mariapersist_small_files WHERE mariapersist_small_files.file_path LIKE "torrents/%" ORDER BY created, file_path LIMIT 50000')
        small_files = cursor.fetchall()
        cursor.execute('SELECT * FROM mariapersist_torrent_scrapes INNER JOIN (SELECT file_path, MAX(created) AS max_created FROM mariapersist_torrent_scrapes GROUP BY file_path) s2 ON (mariapersist_torrent_scrapes.file_path = s2.file_path AND mariapersist_torrent_scrapes.created = s2.max_created)')
        scrapes_by_file_path = { row['file_path']: row for row in cursor.fetchall() }

        group_sizes = collections.defaultdict(int)
        small_file_dicts_grouped_aa = collections.defaultdict(list)
        small_file_dicts_grouped_external = collections.defaultdict(list)
        aac_meta_file_paths_grouped = collections.defaultdict(list)
        seeder_sizes = collections.defaultdict(int)
        for small_file in small_files:
            metadata = orjson.loads(small_file['metadata'])
            toplevel = small_file['file_path'].split('/')[1]

            torrent_group_data = torrent_group_data_from_file_path(small_file['file_path'])
            group = torrent_group_data['group']
            if torrent_group_data['aac_meta_group'] != None:
                aac_meta_file_paths_grouped[torrent_group_data['aac_meta_group']].append(small_file['file_path'])

            scrape_row = scrapes_by_file_path.get(small_file['file_path'])
            scrape_metadata = {"scrape":{}}
            scrape_created = datetime.datetime.utcnow()
            if scrape_row is not None:
                scrape_created = scrape_row['created']
                scrape_metadata = orjson.loads(scrape_row['metadata'])
                if (metadata.get('embargo') or False) == False:
                    if scrape_metadata['scrape']['seeders'] < 4:
                        seeder_sizes[0] += metadata['data_size']
                    elif scrape_metadata['scrape']['seeders'] < 11:
                        seeder_sizes[1] += metadata['data_size']
                    else:
                        seeder_sizes[2] += metadata['data_size']

            group_sizes[group] += metadata['data_size']
            if toplevel == 'external':
                list_to_add = small_file_dicts_grouped_external[group]
            else:
                list_to_add = small_file_dicts_grouped_aa[group]
            display_name = small_file['file_path'].split('/')[-1]
            list_to_add.append({
                "created": small_file['created'].strftime("%Y-%m-%d"), # First, so it gets sorted by first. Also, only year-month-day, so it gets secondarily sorted by file path.
                "file_path": small_file['file_path'],
                "metadata": metadata, 
                "aa_currently_seeding": allthethings.utils.aa_currently_seeding(metadata),
                "size_string": format_filesize(metadata['data_size']), 
                "file_path_short": small_file['file_path'].replace('torrents/managed_by_aa/annas_archive_meta__aacid/', '').replace('torrents/managed_by_aa/annas_archive_data__aacid/', '').replace(f'torrents/managed_by_aa/{group}/', '').replace(f'torrents/external/{group}/', ''),
                "display_name": display_name, 
                "scrape_metadata": scrape_metadata, 
                "scrape_created": scrape_created, 
                "is_metadata": (('annas_archive_meta__' in small_file['file_path']) or ('.sql' in small_file['file_path']) or ('-index-' in small_file['file_path']) or ('-derived' in small_file['file_path']) or ('isbndb' in small_file['file_path']) or ('covers-' in small_file['file_path']) or ('-metadata-' in small_file['file_path']) or ('-thumbs' in small_file['file_path']) or ('.csv' in small_file['file_path'])),
                "magnet_link": f"magnet:?xt=urn:btih:{metadata['btih']}&dn={urllib.parse.quote(display_name)}&tr=udp://tracker.opentrackr.org:1337/announce",
                "temp_uuid": shortuuid.uuid(),
            })

        for key in small_file_dicts_grouped_external:
            small_file_dicts_grouped_external[key] = natsort.natsorted(small_file_dicts_grouped_external[key], key=lambda x: list(x.values()))
        for key in small_file_dicts_grouped_aa:
            small_file_dicts_grouped_aa[key] = natsort.natsorted(small_file_dicts_grouped_aa[key], key=lambda x: list(x.values()))

        obsolete_file_paths = [
            'torrents/managed_by_aa/zlib/pilimi-zlib-index-2022-06-28.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics0__shoutout_to_tosec.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics1__adopted_by_yperion.tar.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics2__never_give_up_against_elsevier.tar.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics4__for_science.tar.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics3.0__hone_the_hachette.tar.torrent',
            'torrents/managed_by_aa/libgenli_comics/comics3.1__adopted_by_oskanios.tar.torrent',
            'torrents/managed_by_aa/libgenli_comics/c_2022_12_thousand_dirs.torrent',
            'torrents/managed_by_aa/libgenli_comics/c_2022_12_thousand_dirs_magz.torrent',
        ]
        for file_path_list in aac_meta_file_paths_grouped.values():
            obsolete_file_paths += file_path_list[0:-1]

        # Tack on "obsolete" fields, now that we have them
        for group in list(small_file_dicts_grouped_aa.values()) + list(small_file_dicts_grouped_external.values()):
            for item in group:
                item['obsolete'] = (item['file_path'] in obsolete_file_paths)

        # TODO: exclude obsolete
        group_size_strings = { group: format_filesize(total) for group, total in group_sizes.items() }
        seeder_size_strings = { index: format_filesize(seeder_sizes[index]) for index in [0,1,2] }

        return {
            'small_file_dicts_grouped': {
                'managed_by_aa': dict(sorted(small_file_dicts_grouped_aa.items())),
                'external': dict(sorted(small_file_dicts_grouped_external.items())),
            },
            'group_size_strings': group_size_strings,
            'seeder_size_strings': seeder_size_strings,
        }

@page.get("/datasets")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def datasets_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/ia")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def datasets_ia_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_ia.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/duxiu")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def datasets_duxiu_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_duxiu.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/zlib")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def datasets_zlib_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_zlib.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/isbndb")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def datasets_isbndb_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_isbndb.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/scihub")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def datasets_scihub_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_scihub.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/libgen_rs")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def datasets_libgen_rs_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_libgen_rs.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/libgen_li")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def datasets_libgen_li_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_libgen_li.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/openlib")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def datasets_openlib_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_openlib.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/worldcat")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def datasets_worldcat_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_worldcat.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

# @page.get("/datasets/isbn_ranges")
# @allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
# def datasets_isbn_ranges_page():
#     try:
#         stats_data = get_stats_data()
#     except Exception as e:
#         if 'timed out' in str(e):
#             return "Error with datasets page, please try again.", 503
#     return render_template("page/datasets_isbn_ranges.html", header_active="home/datasets", stats_data=stats_data)

@page.get("/copyright")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def copyright_page():
    return render_template("page/copyright.html", header_active="")

@page.get("/fast_download_no_more")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def fast_download_no_more_page():
    return render_template("page/fast_download_no_more.html", header_active="")

@page.get("/fast_download_not_member")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def fast_download_not_member_page():
    return render_template("page/fast_download_not_member.html", header_active="")

@page.get("/torrents")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def torrents_page():
    torrents_data = get_torrents_data()

    with mariapersist_engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT * FROM mariapersist_torrent_scrapes_histogram WHERE day > DATE_FORMAT(NOW() - INTERVAL 60 DAY, "%Y-%m-%d") ORDER BY day, seeder_group LIMIT 500')
        histogram = cursor.fetchall()

        return render_template(
            "page/torrents.html",
            header_active="home/torrents",
            torrents_data=torrents_data,
            histogram=histogram,
            detailview=False,
        )

@page.get("/torrents/<string:group>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def torrents_group_page(group):
    torrents_data = get_torrents_data()

    group_found = False
    for top_level in torrents_data['small_file_dicts_grouped'].keys():
        if group in torrents_data['small_file_dicts_grouped'][top_level]:
            torrents_data = {
                **torrents_data,
                'small_file_dicts_grouped': { top_level: { group: torrents_data['small_file_dicts_grouped'][top_level][group] } }
            }
            group_found = True
            break
    if not group_found:
        return "", 404

    return render_template(
        "page/torrents.html",
        header_active="home/torrents",
        torrents_data=torrents_data,
        detailview=True,
    )

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
    "cover_url_guess": ("after", ["Anna's Archive best guess of the cover URL, based on the MD5."]),
    "removed": ("after", ["Whether the file has been removed from Z-Library. We typically don't know the precise reason."]),
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

def zlib_cover_url_guess(md5):
    # return f"https://static.1lib.sk/covers/books/{md5[0:2]}/{md5[2:4]}/{md5[4:6]}/{md5}.jpg"
    return f""

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
        zlib_book_dict['cover_url_guess'] = zlib_cover_url_guess(zlib_book_dict['md5_reported'])
        zlib_book_dict['added_date_unified'] = { "zlib_source": zlib_book_dict['date_added'] }
        zlib_add_edition_varia_normalized(zlib_book_dict)

        allthethings.utils.init_identifiers_and_classification_unified(zlib_book_dict)
        allthethings.utils.add_identifier_unified(zlib_book_dict, 'zlib', zlib_book_dict['zlibrary_id'])
        if zlib_book_dict['md5'] is not None:
            allthethings.utils.add_identifier_unified(zlib_book_dict, 'md5', zlib_book_dict['md5'])
        if zlib_book_dict['md5_reported'] is not None:
            allthethings.utils.add_identifier_unified(zlib_book_dict, 'md5', zlib_book_dict['md5_reported'])
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
        cursor.execute(f'SELECT annas_archive_meta__aacid__zlib3_records.aacid AS record_aacid, annas_archive_meta__aacid__zlib3_records.metadata AS record_metadata, annas_archive_meta__aacid__zlib3_files.aacid AS file_aacid, annas_archive_meta__aacid__zlib3_files.data_folder AS file_data_folder, annas_archive_meta__aacid__zlib3_files.metadata AS file_metadata, annas_archive_meta__aacid__zlib3_records.primary_id AS primary_id FROM annas_archive_meta__aacid__zlib3_records LEFT JOIN annas_archive_meta__aacid__zlib3_files USING (primary_id) WHERE {aac_key} IN %(values)s ORDER BY record_aacid', { "values": [str(value) for value in values] })
        aac_zlib3_books_by_primary_id = collections.defaultdict(dict)
        # Merge different iterations of books, so even when a book gets "missing":1 later, we still use old
        # metadata where available (note: depends on `ORDER BY record_aacid` above).
        for row in cursor.fetchall():
            aac_zlib3_books_by_primary_id[row['primary_id']] = {
                **aac_zlib3_books_by_primary_id[row['primary_id']],
                **row,
                'record_metadata': {
                    **(aac_zlib3_books_by_primary_id[row['primary_id']].get('record_metadata') or {}),
                    **orjson.loads(row['record_metadata']),
                },
            }
        aac_zlib3_books = list(aac_zlib3_books_by_primary_id.values())

    except Exception as err:
        print(f"Error in get_aac_zlib3_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    aac_zlib3_book_dicts = []
    for zlib_book in aac_zlib3_books:
        aac_zlib3_book_dict = zlib_book['record_metadata']
        if zlib_book['file_metadata'] is not None:
            file_metadata = orjson.loads(zlib_book['file_metadata'])
            aac_zlib3_book_dict['md5'] = file_metadata['md5']
            if 'filesize' in file_metadata:
                aac_zlib3_book_dict['filesize'] = file_metadata['filesize']
        else:
            aac_zlib3_book_dict['md5'] = None
        aac_zlib3_book_dict['record_aacid'] = zlib_book['record_aacid']
        aac_zlib3_book_dict['file_aacid'] = zlib_book['file_aacid']
        aac_zlib3_book_dict['file_data_folder'] = zlib_book['file_data_folder']
        if 'description' not in aac_zlib3_book_dict:
            print(f'WARNING WARNING! missing description in aac_zlib3_book_dict: {aac_zlib3_book_dict=} {zlib_book=}')
            print('------------------')
        aac_zlib3_book_dict['stripped_description'] = strip_description(aac_zlib3_book_dict['description'])
        aac_zlib3_book_dict['language_codes'] = get_bcp47_lang_codes(aac_zlib3_book_dict['language'] or '')
        aac_zlib3_book_dict['cover_url_guess'] = zlib_cover_url_guess(aac_zlib3_book_dict['md5_reported'])
        aac_zlib3_book_dict['added_date_unified'] = { "zlib_source": aac_zlib3_book_dict['date_added'] }
        zlib_add_edition_varia_normalized(aac_zlib3_book_dict)

        allthethings.utils.init_identifiers_and_classification_unified(aac_zlib3_book_dict)
        allthethings.utils.add_identifier_unified(aac_zlib3_book_dict, 'zlib', aac_zlib3_book_dict['zlibrary_id'])
        if aac_zlib3_book_dict['md5'] is not None:
            allthethings.utils.add_identifier_unified(aac_zlib3_book_dict, 'md5', aac_zlib3_book_dict['md5'])
        if aac_zlib3_book_dict['md5_reported'] is not None:
            allthethings.utils.add_identifier_unified(aac_zlib3_book_dict, 'md5', aac_zlib3_book_dict['md5_reported'])
        allthethings.utils.add_isbns_unified(aac_zlib3_book_dict, aac_zlib3_book_dict['isbns'])

        aac_zlib3_book_dicts.append(add_comments_to_dict(aac_zlib3_book_dict, zlib_book_dict_comments))
    return aac_zlib3_book_dicts

@page.get("/db/zlib/<int:zlib_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def zlib_book_json(zlib_id):
    with Session(engine) as session:
        zlib_book_dicts = get_zlib_book_dicts(session, "zlibrary_id", [zlib_id])
        if len(zlib_book_dicts) == 0:
            return "{}", 404
        return nice_json(zlib_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/aac_zlib3/<int:zlib_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
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
    ia_entries2 = []
    try:
        base_query = select(AaIa202306Metadata, AaIa202306Files, Ia2AcsmpdfFiles).join(AaIa202306Files, AaIa202306Files.ia_id == AaIa202306Metadata.ia_id, isouter=True).join(Ia2AcsmpdfFiles, Ia2AcsmpdfFiles.primary_id == AaIa202306Metadata.ia_id, isouter=True)
        base_query2 = select(Ia2Records, AaIa202306Files, Ia2AcsmpdfFiles).join(AaIa202306Files, AaIa202306Files.ia_id == Ia2Records.primary_id, isouter=True).join(Ia2AcsmpdfFiles, Ia2AcsmpdfFiles.primary_id == Ia2Records.primary_id, isouter=True)
        if key.lower() in ['md5']:
            # TODO: we should also consider matching on libgen_md5, but we used to do that before and it had bad SQL performance,
            # when combined in a single query, so we'd have to split it up.
            ia_entries = list(session.execute(
                base_query.where(AaIa202306Files.md5.in_(values))
            ).unique().all()) + list(session.execute(
                base_query.where(Ia2AcsmpdfFiles.md5.in_(values))
            ).unique().all())
            ia_entries2 = list(session.execute(
                base_query2.where(AaIa202306Files.md5.in_(values))
            ).unique().all()) + list(session.execute(
                base_query2.where(Ia2AcsmpdfFiles.md5.in_(values))
            ).unique().all())
        else:
            ia_entries = session.execute(
                base_query.where(getattr(AaIa202306Metadata, key).in_(values))
            ).unique().all()
            ia_entries2 = session.execute(
                base_query2.where(getattr(Ia2Records, key.replace('ia_id', 'primary_id')).in_(values))
            ).unique().all()
    except Exception as err:
        print(f"Error in get_ia_record_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    ia_record_dicts = []
    # Prioritize ia_entries2 first, because their records are newer.
    for ia_record, ia_file, ia2_acsmpdf_file in (ia_entries2 + ia_entries):
        ia_record_dict = ia_record.to_dict()
        if 'primary_id' in ia_record_dict:
            # Convert from AAC.
            metadata = orjson.loads(ia_record_dict["metadata"])

            ia_record_dict = {
                "ia_id": metadata["ia_id"],
                # "has_thumb" # We'd need to look at both ia_entries2 and ia_entries to get this, but not worth it.
                "libgen_md5": None,
                "json": metadata['metadata_json'],
            }

            for external_id in extract_list_from_ia_json_field(ia_record_dict, 'external-identifier'):
                if 'urn:libgen:' in external_id:
                    ia_record_dict['libgen_md5'] = external_id.split('/')[-1]
                    break
        else:
            ia_record_dict = {
                "ia_id": ia_record_dict["ia_id"],
                # "has_thumb": ia_record_dict["has_thumb"],
                "libgen_md5": ia_record_dict["libgen_md5"],
                "json": orjson.loads(ia_record_dict["json"]),
            }

        # TODO: When querying by ia_id we can match multiple files. For now we just pick the first one.
        if ia_record_dict['ia_id'] in seen_ia_ids:
            continue
        seen_ia_ids.add(ia_record_dict['ia_id'])

        ia_record_dict['aa_ia_file'] = None
        added_date_unified_file = {}
        if ia_record_dict['libgen_md5'] is None: # If there's a Libgen MD5, then we do NOT serve our IA file.
            if ia_file is not None:
                ia_record_dict['aa_ia_file'] = ia_file.to_dict()
                ia_record_dict['aa_ia_file']['extension'] = 'pdf'
                added_date_unified_file = { "ia_file_scrape": "2023-06-28" }
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
                added_date_unified_file = { "ia_file_scrape": datetime.datetime.strptime(ia2_acsmpdf_file_dict['aacid'].split('__')[2], "%Y%m%dT%H%M%SZ").isoformat() }

        ia_record_dict['aa_ia_derived'] = {}
        ia_record_dict['aa_ia_derived']['printdisabled_only'] = 'inlibrary' not in ((ia_record_dict['json'].get('metadata') or {}).get('collection') or [])
        ia_record_dict['aa_ia_derived']['original_filename'] = (ia_record_dict['ia_id'] + '.pdf') if ia_record_dict['aa_ia_file'] is not None else None
        ia_record_dict['aa_ia_derived']['cover_url'] = f"https://archive.org/download/{ia_record_dict['ia_id']}/__ia_thumb.jpg"
        ia_record_dict['aa_ia_derived']['title'] = (' '.join(extract_list_from_ia_json_field(ia_record_dict, 'title'))).replace(' : ', ': ')
        ia_record_dict['aa_ia_derived']['author'] = ('; '.join(extract_list_from_ia_json_field(ia_record_dict, 'creator') + extract_list_from_ia_json_field(ia_record_dict, 'associated-names'))).replace(' : ', ': ')
        ia_record_dict['aa_ia_derived']['publisher'] = ('; '.join(extract_list_from_ia_json_field(ia_record_dict, 'publisher'))).replace(' : ', ': ')
        ia_record_dict['aa_ia_derived']['combined_comments'] = extract_list_from_ia_json_field(ia_record_dict, 'notes') + extract_list_from_ia_json_field(ia_record_dict, 'comment') + extract_list_from_ia_json_field(ia_record_dict, 'curation')
        ia_record_dict['aa_ia_derived']['subjects'] = '\n\n'.join(extract_list_from_ia_json_field(ia_record_dict, 'subject') + extract_list_from_ia_json_field(ia_record_dict, 'level_subject'))
        ia_record_dict['aa_ia_derived']['stripped_description_and_references'] = strip_description('\n\n'.join(extract_list_from_ia_json_field(ia_record_dict, 'description') + extract_list_from_ia_json_field(ia_record_dict, 'references')))
        ia_record_dict['aa_ia_derived']['language_codes'] = combine_bcp47_lang_codes([get_bcp47_lang_codes(lang) for lang in (extract_list_from_ia_json_field(ia_record_dict, 'language') + extract_list_from_ia_json_field(ia_record_dict, 'ocr_detected_lang'))])
        ia_record_dict['aa_ia_derived']['all_dates'] = list(dict.fromkeys(extract_list_from_ia_json_field(ia_record_dict, 'year') + extract_list_from_ia_json_field(ia_record_dict, 'date') + extract_list_from_ia_json_field(ia_record_dict, 'range')))
        ia_record_dict['aa_ia_derived']['longest_date_field'] = max([''] + ia_record_dict['aa_ia_derived']['all_dates'])
        ia_record_dict['aa_ia_derived']['year'] = ''
        for date in ([ia_record_dict['aa_ia_derived']['longest_date_field']] + ia_record_dict['aa_ia_derived']['all_dates']):
            potential_year = re.search(r"(\d\d\d\d)", date)
            if potential_year is not None:
                ia_record_dict['aa_ia_derived']['year'] = potential_year[0]
                break

        ia_record_dict['aa_ia_derived']['added_date_unified'] = {}
        publicdate = extract_list_from_ia_json_field(ia_record_dict, 'publicdate')
        if len(publicdate) > 0:
            if publicdate[0].encode('ascii', 'ignore').decode() != publicdate[0]:
                print(f"Warning: {publicdate[0]=} is not ASCII; skipping!")
            else:
                ia_record_dict['aa_ia_derived']['added_date_unified'] = { **added_date_unified_file, "ia_source": datetime.datetime.strptime(publicdate[0], "%Y-%m-%d %H:%M:%S").isoformat() }

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
        if ia_record_dict['libgen_md5'] is not None:
            allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'md5', ia_record_dict['libgen_md5'])
        if ia_record_dict['aa_ia_file'] is not None:
            allthethings.utils.add_identifier_unified(ia_record_dict['aa_ia_derived'], 'md5', ia_record_dict['aa_ia_file']['md5'])
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
            # "has_thumb": ("after", "Whether Anna's Archive has stored a thumbnail (scraped from __ia_thumb.jpg)."),
            "json": ("before", "The original metadata JSON, scraped from https://archive.org/metadata/<ia_id>.",
                               "We did strip out the full file list, since it's a bit long, and replaced it with a shorter `aa_shorter_files`."),
            "aa_ia_file": ("before", "File metadata, if we have it."),
            "aa_ia_derived": ("before", "Derived metadata."),
        }
        ia_record_dicts.append(add_comments_to_dict(ia_record_dict, ia_record_dict_comments))

    return ia_record_dicts

@page.get("/db/ia/<string:ia_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
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
            ol_works_by_key = {ol_work.ol_key: ol_work for ol_work in conn.execute(select(OlBase).where(OlBase.ol_key.in_(list(dict.fromkeys(works_ol_keys))))).all()}
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
            author_keys = list(dict.fromkeys(author_keys))
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
                    if author.type == '/type/delete':
                        # Deleted, not sure how to handle this, skipping.
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

            created_normalized = ''
            if len(created_normalized) == 0 and 'created' in ol_book_dict['edition']['json']:
                created_normalized = extract_ol_str_field(ol_book_dict['edition']['json']['created']).strip()
            if len(created_normalized) == 0 and ol_book_dict['work'] and 'created' in ol_book_dict['work']['json']:
                created_normalized = extract_ol_str_field(ol_book_dict['work']['json']['created']).strip()
            ol_book_dict['added_date_unified'] = {}
            if len(created_normalized) > 0:
                if '.' in created_normalized:
                    ol_book_dict['added_date_unified'] = { 'ol_source': datetime.datetime.strptime(created_normalized, '%Y-%m-%dT%H:%M:%S.%f').isoformat() }
                else:
                    ol_book_dict['added_date_unified'] = { 'ol_source': datetime.datetime.strptime(created_normalized, '%Y-%m-%dT%H:%M:%S').isoformat() }

            # {% for source_record in ol_book_dict.json.source_records %}
            #   <div class="flex odd:bg-black/5 hover:bg-black/64">
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

def get_ol_book_dicts_by_isbn13(session, isbn13s):
    if len(isbn13s) == 0:
        return {}
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT ol_key, isbn FROM ol_isbn13 WHERE isbn IN %(isbn13s)s', { "isbn13s": isbn13s })
        rows = cursor.fetchall()
        if len(rows) == 0:
            return {}
        isbn13s_by_ol_edition = collections.defaultdict(list)
        for row in rows:
            if row['ol_key'].startswith('/books/OL') and row['ol_key'].endswith('M'):
                ol_edition = row['ol_key'][len('/books/'):]
                isbn13s_by_ol_edition[ol_edition].append(row['isbn'])
        ol_book_dicts = get_ol_book_dicts(session, 'ol_edition', list(isbn13s_by_ol_edition.keys()))
        retval = collections.defaultdict(list)
        for ol_book_dict in ol_book_dicts:
            for isbn13 in isbn13s_by_ol_edition[ol_book_dict['ol_edition']]: 
                retval[isbn13].append(ol_book_dict)
        return dict(retval)

@page.get("/db/ol/<string:ol_edition>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def ol_book_json(ol_edition):
    with Session(engine) as session:
        ol_book_dicts = get_ol_book_dicts(session, "ol_edition", [ol_edition])
        if len(ol_book_dicts) == 0:
            return "{}", 404
        return nice_json(ol_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

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

        lgrs_book_dict['added_date_unified'] = {}
        if lgrs_book_dict['timeadded'] != '0000-00-00 00:00:00':
            if not isinstance(lgrs_book_dict['timeadded'], datetime.datetime):
                raise Exception(f"Unexpected {lgrs_book_dict['timeadded']=} for {lgrs_book_dict=}")
            lgrs_book_dict['added_date_unified'] = { 'lgrsnf_source': lgrs_book_dict['timeadded'].isoformat() }

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
        allthethings.utils.add_identifier_unified(lgrs_book_dict, 'md5', lgrs_book_dict['md5'])
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
        
        lgrs_book_dict['added_date_unified'] = {}
        if lgrs_book_dict['timeadded'] != '0000-00-00 00:00:00':
            if not isinstance(lgrs_book_dict['timeadded'], datetime.datetime):
                raise Exception(f"Unexpected {lgrs_book_dict['timeadded']=} for {lgrs_book_dict=}")
            lgrs_book_dict['added_date_unified'] = { 'lgrsfic_source': lgrs_book_dict['timeadded'].isoformat() }

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
        allthethings.utils.add_identifier_unified(lgrs_book_dict, 'md5', lgrs_book_dict['md5'])
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def lgrsnf_book_json_redirect(lgrsnf_book_id):
    return redirect(f"/db/lgrsnf/{lgrsnf_book_id}.json", code=301)
@page.get("/db/lgrs/fic/<int:lgrsfic_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def lgrsfic_book_json_redirect(lgrsfic_book_id):
    return redirect(f"/db/lgrsfic/{lgrsfic_book_id}.json", code=301)

@page.get("/db/lgrsnf/<int:lgrsnf_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def lgrsnf_book_json(lgrsnf_book_id):
    with Session(engine) as session:
        lgrs_book_dicts = get_lgrsnf_book_dicts(session, "ID", [lgrsnf_book_id])
        if len(lgrs_book_dicts) == 0:
            return "{}", 404
        return nice_json(lgrs_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}
@page.get("/db/lgrsfic/<int:lgrsfic_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
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
        allthethings.utils.add_identifier_unified(lgli_file_dict, 'md5', lgli_file_dict['md5'])
        lgli_file_dict['scimag_archive_path_decoded'] = urllib.parse.unquote(lgli_file_dict['scimag_archive_path'].replace('\\', '/'))
        potential_doi_scimag_archive_path = lgli_file_dict['scimag_archive_path_decoded']
        if potential_doi_scimag_archive_path.endswith('.pdf'):
            potential_doi_scimag_archive_path = potential_doi_scimag_archive_path[:-len('.pdf')]
        potential_doi_scimag_archive_path = normalize_doi(potential_doi_scimag_archive_path)
        if potential_doi_scimag_archive_path != '':
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'doi', potential_doi_scimag_archive_path)

        lgli_file_dict['added_date_unified'] = {}
        if lgli_file_dict['time_added'] != '0000-00-00 00:00:00':
            if not isinstance(lgli_file_dict['time_added'], datetime.datetime):
                raise Exception(f"Unexpected {lgli_file_dict['time_added']=} for {lgli_file_dict=}")
            lgli_file_dict['added_date_unified'] = { 'lgli_source': lgli_file_dict['time_added'].isoformat() }

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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def lgli_file_json(lgli_file_id):
    return redirect(f"/db/lgli/{lgli_file_id}.json", code=301)

@page.get("/db/lgli/<int:lgli_file_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def lgli_json(lgli_file_id):
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
            "added_date_unified": { "isbndb_scrape": "2022-09-01" },
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
            isbndb_dict['edition_varia_normalized'] = ", ".join(list(dict.fromkeys([item for item in [
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
            isbndb_dict['added_date_unified'] = { "isbndb_scrape": "2022-09-01" }

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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
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
                oclc_dict["aa_oclc_derived"]["isbn_multiple"] += (aac_metadata['record'].get('isbns') or [])
                oclc_dict["aa_oclc_derived"]["isbn_multiple"] += (rft.get('rft.isbn') or [])

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
                oclc_dict["aa_oclc_derived"]["isbn_multiple"] += (rft.get('rft.isbn') or [])
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

        oclc_dict['aa_oclc_derived']["added_date_unified"] = { "oclc_scrape": "2023-10-01" }

        # TODO:
        # * cover_url
        # * comments
        # * other/related OCLC numbers
        # * redirects
        # * Genre for fiction detection
        # * Full audit of all fields
        # * dict comments

        oclc_dicts.append(oclc_dict)
    return oclc_dicts

def get_oclc_id_by_isbn13(session, isbn13s):
    if len(isbn13s) == 0:
        return {}
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        # TODO: Replace with aarecords_isbn13
        cursor.execute('SELECT isbn13, oclc_id FROM isbn13_oclc WHERE isbn13 IN %(isbn13s)s', { "isbn13s": isbn13s })
        rows = cursor.fetchall()
        if len(rows) == 0:
            return {}
        oclc_ids_by_isbn13 = collections.defaultdict(list)
        for row in rows:
            oclc_ids_by_isbn13[row['isbn13']].append(row['oclc_id'])
        return dict(oclc_ids_by_isbn13)

def get_oclc_dicts_by_isbn13(session, isbn13s):
    if len(isbn13s) == 0:
        return {}
    with engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        # TODO: Replace with aarecords_isbn13
        cursor.execute('SELECT isbn13, oclc_id FROM isbn13_oclc WHERE isbn13 IN %(isbn13s)s', { "isbn13s": isbn13s })
        rows = cursor.fetchall()
        if len(rows) == 0:
            return {}
        isbn13s_by_oclc_id = collections.defaultdict(list)
        for row in rows:
            isbn13s_by_oclc_id[row['oclc_id']].append(row['isbn13'])
        oclc_dicts = get_oclc_dicts(session, 'oclc', list(isbn13s_by_oclc_id.keys()))
        retval = collections.defaultdict(list)
        for oclc_dict in oclc_dicts:
            for isbn13 in isbn13s_by_oclc_id[oclc_dict['oclc_id']]:
                retval[isbn13].append(oclc_dict)
        return dict(retval)

@page.get("/db/oclc/<path:oclc>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def oclc_oclc_json(oclc):
    with Session(engine) as session:
        oclc_dicts = get_oclc_dicts(session, 'oclc', [oclc])
        if len(oclc_dicts) == 0:
            return "{}", 404
        return nice_json(oclc_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def get_duxiu_dicts(session, key, values):
    if len(values) == 0:
        return []
    if key not in ['duxiu_ssid', 'cadal_ssno', 'md5']:
        raise Exception(f"Unexpected 'key' in get_duxiu_dicts: '{key}'")

    primary_id_prefix = f"{key}_"

    aac_records_by_primary_id = collections.defaultdict(list)
    try:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        if key == 'md5':
            cursor.execute(f'SELECT annas_archive_meta__aacid__duxiu_records.aacid AS aacid, annas_archive_meta__aacid__duxiu_records.metadata AS metadata, annas_archive_meta__aacid__duxiu_files.primary_id AS primary_id, annas_archive_meta__aacid__duxiu_files.data_folder AS generated_file_data_folder, annas_archive_meta__aacid__duxiu_files.aacid AS generated_file_aacid, annas_archive_meta__aacid__duxiu_files.metadata AS generated_file_metadata FROM annas_archive_meta__aacid__duxiu_records JOIN annas_archive_meta__aacid__duxiu_files ON (CONCAT("md5_", annas_archive_meta__aacid__duxiu_files.md5) = annas_archive_meta__aacid__duxiu_records.primary_id) WHERE annas_archive_meta__aacid__duxiu_files.primary_id IN %(values)s', { "values": values })
        else:
            cursor.execute(f'SELECT * FROM annas_archive_meta__aacid__duxiu_records WHERE primary_id IN %(values)s', { "values": [f'{primary_id_prefix}{value}' for value in values] })
    except Exception as err:
        print(f"Error in get_duxiu_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    for aac_record in cursor.fetchall():
        new_aac_record = {
            **aac_record,
            "metadata": orjson.loads(aac_record['metadata']),
        }
        if "generated_file_metadata" in aac_record:
            new_aac_record["generated_file_metadata"] = orjson.loads(new_aac_record["generated_file_metadata"])
        if "serialized_files" in new_aac_record["metadata"]["record"]:
            for serialized_file in new_aac_record["metadata"]["record"]["serialized_files"]:
                serialized_file['aa_derived_deserialized_gbk'] = ''
                try:
                    serialized_file['aa_derived_deserialized_gbk'] = base64.b64decode(serialized_file['data_base64']).decode('gbk')
                except:
                    pass

            new_aac_record["metadata"]["record"]["aa_derived_ini_values"] = {}
            for serialized_file in new_aac_record['metadata']['record']['serialized_files']:
                if 'bkmk.txt' in serialized_file['filename'].lower():
                    continue
                if 'downpdg.log' in serialized_file['filename'].lower():
                    continue
                for line in serialized_file['aa_derived_deserialized_gbk'].split('\n'):
                    line = line.strip()
                    if '=' in line:
                        line_key, line_value = line.split('=', 1)
                        if line_value.strip() != '':
                            if line_key not in new_aac_record["metadata"]["record"]["aa_derived_ini_values"]:
                                new_aac_record["metadata"]["record"]["aa_derived_ini_values"][line_key] = []
                            new_aac_record["metadata"]["record"]["aa_derived_ini_values"][line_key].append({ 
                                "aacid": new_aac_record["aacid"],
                                "filename": serialized_file["filename"], 
                                "key": line_key, 
                                "value": line_value,
                            })

            if 'SS号' in new_aac_record["metadata"]["record"]["aa_derived_ini_values"]:
                new_aac_record["metadata"]["record"]["aa_derived_duxiu_ssid"] = new_aac_record["metadata"]["record"]["aa_derived_ini_values"]["SS号"][0]["value"]
            else:
                ssid_filename_match = re.search(r'(?:^|\D)(\d{8})(?:\D|$)', new_aac_record['metadata']['record']['filename_decoded'])
                if ssid_filename_match is not None:
                    # TODO: Only duxiu_ssid here? Or also CADAL?
                    new_aac_record["metadata"]["record"]["aa_derived_duxiu_ssid"] = ssid_filename_match[1]

        aac_records_by_primary_id[aac_record['primary_id']].append(new_aac_record)

    aa_derived_duxiu_ssids_to_primary_id = {}
    for primary_id, aac_records in aac_records_by_primary_id.items():
        if "aa_derived_duxiu_ssid" in new_aac_record["metadata"]["record"]:
            aa_derived_duxiu_ssids_to_primary_id[new_aac_record["metadata"]["record"]["aa_derived_duxiu_ssid"]] = primary_id

    if len(aa_derived_duxiu_ssids_to_primary_id) > 0:
        for record in get_duxiu_dicts(session, 'duxiu_ssid', list(aa_derived_duxiu_ssids_to_primary_id.keys())):
            primary_id = aa_derived_duxiu_ssids_to_primary_id[record['duxiu_ssid']]
            for aac_record in record['aac_records']:
                # NOTE: It's important that we append these aac_records at the end, since we select the "best" records
                # first, and any data we get directly from the fields associated with the file itself should take precedence.
                aac_records_by_primary_id[primary_id].append(aac_record)

    duxiu_dicts = []
    for primary_id, aac_records in aac_records_by_primary_id.items():
        if any([record['metadata']['type'] == 'dx_20240122__books' for record in aac_records]) and not any([record['metadata']['type'] == '512w_final_csv' for record in aac_records]):
            # 512w_final_csv has a bunch of incorrect records from dx_20240122__books deleted.
            continue

        duxiu_dict = {}

        if key == 'duxiu_ssid':
            duxiu_dict['duxiu_ssid'] = primary_id.replace('duxiu_ssid_', '')
        elif key == 'cadal_ssno':
            duxiu_dict['cadal_ssno'] = primary_id.replace('cadal_ssno_', '')
        elif key == 'md5':
            duxiu_dict['md5'] = primary_id
        else:
            raise Exception(f"Unexpected 'key' in get_duxiu_dicts: '{key}'")
        duxiu_dict['duxiu_file'] = None
        duxiu_dict['aa_duxiu_derived'] = {}
        duxiu_dict['aa_duxiu_derived']['source_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['title_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['author_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['publisher_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['year_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['series_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['pages_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['duxiu_ssid_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['cadal_ssno_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['isbn_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['issn_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['ean13_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['dxid_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['md5_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['filesize_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['miaochuan_links_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['filepath_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['ini_values_multiple'] = []
        duxiu_dict['aa_duxiu_derived']['description_cumulative'] = []
        duxiu_dict['aa_duxiu_derived']['comments_cumulative'] = []
        duxiu_dict['aa_duxiu_derived']['debug_language_codes'] = {}
        duxiu_dict['aa_duxiu_derived']['language_codes'] = []
        duxiu_dict['aa_duxiu_derived']['added_date_unified'] = {}
        duxiu_dict['aac_records'] = aac_records

        if key == 'duxiu_ssid':
            duxiu_dict['aa_duxiu_derived']['duxiu_ssid_multiple'].append(duxiu_dict['duxiu_ssid'])
        elif key == 'cadal_ssno':
            duxiu_dict['aa_duxiu_derived']['cadal_ssno_multiple'].append(duxiu_dict['cadal_ssno'])
        elif key == 'md5':
            duxiu_dict['aa_duxiu_derived']['md5_multiple'].append(duxiu_dict['md5'])

        for aac_record in aac_records:
            duxiu_dict['aa_duxiu_derived']['added_date_unified']['duxiu_meta_scrape'] = max(duxiu_dict['aa_duxiu_derived']['added_date_unified'].get('duxiu_meta_scrape') or '', datetime.datetime.strptime(aac_record['aacid'].split('__')[2], "%Y%m%dT%H%M%SZ").isoformat())

            if aac_record['metadata']['type'] == 'dx_20240122__books':
                if len(aac_record['metadata']['record'].get('source') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['source_multiple'].append(['dx_20240122__books', aac_record['metadata']['record']['source']])
            elif aac_record['metadata']['type'] in ['512w_final_csv', 'DX_corrections240209_csv']:
                if aac_record['metadata']['type'] == '512w_final_csv' and any([record['metadata']['type'] == 'DX_corrections240209_csv' for record in aac_records]):
                    # Skip if there is also a correction.
                    pass

                if len(aac_record['metadata']['record'].get('title') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['title_multiple'].append(aac_record['metadata']['record']['title'])
                if len(aac_record['metadata']['record'].get('author') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['author_multiple'].append(aac_record['metadata']['record']['author'])
                if len(aac_record['metadata']['record'].get('publisher') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['publisher_multiple'].append(aac_record['metadata']['record']['publisher'])
                if len(aac_record['metadata']['record'].get('year') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['year_multiple'].append(aac_record['metadata']['record']['year'])
                if len(aac_record['metadata']['record'].get('pages') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['pages_multiple'].append(aac_record['metadata']['record']['pages'])
                if len(aac_record['metadata']['record'].get('dx_id') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['dxid_multiple'].append(aac_record['metadata']['record']['dx_id'])

                if len(aac_record['metadata']['record'].get('isbn') or '') > 0:
                    identifiers = []
                    if aac_record['metadata']['record']['isbn_type'].startswith('multiple('):
                        identifier_values = aac_record['metadata']['record']['isbn'].split('_')
                        for index, identifier_type in enumerate(aac_record['metadata']['record']['isbn_type'][len('multiple('):-len(')')].split(',')):
                            identifiers.append({ 'type': identifier_type, 'value': identifier_values[index] })
                    elif aac_record['metadata']['record']['isbn_type'] != 'none':
                        identifiers.append({ 'type': aac_record['metadata']['record']['isbn_type'], 'value': aac_record['metadata']['record']['isbn'] })

                    for identifier in identifiers:
                        if identifier['type'] in ['ISBN-13', 'ISBN-10', 'CSBN']:
                            duxiu_dict['aa_duxiu_derived']['isbn_multiple'].append(identifier['value'])
                        elif identifier['type'] in ['ISSN-13', 'ISSN-8']:
                            duxiu_dict['aa_duxiu_derived']['issn_multiple'].append(identifier['value'])
                        elif identifier['type'] == 'EAN-13':
                            duxiu_dict['aa_duxiu_derived']['ean13_multiple'].append(identifier['value'])
                        elif identifier['type'] in ['unknown', 'unknow']:
                            pass
                        else:
                            raise Exception(f"Unknown type of duxiu 512w_final_csv isbn_type {identifier_type=}")
            elif aac_record['metadata']['type'] == 'dx_20240122__remote_files':
                if len(aac_record['metadata']['record'].get('source') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['source_multiple'].append(['dx_20240122__remote_files', aac_record['metadata']['record']['source']])
                if len(aac_record['metadata']['record'].get('dx_id') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['dxid_multiple'].append(aac_record['metadata']['record']['dx_id'])
                if len(aac_record['metadata']['record'].get('md5') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['md5_multiple'].append(aac_record['metadata']['record']['md5'])
                if (aac_record['metadata']['record'].get('size') or 0) > 0:
                    duxiu_dict['aa_duxiu_derived']['filesize_multiple'].append(aac_record['metadata']['record']['size'])

                filepath_components = []
                if len(aac_record['metadata']['record'].get('path') or '') > 0:
                    filepath_components.append(aac_record['metadata']['record']['path'])
                    if not aac_record['metadata']['record']['path'].endswith('/'):
                        filepath_components.append('/')
                if len(aac_record['metadata']['record'].get('filename') or '') > 0:
                    filepath_components.append(aac_record['metadata']['record']['filename'])
                if len(filepath_components) > 0:
                    duxiu_dict['aa_duxiu_derived']['filepath_multiple'].append(''.join(filepath_components))

                if (len(aac_record['metadata']['record'].get('md5') or '') > 0) and ((aac_record['metadata']['record'].get('size') or 0) > 0) and (len(aac_record['metadata']['record'].get('filename') or '') > 0):
                    miaochuan_link_parts = []
                    miaochuan_link_parts.append(aac_record['metadata']['record']['md5'])
                    if len(aac_record['metadata']['record'].get('header_md5') or '') > 0:
                        miaochuan_link_parts.append(aac_record['metadata']['record']['header_md5'])
                    miaochuan_link_parts.append(str(aac_record['metadata']['record']['size']))
                    miaochuan_link_parts.append(aac_record['metadata']['record']['filename'])
                    duxiu_dict['aa_duxiu_derived']['miaochuan_links_multiple'].append('#'.join(miaochuan_link_parts))
            elif aac_record['metadata']['type'] == 'dx_toc_db__dx_toc':
                # TODO: Better parsing; maintain tree structure.
                toc_xml = (aac_record['metadata']['record'].get('toc_xml') or '')
                toc_matches = re.findall(r'id="([^"]+)" Caption="([^"]+)" PageNumber="([^"]+)"', toc_xml)
                if len(toc_matches) > 0:
                    duxiu_dict['aa_duxiu_derived']['description_cumulative'].append('\n'.join([f"{match[2]} ({match[0]}): {match[1]}" for match in toc_matches]))
            elif aac_record['metadata']['type'] == 'cadal_table__books_detail':
                if len(aac_record['metadata']['record'].get('title') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['title_multiple'].append(aac_record['metadata']['record']['title'])
                if len(aac_record['metadata']['record'].get('creator') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['author_multiple'].append(aac_record['metadata']['record']['creator'])
                if len(aac_record['metadata']['record'].get('publisher') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['publisher_multiple'].append(aac_record['metadata']['record']['publisher'])
                if len(aac_record['metadata']['record'].get('isbn') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['isbn_multiple'].append(aac_record['metadata']['record']['isbn'])
                if len(aac_record['metadata']['record'].get('date') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['year_multiple'].append(aac_record['metadata']['record']['date'])
                if len(aac_record['metadata']['record'].get('page_num') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['pages_multiple'].append(aac_record['metadata']['record']['page_num'])
                if len(aac_record['metadata']['record'].get('common_title') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['common_title'])
                if len(aac_record['metadata']['record'].get('topic') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['topic'])
                if len(aac_record['metadata']['record'].get('tags') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['tags'])
                if len(aac_record['metadata']['record'].get('period') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['period'])
                if len(aac_record['metadata']['record'].get('period_year') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['period_year'])
                if len(aac_record['metadata']['record'].get('publication_place') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['publication_place'])
                if len(aac_record['metadata']['record'].get('common_title') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['common_title'])
                if len(aac_record['metadata']['record'].get('type') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['type'])
            elif aac_record['metadata']['type'] == 'cadal_table__books_solr':
                if len(aac_record['metadata']['record'].get('Title') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['title_multiple'].append(aac_record['metadata']['record']['Title'])
                if len(aac_record['metadata']['record'].get('CreateDate') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['year_multiple'].append(aac_record['metadata']['record']['CreateDate'])
                if len(aac_record['metadata']['record'].get('ISBN') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['isbn_multiple'].append(aac_record['metadata']['record']['ISBN'])
                if len(aac_record['metadata']['record'].get('Creator') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['author_multiple'].append(aac_record['metadata']['record']['Creator'])
                if len(aac_record['metadata']['record'].get('Publisher') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['publisher_multiple'].append(aac_record['metadata']['record']['Publisher'])
                if len(aac_record['metadata']['record'].get('Page') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['pages_multiple'].append(aac_record['metadata']['record']['Page'])
                if len(aac_record['metadata']['record'].get('Description') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['description_cumulative'].append(aac_record['metadata']['record']['Description'])
                if len(aac_record['metadata']['record'].get('Subject') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Subject'])
                if len(aac_record['metadata']['record'].get('theme') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['theme'])
                if len(aac_record['metadata']['record'].get('label') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['label'])
                if len(aac_record['metadata']['record'].get('HostID') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['HostID'])
                if len(aac_record['metadata']['record'].get('Contributor') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Contributor'])
                if len(aac_record['metadata']['record'].get('Relation') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Relation'])
                if len(aac_record['metadata']['record'].get('Rights') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Rights'])
                if len(aac_record['metadata']['record'].get('Format') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Format'])
                if len(aac_record['metadata']['record'].get('Type') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Type'])
                if len(aac_record['metadata']['record'].get('BookType') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['BookType'])
                if len(aac_record['metadata']['record'].get('Coverage') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(aac_record['metadata']['record']['Coverage'])
            elif aac_record['metadata']['type'] == 'cadal_table__site_journal_items':
                if len(aac_record['metadata']['record'].get('date_year') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['year_multiple'].append(aac_record['metadata']['record']['date_year'])
                # TODO
            elif aac_record['metadata']['type'] == 'cadal_table__sa_newspaper_items':
                if len(aac_record['metadata']['record'].get('date_year') or '') > 0:
                    duxiu_dict['aa_duxiu_derived']['year_multiple'].append(aac_record['metadata']['record']['date_year'])
                # TODO
            elif aac_record['metadata']['type'] == 'cadal_table__books_search':
                pass # TODO
            elif aac_record['metadata']['type'] == 'cadal_table__site_book_collection_items':
                pass # TODO
            elif aac_record['metadata']['type'] == 'cadal_table__sa_collection_items':
                pass # TODO
            elif aac_record['metadata']['type'] == 'cadal_table__books_aggregation':
                pass # TODO
            elif aac_record['metadata']['type'] == 'aa_catalog_files':
                if len(aac_record.get('generated_file_aacid') or '') > 0:
                    duxiu_dict['duxiu_file'] = {
                        "aacid": aac_record['generated_file_aacid'],
                        "data_folder": aac_record['generated_file_data_folder'],
                        "filesize": aac_record['generated_file_metadata']['filesize'],
                        "extension": 'pdf',
                    }
                    # Make sure to prepend these, in case there is another 'aa_catalog_files' entry without a generated_file.
                    duxiu_dict['aa_duxiu_derived']['md5_multiple'] = [aac_record['generated_file_metadata']['md5']] + duxiu_dict['aa_duxiu_derived']['md5_multiple']
                    duxiu_dict['aa_duxiu_derived']['md5_multiple'] = [aac_record['generated_file_metadata']['original_md5']] + duxiu_dict['aa_duxiu_derived']['md5_multiple']
                    duxiu_dict['aa_duxiu_derived']['filesize_multiple'] = [int(aac_record['generated_file_metadata']['filesize'])] + duxiu_dict['aa_duxiu_derived']['filesize_multiple']
                    duxiu_dict['aa_duxiu_derived']['added_date_unified']['duxiu_filegen'] = datetime.datetime.strptime(aac_record['generated_file_aacid'].split('__')[2], "%Y%m%dT%H%M%SZ").isoformat()

                duxiu_dict['aa_duxiu_derived']['source_multiple'].append(['aa_catalog_files'])

                aa_derived_ini_values = aac_record['metadata']['record']['aa_derived_ini_values']
                for aa_derived_ini_values_list in aa_derived_ini_values.values():
                    duxiu_dict['aa_duxiu_derived']['ini_values_multiple'] += aa_derived_ini_values_list
                for ini_value in ((aa_derived_ini_values.get('Title') or []) + (aa_derived_ini_values.get('书名') or [])):
                    duxiu_dict['aa_duxiu_derived']['title_multiple'].append(ini_value['value'])
                for ini_value in ((aa_derived_ini_values.get('Author') or []) + (aa_derived_ini_values.get('作者') or [])):
                    duxiu_dict['aa_duxiu_derived']['author_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('出版社') or []):
                    duxiu_dict['aa_duxiu_derived']['publisher_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('丛书名') or []):
                    duxiu_dict['aa_duxiu_derived']['series_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('出版日期') or []):
                    potential_year = re.search(r"(\d\d\d\d)", ini_value['value'])
                    if potential_year is not None:
                        duxiu_dict['aa_duxiu_derived']['year_multiple'].append(potential_year[0])
                for ini_value in (aa_derived_ini_values.get('页数') or []):
                    duxiu_dict['aa_duxiu_derived']['pages_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('ISBN号') or []):
                    duxiu_dict['aa_duxiu_derived']['isbn_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('DX号') or []):
                    duxiu_dict['aa_duxiu_derived']['dxid_multiple'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('SS号') or []):
                    duxiu_dict['aa_duxiu_derived']['duxiu_ssid_multiple'].append(ini_value['value'])

                for ini_value in (aa_derived_ini_values.get('参考文献格式') or []): # Reference format
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('原书定价') or []): # Original Book Pricing
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('中图法分类号') or []): # CLC Classification Number # TODO: more proper handling than throwing in description
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('主题词') or []): # Keywords
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('Subject') or []):
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])
                for ini_value in (aa_derived_ini_values.get('Keywords') or []):
                    duxiu_dict['aa_duxiu_derived']['comments_cumulative'].append(ini_value['value'])

                miaochuan_link_parts = [
                    aac_record['metadata']['record']['md5'],
                    aac_record['metadata']['record']['header_md5'],
                    str(aac_record['metadata']['record']['filesize']),
                    aac_record['metadata']['record']['filename_decoded'],
                ]
                duxiu_dict['aa_duxiu_derived']['miaochuan_links_multiple'].append('#'.join(miaochuan_link_parts))
                duxiu_dict['aa_duxiu_derived']['filesize_multiple'].append(int(aac_record['metadata']['record']['filesize']))
                duxiu_dict['aa_duxiu_derived']['filepath_multiple'].append(aac_record['metadata']['record']['filename_decoded'])

                if 'aa_derived_duxiu_ssid' in aac_record['metadata']['record']:
                    duxiu_dict['aa_duxiu_derived']['duxiu_ssid_multiple'].append(aac_record['metadata']['record']['aa_derived_duxiu_ssid'])
            else:
                raise Exception(f"Unknown type of duxiu metadata type {aac_record['metadata']['type']=}")

        allthethings.utils.init_identifiers_and_classification_unified(duxiu_dict['aa_duxiu_derived'])
        allthethings.utils.add_isbns_unified(duxiu_dict['aa_duxiu_derived'], duxiu_dict['aa_duxiu_derived']['isbn_multiple'])
        for duxiu_ssid in duxiu_dict['aa_duxiu_derived']['duxiu_ssid_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'duxiu_ssid', duxiu_ssid)
        for cadal_ssno in duxiu_dict['aa_duxiu_derived']['cadal_ssno_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'cadal_ssno', cadal_ssno)
        for issn in duxiu_dict['aa_duxiu_derived']['issn_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'issn', issn)
        for ean13 in duxiu_dict['aa_duxiu_derived']['ean13_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'ean13', ean13)
        for dxid in duxiu_dict['aa_duxiu_derived']['dxid_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'duxiu_dxid', dxid)
        for md5 in duxiu_dict['aa_duxiu_derived']['md5_multiple']:
            allthethings.utils.add_identifier_unified(duxiu_dict['aa_duxiu_derived'], 'md5', md5)

        # We know this collection is mostly Chinese language, so mark as Chinese if any of these (lightweight) tests pass.
        if 'isbn13' in duxiu_dict['aa_duxiu_derived']['identifiers_unified']:
            isbnlib_info = isbnlib.info(duxiu_dict['aa_duxiu_derived']['identifiers_unified']['isbn13'][0])
            if 'china' in isbnlib_info.lower():
                duxiu_dict['aa_duxiu_derived']['language_codes'] = ['zh']
        else: # If there is an isbn13 and it's not from China, then there's a good chance it's a foreign work, so don't do the language detect in that case.
            language_detect_string = " ".join(list(dict.fromkeys(duxiu_dict['aa_duxiu_derived']['title_multiple'] + duxiu_dict['aa_duxiu_derived']['author_multiple'] + duxiu_dict['aa_duxiu_derived']['publisher_multiple'])))
            langdetect_response = {}
            try:
                langdetect_response = ftlangdetect.detect(language_detect_string)
            except:
                pass
            duxiu_dict['aa_duxiu_derived']['debug_language_codes'] = { 'langdetect_response': langdetect_response }

            if langdetect_response['lang'] in ['zh', 'ja', 'ko'] and langdetect_response['score'] > 0.5: # Somewhat arbitrary cutoff for any CJK lang.
                duxiu_dict['aa_duxiu_derived']['language_codes'] = ['zh']

        duxiu_dict['aa_duxiu_derived']['title_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['title_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['author_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['author_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['publisher_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['publisher_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['year_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['year_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['series_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['series_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['pages_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['pages_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['filesize_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['filesize_multiple']), 0)
        duxiu_dict['aa_duxiu_derived']['filepath_best'] = next(iter(duxiu_dict['aa_duxiu_derived']['filepath_multiple']), '')
        duxiu_dict['aa_duxiu_derived']['description_best'] = '\n\n'.join(list(dict.fromkeys(duxiu_dict['aa_duxiu_derived']['description_cumulative'])))
        duxiu_dict['aa_duxiu_derived']['combined_comments'] = list(dict.fromkeys(filter(len, duxiu_dict['aa_duxiu_derived']['comments_cumulative'] + [
            # TODO: pass through comments metadata in a structured way so we can add proper translations.
            f"sources: {duxiu_dict['aa_duxiu_derived']['source_multiple']}" if len(duxiu_dict['aa_duxiu_derived']['source_multiple']) > 0 else "",
            f"original file paths: {duxiu_dict['aa_duxiu_derived']['filepath_multiple']}" if len(duxiu_dict['aa_duxiu_derived']['filepath_multiple']) > 0 else "",
        ])))
        duxiu_dict['aa_duxiu_derived']['edition_varia_normalized'] = ', '.join(list(dict.fromkeys(filter(len, [
            next(iter(duxiu_dict['aa_duxiu_derived']['series_multiple']), ''),
            next(iter(duxiu_dict['aa_duxiu_derived']['year_multiple']), ''),
        ]))))


        duxiu_dict_derived_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "source_multiple": ("before", ["Sources of the metadata."]),
            "md5_multiple": ("before", ["Includes both our generated MD5, and the original file MD5."]),
            "filesize_multiple": ("before", ["Includes both our generated file’s size, and the original filesize.",
                                "Our generated filesize should be the first listed."]),
            "miaochuan_links_multiple": ("before", ["For use with BaiduYun, though apparently now discontinued."]),
            "filepath_multiple": ("before", ["Original filenames."]),
            "ini_values_multiple": ("before", ["Extracted .ini-style entries from serialized_files."]),
            "language_codes": ("before", ["Our inferred language codes (BCP 47).",
                                "Gets set to 'zh' if the ISBN is Chinese, or if the language detection finds a CJK lang."]),
            "duxiu_ssid_multiple": ("before", ["Duxiu SSID, often extracted from .ini-style values or filename (8 digits)."
                                "This is then used to bring in more metadata."]),
            "title_best": ("before", ["For the DuXiu collection, these 'best' fields pick the first value from the '_multiple' fields."
                                "The first values are metadata taken directly from the files, followed by metadata from associated DuXiu SSID records."]),
        }
        duxiu_dict['aa_duxiu_derived'] = add_comments_to_dict(duxiu_dict['aa_duxiu_derived'], duxiu_dict_derived_comments)

        duxiu_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "duxiu_ssid": ("before", ["This is a DuXiu metadata record.",
                                "More details at https://annas-archive.org/datasets/duxiu",
                                allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "cadal_ssno": ("before", ["This is a CADAL metadata record.",
                                "More details at https://annas-archive.org/datasets/duxiu",
                                allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "md5": ("before", ["This is a DuXiu/related metadata record.",
                                "More details at https://annas-archive.org/datasets/duxiu",
                                allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "duxiu_file": ("before", ["Information on the actual file in our collection (see torrents)."]),
            "aa_duxiu_derived": ("before", "Derived metadata."),
            "aac_records": ("before", "Metadata records from the 'duxiu_records' file, which is a compilation of metadata from various sources."),
        }
        duxiu_dicts.append(add_comments_to_dict(duxiu_dict, duxiu_dict_comments))

    # TODO: Look at more ways of associating remote files besides SSID.
    # TODO: Parse TOCs.
    # TODO: Book covers.
    # TODO: DuXiu book types mostly (even only?) non-fiction?
    # TODO: Mostly Chinese, detect non-Chinese based on English text or chars in title?
    # TODO: Pull in more CADAL fields.

    return duxiu_dicts

# Good examples:
# select primary_id, count(*) as c, group_concat(json_extract(metadata, '$.type')) as type from annas_archive_meta__aacid__duxiu_records group by primary_id order by c desc limit 100;
# duxiu_ssid_10000431    |        3 | "dx_20240122__books","dx_20240122__remote_files","512w_final_csv"
# cadal_ssno_06G48911    |        2 | "cadal_table__site_journal_items","cadal_table__sa_newspaper_items"
# cadal_ssno_01000257    |        2 | "cadal_table__site_book_collection_items","cadal_table__sa_collection_items"
# cadal_ssno_06G48910    |        2 | "cadal_table__sa_newspaper_items","cadal_table__site_journal_items"
# cadal_ssno_ZY297043388 |        2 | "cadal_table__sa_collection_items","cadal_table__books_aggregation"
# cadal_ssno_01000001    |        2 | "cadal_table__books_solr","cadal_table__books_detail"
# duxiu_ssid_11454502    |        1 | "dx_toc_db__dx_toc"
# duxiu_ssid_10002062    |        1 | "DX_corrections240209_csv"
# 
# duxiu_ssid_14084714 has Miaochuan link.
# cadal_ssno_44517971 has some <font>s.
# 
@page.get("/db/duxiu_ssid/<path:duxiu_ssid>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def duxiu_ssid_json(duxiu_ssid):
    with Session(engine) as session:
        duxiu_dicts = get_duxiu_dicts(session, 'duxiu_ssid', [duxiu_ssid])
        if len(duxiu_dicts) == 0:
            return "{}", 404
        return nice_json(duxiu_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/cadal_ssno/<path:cadal_ssno>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def cadal_ssno_json(cadal_ssno):
    with Session(engine) as session:
        duxiu_dicts = get_duxiu_dicts(session, 'cadal_ssno', [cadal_ssno])
        if len(duxiu_dicts) == 0:
            return "{}", 404
        return nice_json(duxiu_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/duxiu_md5/<path:md5>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def duxiu_md5_json(md5):
    with Session(engine) as session:
        duxiu_dicts = get_duxiu_dicts(session, 'md5', [md5])
        if len(duxiu_dicts) == 0:
            return "{}", 404
        return nice_json(duxiu_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def get_embeddings_for_aarecords(session, aarecords):
    aarecord_ids = [aarecord['id'] for aarecord in aarecords]
    hashed_aarecord_ids = [hashlib.md5(aarecord['id'].encode()).digest() for aarecord in aarecords]

    embedding_text_by_aarecord_id = { aarecord['id']: (' '.join([
            *f"Title: '{aarecord['file_unified_data']['title_best']}'".split(' '),
            *f"Author: '{aarecord['file_unified_data']['author_best']}'".split(' '),
            *f"Edition: '{aarecord['file_unified_data']['edition_varia_best']}'".split(' '),
            *f"Publisher: '{aarecord['file_unified_data']['publisher_best']}'".split(' '),
            *f"Filename: '{aarecord['file_unified_data']['original_filename_best_name_only']}'".split(' '),
            *f"Description: '{aarecord['file_unified_data']['stripped_description_best']}'".split(' '),
        ][0:500])) for aarecord in aarecords }

    session.connection().connection.ping(reconnect=True)
    cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
    cursor.execute(f'SELECT * FROM model_cache WHERE model_name = "e5_small_query" AND hashed_aarecord_id IN %(hashed_aarecord_ids)s', { "hashed_aarecord_ids": hashed_aarecord_ids })
    rows_by_aarecord_id = { row['aarecord_id']: row for row in cursor.fetchall() }

    embeddings = []
    insert_data_e5_small_query = []
    for aarecord_id in aarecord_ids:
        embedding_text = embedding_text_by_aarecord_id[aarecord_id]
        if aarecord_id in rows_by_aarecord_id:
            if rows_by_aarecord_id[aarecord_id]['embedding_text'] != embedding_text:
                print(f"WARNING! embedding_text has changed for e5_small_query: {aarecord_id=} {rows_by_aarecord_id[aarecord_id]['embedding_text']=} {embedding_text=}")
            embeddings.append({ 'e5_small_query': list(struct.unpack(f"{len(rows_by_aarecord_id[aarecord_id]['embedding'])//4}f", rows_by_aarecord_id[aarecord_id]['embedding'])) })
        else:
            e5_small_query = list(map(float, get_e5_small_model().encode(f"query: {embedding_text}", normalize_embeddings=True)))
            embeddings.append({ 'e5_small_query': e5_small_query })
            insert_data_e5_small_query.append({
                'hashed_aarecord_id': hashlib.md5(aarecord_id.encode()).digest(),
                'aarecord_id': aarecord_id,
                'model_name': 'e5_small_query',
                'embedding_text': embedding_text,
                'embedding': struct.pack(f'{len(e5_small_query)}f', *e5_small_query),
            })

    if len(insert_data_e5_small_query) > 0:
        session.connection().connection.ping(reconnect=True)
        cursor.executemany(f"REPLACE INTO model_cache (hashed_aarecord_id, aarecord_id, model_name, embedding_text, embedding) VALUES (%(hashed_aarecord_id)s, %(aarecord_id)s, %(model_name)s, %(embedding_text)s, %(embedding)s)", insert_data_e5_small_query)
        cursor.execute("COMMIT")

    return embeddings


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
    #     return [add_additional_to_aarecord({ '_source': aarecord }) for aarecord in get_aarecords_mysql(session, aarecord_ids)]

    docs_by_es_handle = collections.defaultdict(list)
    for aarecord_id in aarecord_ids:
        indexes = allthethings.utils.get_aarecord_search_indexes_for_id_prefix(aarecord_id.split(':', 1)[0])
        for index in indexes:
            es_handle = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[index]
            docs_by_es_handle[es_handle].append({'_id': aarecord_id, '_index': f'{index}__{allthethings.utils.virtshard_for_aarecord_id(aarecord_id)}' })

    search_results_raw = []
    for es_handle, docs in docs_by_es_handle.items():
        search_results_raw += es_handle.mget(docs=docs)['docs']
    return [add_additional_to_aarecord(aarecord_raw) for aarecord_raw in search_results_raw if aarecord_raw.get('found') and (aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids)]


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
    aarecord_ids = list(dict.fromkeys([val for val in aarecord_ids if val not in search_filtered_bad_aarecord_ids]))

    split_ids = allthethings.utils.split_aarecord_ids(aarecord_ids)
    lgrsnf_book_dicts = dict(('md5:' + item['md5'].lower(), item) for item in get_lgrsnf_book_dicts(session, "MD5", split_ids['md5']))
    lgrsfic_book_dicts = dict(('md5:' + item['md5'].lower(), item) for item in get_lgrsfic_book_dicts(session, "MD5", split_ids['md5']))
    lgli_file_dicts = dict(('md5:' + item['md5'].lower(), item) for item in get_lgli_file_dicts(session, "md5", split_ids['md5']))
    zlib_book_dicts1 = dict(('md5:' + item['md5_reported'].lower(), item) for item in get_zlib_book_dicts(session, "md5_reported", split_ids['md5']))
    zlib_book_dicts2 = dict(('md5:' + item['md5'].lower(), item) for item in get_zlib_book_dicts(session, "md5", split_ids['md5']))
    aac_zlib3_book_dicts1 = dict(('md5:' + item['md5_reported'].lower(), item) for item in get_aac_zlib3_book_dicts(session, "md5_reported", split_ids['md5']))
    aac_zlib3_book_dicts2 = dict(('md5:' + item['md5'].lower(), item) for item in get_aac_zlib3_book_dicts(session, "md5", split_ids['md5']))
    ia_record_dicts = dict(('md5:' + item['aa_ia_file']['md5'].lower(), item) for item in get_ia_record_dicts(session, "md5", split_ids['md5']) if item.get('aa_ia_file') is not None)
    ia_record_dicts2 = dict(('ia:' + item['ia_id'].lower(), item) for item in get_ia_record_dicts(session, "ia_id", split_ids['ia']) if item.get('aa_ia_file') is None)
    isbndb_dicts = {('isbn:' + item['ean13']): item['isbndb'] for item in get_isbndb_dicts(session, split_ids['isbn'])}
    ol_book_dicts = {('ol:' + item['ol_edition']): [item] for item in get_ol_book_dicts(session, 'ol_edition', split_ids['ol'])}
    scihub_doi_dicts = {('doi:' + item['doi']): [item] for item in get_scihub_doi_dicts(session, 'doi', split_ids['doi'])}
    oclc_dicts = {('oclc:' + item['oclc_id']): [item] for item in get_oclc_dicts(session, 'oclc', split_ids['oclc'])}
    duxiu_dicts = {('duxiu_ssid:' + item['duxiu_ssid']): item for item in get_duxiu_dicts(session, 'duxiu_ssid', split_ids['duxiu_ssid'])}
    duxiu_dicts2 = {('cadal_ssno:' + item['cadal_ssno']): item for item in get_duxiu_dicts(session, 'cadal_ssno', split_ids['cadal_ssno'])}
    duxiu_dicts3 = {('md5:' + item['md5']): item for item in get_duxiu_dicts(session, 'md5', split_ids['md5'])}

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
        aarecord['ia_record'] = ia_record_dicts.get(aarecord_id) or ia_record_dicts2.get(aarecord_id)
        aarecord['isbndb'] = list(isbndb_dicts.get(aarecord_id) or [])
        aarecord['ol'] = list(ol_book_dicts.get(aarecord_id) or [])
        aarecord['scihub_doi'] = list(scihub_doi_dicts.get(aarecord_id) or [])
        aarecord['oclc'] = list(oclc_dicts.get(aarecord_id) or [])
        aarecord['duxiu'] = duxiu_dicts.get(aarecord_id) or duxiu_dicts2.get(aarecord_id) or duxiu_dicts3.get(aarecord_id)
        
        lgli_all_editions = aarecord['lgli_file']['editions'] if aarecord.get('lgli_file') else []

        aarecord['file_unified_data'] = {}
        # Duplicated below, with more fields
        aarecord['file_unified_data']['identifiers_unified'] = allthethings.utils.merge_unified_fields([
            ((aarecord['lgrsnf_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgli_file'] or {}).get('identifiers_unified') or {}),
            *[(edition['identifiers_unified'].get('identifiers_unified') or {}) for edition in lgli_all_editions],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('identifiers_unified') or {}),
            *[isbndb['identifiers_unified'] for isbndb in aarecord['isbndb']],
            *[ol_book_dict['identifiers_unified'] for ol_book_dict in aarecord['ol']],
            *[scihub_doi['identifiers_unified'] for scihub_doi in aarecord['scihub_doi']],
            *[oclc['aa_oclc_derived']['identifiers_unified'] for oclc in aarecord['oclc']],
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('identifiers_unified') or {}),
        ])
        # TODO: This `if` is not necessary if we make sure that the fields of the primary records get priority.
        if not allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0]):
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

    isbndb_dicts2 = {item['ean13']: item for item in get_isbndb_dicts(session, list(dict.fromkeys(canonical_isbn13s)))}
    ol_book_dicts2 = {item['ol_edition']: item for item in get_ol_book_dicts(session, 'ol_edition', list(dict.fromkeys(ol_editions)))}
    ol_book_dicts2_for_isbn13 = get_ol_book_dicts_by_isbn13(session, list(dict.fromkeys(canonical_isbn13s)))
    scihub_doi_dicts2 = {item['doi']: item for item in get_scihub_doi_dicts(session, 'doi', list(dict.fromkeys(dois)))}

    # Too expensive.. TODO: enable combining results from ES?
    # oclc_dicts2 = {item['oclc_id']: item for item in get_oclc_dicts(session, 'oclc', list(dict.fromkeys(oclc_ids)))}
    # oclc_dicts2_for_isbn13 = get_oclc_dicts_by_isbn13(session, list(dict.fromkeys(canonical_isbn13s)))
    oclc_id_by_isbn13 = get_oclc_id_by_isbn13(session, list(dict.fromkeys(canonical_isbn13s)))

    # Second pass
    for aarecord in aarecords:
        aarecord_id = aarecord['id']
        aarecord_id_split = aarecord_id.split(':', 1)
        lgli_single_edition = aarecord['lgli_file']['editions'][0] if len((aarecord.get('lgli_file') or {}).get('editions') or []) == 1 else None
        lgli_all_editions = aarecord['lgli_file']['editions'] if aarecord.get('lgli_file') else []

        if not allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0]):
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

            ol_book_dicts_all = []
            existing_ol_editions = set([ol_book_dict['ol_edition'] for ol_book_dict in aarecord['ol']])
            for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
                for ol_book_dict in (ol_book_dicts2_for_isbn13.get(canonical_isbn13) or []):
                    if ol_book_dict['ol_edition'] not in existing_ol_editions:
                        ol_book_dicts_all.append(ol_book_dict)
                        existing_ol_editions.add(ol_book_dict['ol_edition']) # TODO: restructure others to also do something similar?
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

            # oclc_all = []
            # existing_oclc_ids = set([oclc['oclc_id'] for oclc in aarecord['oclc']])
            # for oclc_id in (aarecord['file_unified_data']['identifiers_unified'].get('oclc') or []):
            #     if (oclc_id in oclc_dicts2) and (oclc_id not in existing_oclc_ids):
            #         oclc_all.append(oclc_dicts2[oclc_id])
            # if len(oclc_all) > 3:
            #     oclc_all = []
            # aarecord['oclc'] = (aarecord['oclc'] + oclc_all)

            # oclc_all = []
            # existing_oclc_ids = set([oclc['oclc_id'] for oclc in aarecord['oclc']])
            # for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
            #     for oclc_dict in (oclc_dicts2_for_isbn13.get(canonical_isbn13) or []):
            #         if oclc_dict['oclc_id'] not in existing_oclc_ids:
            #             oclc_all.append(oclc_dict)
            #             existing_oclc_ids.add(oclc_dict['oclc_id']) # TODO: restructure others to also do something similar?
            # if len(oclc_all) > 3:
            #     oclc_all = []
            # aarecord['oclc'] = (aarecord['oclc'] + oclc_all)

            for canonical_isbn13 in (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []):
                for oclc_id in (oclc_id_by_isbn13.get(canonical_isbn13) or []):
                    allthethings.utils.add_identifier_unified(aarecord['file_unified_data'], 'oclc', oclc_id)            

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
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('filepath_best') or '').strip(),
        ]
        original_filename_multiple_processed = sort_by_length_and_filter_subsequences_with_longest_string(original_filename_multiple)
        aarecord['file_unified_data']['original_filename_best'] = min(original_filename_multiple_processed, key=len) if len(original_filename_multiple_processed) > 0 else ''
        original_filename_multiple += [(scihub_doi['doi'].strip() + '.pdf') for scihub_doi in aarecord['scihub_doi']]
        original_filename_multiple += (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('filepath_multiple') or [])
        if aarecord['file_unified_data']['original_filename_best'] == '':
            original_filename_multiple_processed = sort_by_length_and_filter_subsequences_with_longest_string(original_filename_multiple)
            aarecord['file_unified_data']['original_filename_best'] = min(original_filename_multiple_processed, key=len) if len(original_filename_multiple_processed) > 0 else ''
        aarecord['file_unified_data']['original_filename_additional'] = [s for s in original_filename_multiple_processed if s != aarecord['file_unified_data']['original_filename_best']]
        aarecord['file_unified_data']['original_filename_best_name_only'] = re.split(r'[\\/]', aarecord['file_unified_data']['original_filename_best'])[-1] if not aarecord['file_unified_data']['original_filename_best'].startswith('10.') else aarecord['file_unified_data']['original_filename_best']
        if len(aarecord['file_unified_data']['original_filename_additional']) == 0:
            del aarecord['file_unified_data']['original_filename_additional']

        # Select the cover_url_normalized in order of what is likely to be the best one: ia, lgrsnf, lgrsfic, lgli, zlib.
        cover_url_multiple = [
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('cover_url') or '').strip(),
            ((aarecord['lgrsnf_book'] or {}).get('cover_url_normalized') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('cover_url_normalized') or '').strip(),
            ((aarecord['lgli_file'] or {}).get('cover_url_guess_normalized') or '').strip(),
            ((aarecord['zlib_book'] or {}).get('cover_url_guess') or '').strip(),
            *[ol_book_dict['cover_url_normalized'] for ol_book_dict in aarecord['ol']],
            *[(isbndb['json'].get('image') or '').strip() for isbndb in aarecord['isbndb']],
        ]
        cover_url_multiple_processed = list(dict.fromkeys(filter(len, cover_url_multiple)))
        aarecord['file_unified_data']['cover_url_best'] = (cover_url_multiple_processed + [''])[0]
        aarecord['file_unified_data']['cover_url_additional'] = [s for s in cover_url_multiple_processed if s != aarecord['file_unified_data']['cover_url_best']]
        if aarecord['file_unified_data']['cover_url_best'] == '':
            cover_url_multiple += [isbndb['cover_url_guess'] for isbndb in aarecord['isbndb']]
            cover_url_multiple.append(((aarecord['aac_zlib3_book'] or {}).get('cover_url_guess') or '').strip())
            cover_url_multiple.append(((aarecord['zlib_book'] or {}).get('cover_url_guess') or '').strip())
            cover_url_multiple_processed = list(dict.fromkeys(filter(len, cover_url_multiple)))
            aarecord['file_unified_data']['cover_url_best'] = (cover_url_multiple_processed + [''])[0]
            aarecord['file_unified_data']['cover_url_additional'] = [s for s in cover_url_multiple_processed if s != aarecord['file_unified_data']['cover_url_best']]
        if len(aarecord['file_unified_data']['cover_url_additional']) == 0:
            del aarecord['file_unified_data']['cover_url_additional']

        extension_multiple = [
            (((aarecord['ia_record'] or {}).get('aa_ia_file') or {}).get('extension') or '').strip(),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgrsnf_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgrsfic_book'] or {}).get('extension') or '').strip().lower(),
            ((aarecord['lgli_file'] or {}).get('extension') or '').strip().lower(),
            (((aarecord['duxiu'] or {}).get('duxiu_file') or {}).get('extension') or '').strip().lower(),
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
            (aarecord['aac_zlib3_book'] or {}).get('filesize') or 0,
            (aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('filesize_reported') or 0,
            (aarecord['zlib_book'] or {}).get('filesize') or 0,
            (aarecord['lgrsnf_book'] or {}).get('filesize') or 0,
            (aarecord['lgrsfic_book'] or {}).get('filesize') or 0,
            (aarecord['lgli_file'] or {}).get('filesize') or 0,
            ((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('filesize_best') or 0,
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
        filesize_multiple += (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('filesize_multiple') or [])
        aarecord['file_unified_data']['filesize_additional'] = [s for s in dict.fromkeys(filter(lambda fz: fz > 0, filesize_multiple)) if s != aarecord['file_unified_data']['filesize_best']]
        if len(aarecord['file_unified_data']['filesize_additional']) == 0:
            del aarecord['file_unified_data']['filesize_additional']

        title_multiple = [
            ((aarecord['lgrsnf_book'] or {}).get('title') or '').strip(),
            ((aarecord['lgrsfic_book'] or {}).get('title') or '').strip(),
            ((lgli_single_edition or {}).get('title') or '').strip(),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('title') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('title') or '').strip(),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('title_best') or '').strip(),
        ]
        aarecord['file_unified_data']['title_best'] = max(title_multiple, key=len)
        title_multiple += [(edition.get('title') or '').strip() for edition in lgli_all_editions]
        title_multiple += [title.strip() for edition in lgli_all_editions for title in (edition['descriptions_mapped'].get('maintitleonoriginallanguage') or [])]
        title_multiple += [title.strip() for edition in lgli_all_editions for title in (edition['descriptions_mapped'].get('maintitleonenglishtranslate') or [])]
        title_multiple += [(ol_book_dict.get('title_normalized') or '').strip() for ol_book_dict in aarecord['ol']]
        title_multiple += [(isbndb.get('title_normalized') or '').strip() for isbndb in aarecord['isbndb']]
        title_multiple += (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('title_multiple') or [])
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
            (aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('author', '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('author') or '').strip(),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('author_best') or '').strip(),
        ]
        aarecord['file_unified_data']['author_best'] = max(author_multiple, key=len)
        author_multiple += [edition.get('authors_normalized', '').strip() for edition in lgli_all_editions]
        author_multiple += [ol_book_dict['authors_normalized'] for ol_book_dict in aarecord['ol']]
        author_multiple += [", ".join(isbndb['json'].get('authors') or []) for isbndb in aarecord['isbndb']]
        author_multiple += (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('author_multiple') or [])
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
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('publisher') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('publisher') or '').strip(),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('publisher_best') or '').strip(),
        ]
        aarecord['file_unified_data']['publisher_best'] = max(publisher_multiple, key=len)
        publisher_multiple += [(edition.get('publisher_normalized') or '').strip() for edition in lgli_all_editions]
        publisher_multiple += [(ol_book_dict.get('publishers_normalized') or '').strip() for ol_book_dict in aarecord['ol']]
        publisher_multiple += [(isbndb['json'].get('publisher') or '').strip() for isbndb in aarecord['isbndb']]
        publisher_multiple += (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('publisher_multiple') or [])
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
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('edition_varia_normalized') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('edition_varia_normalized') or '').strip(),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('edition_varia_normalized') or '').strip(),
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
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('year') or '').strip(),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('year') or '').strip(),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('year_best') or '').strip(),
        ]
        # Filter out years in for which we surely don't have books (famous last words..)
        year_multiple = [(year if year.isdigit() and int(year) >= 1600 and int(year) < 2100 else '') for year in year_multiple_raw]
        aarecord['file_unified_data']['year_best'] = max(year_multiple, key=len)
        year_multiple += [(edition.get('year_normalized') or '').strip() for edition in lgli_all_editions]
        year_multiple += [(ol_book_dict.get('year_normalized') or '').strip() for ol_book_dict in aarecord['ol']]
        year_multiple += [(isbndb.get('year_normalized') or '').strip() for isbndb in aarecord['isbndb']]
        year_multiple += (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('year_multiple') or [])
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
            *(((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('combined_comments') or []),
            *(((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('combined_comments') or []),
        ]
        comments_multiple += [(edition.get('comments_normalized') or '').strip() for edition in lgli_all_editions]
        for edition in lgli_all_editions:
            comments_multiple.append((edition.get('editions_add_info') or '').strip())
            comments_multiple.append((edition.get('commentary') or '').strip())
            for note in (edition.get('descriptions_mapped') or {}).get('descriptions_mapped.notes', []):
                comments_multiple.append(note.strip())
        for ol_book_dict in aarecord['ol']:
            for comment in ol_book_dict.get('comments_normalized') or []:
                comments_multiple.append(comment.strip())
        aarecord['file_unified_data']['comments_multiple'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(comments_multiple)]
        if len(aarecord['file_unified_data']['comments_multiple']) == 0:
            del aarecord['file_unified_data']['comments_multiple']

        stripped_description_multiple = [
            ((aarecord['lgrsnf_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((aarecord['lgrsfic_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((lgli_single_edition or {}).get('stripped_description') or '').strip()[0:5000],
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('description_best') or '').strip(),
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
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('language_codes') or []),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('language_codes') or []),
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('language_codes') or []),
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
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('identifiers_unified') or {}),
            ((aarecord['lgli_file'] or {}).get('identifiers_unified') or {}),
            *[(edition['identifiers_unified'].get('identifiers_unified') or {}) for edition in lgli_all_editions],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('identifiers_unified') or {}),
            *[isbndb['identifiers_unified'] for isbndb in aarecord['isbndb']],
            *[ol_book_dict['identifiers_unified'] for ol_book_dict in aarecord['ol']],
            *[scihub_doi['identifiers_unified'] for scihub_doi in aarecord['scihub_doi']],
            *[oclc['aa_oclc_derived']['identifiers_unified'] for oclc in aarecord['oclc']],
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('identifiers_unified') or {}),
        ])
        aarecord['file_unified_data']['classifications_unified'] = allthethings.utils.merge_unified_fields([
            ((aarecord['lgrsnf_book'] or {}).get('classifications_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('classifications_unified') or {}),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('classifications_unified') or {}),
            *[(edition.get('classifications_unified') or {}) for edition in lgli_all_editions],
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('classifications_unified') or {}),
            *[isbndb['classifications_unified'] for isbndb in aarecord['isbndb']],
            *[ol_book_dict['classifications_unified'] for ol_book_dict in aarecord['ol']],
            *[scihub_doi['classifications_unified'] for scihub_doi in aarecord['scihub_doi']],
        ])

        aarecord['file_unified_data']['added_date_unified'] = dict(collections.ChainMap(*[
            ((aarecord['lgrsnf_book'] or {}).get('added_date_unified') or {}),
            ((aarecord['lgrsfic_book'] or {}).get('added_date_unified') or {}),
            ((aarecord['aac_zlib3_book'] or aarecord['zlib_book'] or {}).get('added_date_unified') or {}),
            ((aarecord['lgli_file'] or {}).get('added_date_unified') or {}),
            (((aarecord['ia_record'] or {}).get('aa_ia_derived') or {}).get('added_date_unified') or {}),
            *[isbndb['added_date_unified'] for isbndb in aarecord['isbndb']],
            *[ol_book_dict['added_date_unified'] for ol_book_dict in aarecord['ol']],
            *[oclc['aa_oclc_derived']['added_date_unified'] for oclc in aarecord['oclc']],
            (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('added_date_unified') or {}),
        ]))

        aarecord['file_unified_data']['added_date_best'] = ''
        if aarecord_id_split[0] == 'md5':
            potential_dates = list(filter(len, [
                (aarecord['file_unified_data']['added_date_unified'].get('duxiu_filegen') or ''),
                (aarecord['file_unified_data']['added_date_unified'].get('ia_file_scrape') or ''),
                (aarecord['file_unified_data']['added_date_unified'].get('lgli_source') or ''),
                (aarecord['file_unified_data']['added_date_unified'].get('lgrsfic_source') or ''),
                (aarecord['file_unified_data']['added_date_unified'].get('lgrsnf_source') or ''),
                (aarecord['file_unified_data']['added_date_unified'].get('zlib_source') or ''),
            ]))
            if len(potential_dates) > 0:
                aarecord['file_unified_data']['added_date_best'] = min(potential_dates)
        elif aarecord_id_split[0] == 'ia':
            if 'ia_source' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['ia_source']
        elif aarecord_id_split[0] == 'isbn':
            if 'isbndb_scrape' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['isbndb_scrape']
        elif aarecord_id_split[0] == 'ol':
            if 'ol_source' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['ol_source']
        elif aarecord_id_split[0] == 'doi':
            pass # We don't have the information of when this was added to scihub sadly.
        elif aarecord_id_split[0] == 'oclc':
            if 'oclc_scrape' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['oclc_scrape']
        elif aarecord_id_split[0] == 'duxiu_ssid':
            if 'duxiu_meta_scrape' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['duxiu_meta_scrape']
        elif aarecord_id_split[0] == 'cadal_ssno':
            if 'duxiu_meta_scrape' in aarecord['file_unified_data']['added_date_unified']:
                aarecord['file_unified_data']['added_date_best'] = aarecord['file_unified_data']['added_date_unified']['duxiu_meta_scrape']
        else:
            raise Exception(f"Unknown {aarecord_id_split[0]=}")

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
        if ((aarecord['aac_zlib3_book'] or {}).get('removed') or 0) == 1:
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
                'removed': (aarecord['aac_zlib3_book'].get('removed') or 0),
            }
        if aarecord['ia_record'] is not None:
            aarecord['ia_record'] = {
                'ia_id': aarecord['ia_record']['ia_id'],
                # 'has_thumb': aarecord['ia_record']['has_thumb'],
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
        if aarecord['duxiu'] is not None:
            aarecord['duxiu'] = {
                'duxiu_ssid': aarecord['duxiu'].get('duxiu_ssid'),
                'cadal_ssno': aarecord['duxiu'].get('cadal_ssno'),
                'md5': aarecord['duxiu'].get('md5'),
                'duxiu_file': aarecord['duxiu'].get('duxiu_file'),
                'aa_duxiu_derived': {
                    'miaochuan_links_multiple': aarecord['duxiu']['aa_duxiu_derived']['miaochuan_links_multiple'],
                }
            }
            if aarecord['duxiu']['duxiu_ssid'] is None:
                del aarecord['duxiu']['duxiu_ssid']
            if aarecord['duxiu']['cadal_ssno'] is None:
                del aarecord['duxiu']['cadal_ssno']

        # Even though `additional` is only for computing real-time stuff,
        # we'd like to cache some fields for in the search results.
        with force_locale('en'):
            additional = get_additional_for_aarecord(aarecord)
            aarecord['file_unified_data']['has_aa_downloads'] = additional['has_aa_downloads']
            aarecord['file_unified_data']['has_aa_exclusive_downloads'] = additional['has_aa_exclusive_downloads']
            aarecord['file_unified_data']['has_torrent_paths'] = (1 if (len(additional['torrent_paths']) > 0) else 0)

        search_content_type = aarecord['file_unified_data']['content_type']
        # Once we have the content type.
        aarecord['indexes'] = [allthethings.utils.get_aarecord_search_index(aarecord_id_split[0], search_content_type)]

        initial_search_text = "\n".join([
            aarecord['file_unified_data']['title_best'][:1000],
            aarecord['file_unified_data']['title_best'][:1000],
            aarecord['file_unified_data']['title_best'][:1000],
            aarecord['file_unified_data']['author_best'][:1000],
            aarecord['file_unified_data']['author_best'][:1000],
            aarecord['file_unified_data']['author_best'][:1000],
            aarecord['file_unified_data']['edition_varia_best'][:1000],
            aarecord['file_unified_data']['edition_varia_best'][:1000],
            aarecord['file_unified_data']['publisher_best'][:1000],
            aarecord['file_unified_data']['publisher_best'][:1000],
            aarecord['file_unified_data']['original_filename_best_name_only'][:1000],
            aarecord['file_unified_data']['original_filename_best_name_only'][:1000],
            aarecord['id'][:1000],
        ])
        # Duplicate search terms that contain punctuation, in *addition* to the original search terms (so precise matches still work).
        split_search_text = set(initial_search_text.split())
        normalized_search_terms = initial_search_text.replace('.', ' ').replace(':', ' ').replace('_', ' ').replace('/', ' ').replace('\\', ' ')
        filtered_normalized_search_terms = ' '.join([term for term in normalized_search_terms.split() if term not in split_search_text])
        more_search_text = "\n".join([
            aarecord['file_unified_data']['extension_best'],
            *[f"{key}:{item} {item}" for key, items in aarecord['file_unified_data']['identifiers_unified'].items() for item in items],
            *[f"{key}:{item} {item}" for key, items in aarecord['file_unified_data']['classifications_unified'].items() for item in items],
            aarecord_id,
        ])
        search_text = f"{initial_search_text}\n\n{filtered_normalized_search_terms}\n\n{more_search_text}"

        aarecord['search_only_fields'] = {
            # 'search_e5_small_query': embeddings['e5_small_query'],
            'search_filesize': aarecord['file_unified_data']['filesize_best'],
            'search_year': aarecord['file_unified_data']['year_best'],
            'search_extension': aarecord['file_unified_data']['extension_best'],
            'search_content_type': search_content_type,
            'search_most_likely_language_code': aarecord['file_unified_data']['most_likely_language_code'],
            'search_isbn13': (aarecord['file_unified_data']['identifiers_unified'].get('isbn13') or []),
            'search_doi': (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []),
            'search_title': aarecord['file_unified_data']['title_best'],
            'search_author': aarecord['file_unified_data']['author_best'],
            'search_publisher': aarecord['file_unified_data']['publisher_best'],
            'search_edition_varia': aarecord['file_unified_data']['edition_varia_best'],
            'search_original_filename': aarecord['file_unified_data']['original_filename_best'],
            'search_added_date': aarecord['file_unified_data']['added_date_best'],
            'search_description_comments': ('\n'.join([aarecord['file_unified_data']['stripped_description_best']] + (aarecord['file_unified_data'].get('comments_multiple') or [])))[:10000],
            'search_text': search_text,
            'search_access_types': [
                *(['external_download'] if any([((aarecord.get(field) is not None) and (type(aarecord[field]) != list or len(aarecord[field]) > 0)) for field in ['lgrsnf_book', 'lgrsfic_book', 'lgli_file', 'zlib_book', 'aac_zlib3_book', 'scihub_doi']]) else []),
                *(['external_borrow'] if (aarecord.get('ia_record') and (not aarecord['ia_record']['aa_ia_derived']['printdisabled_only'])) else []),
                *(['external_borrow_printdisabled'] if (aarecord.get('ia_record') and (aarecord['ia_record']['aa_ia_derived']['printdisabled_only'])) else []),
                *(['aa_download'] if aarecord['file_unified_data']['has_aa_downloads'] == 1 else []),
                *(['meta_explore'] if allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0]) else []),
            ],
            'search_record_sources': list(dict.fromkeys([
                *(['lgrs']      if aarecord['lgrsnf_book'] is not None else []),
                *(['lgrs']      if aarecord['lgrsfic_book'] is not None else []),
                *(['lgli']      if aarecord['lgli_file'] is not None else []),
                *(['zlib']      if aarecord['zlib_book'] is not None else []),
                *(['zlib']      if aarecord['aac_zlib3_book'] is not None else []),
                *(['ia']        if aarecord['ia_record'] is not None else []),
                *(['scihub']    if len(aarecord['scihub_doi']) > 0 else []),
                *(['isbndb']    if (aarecord_id_split[0] == 'isbn' and len(aarecord['isbndb'] or []) > 0) else []),
                *(['ol']        if (aarecord_id_split[0] == 'ol' and len(aarecord['ol'] or []) > 0) else []),
                *(['oclc']      if (aarecord_id_split[0] == 'oclc' and len(aarecord['oclc'] or []) > 0) else []),
                *(['duxiu']     if aarecord['duxiu'] is not None else []),
            ])),
            # Used in external system, check before changing.
            'search_bulk_torrents': 'has_bulk_torrents' if aarecord['file_unified_data']['has_torrent_paths'] else 'no_bulk_torrents',
        }
        
        # At the very end
        aarecord['search_only_fields']['search_score_base_rank'] = float(aarecord_score_base(aarecord))

    # embeddings = get_embeddings_for_aarecords(session, aarecords)
    # for embedding, aarecord in zip(embeddings, aarecords):
    #     aarecord['search_only_fields']['search_e5_small_query'] = embedding['e5_small_query']
    
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
            "book_unknown":       "📗 " + gettext("common.md5_content_type_mapping.book_unknown"),
            "book_nonfiction":    "📘 " + gettext("common.md5_content_type_mapping.book_nonfiction"),
            "book_fiction":       "📕 " + gettext("common.md5_content_type_mapping.book_fiction"),
            "journal_article":    "📄 " + gettext("common.md5_content_type_mapping.journal_article"),
            "standards_document": "📝 " + gettext("common.md5_content_type_mapping.standards_document"),
            "magazine":           "📰 " + gettext("common.md5_content_type_mapping.magazine"),
            "book_comic":         "💬 " + gettext("common.md5_content_type_mapping.book_comic"),
            "musical_score":      "🎶 " + gettext("common.md5_content_type_mapping.musical_score"),
            "other":              "🤨 " + gettext("common.md5_content_type_mapping.other"),
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
            "oclc": gettext("common.record_sources_mapping.oclc"),
            "duxiu": gettext("common.record_sources_mapping.duxiu"),
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
    targeted_seconds = 200
    if modifier == 'aa_exclusive':
        targeted_seconds = 300
        additional['has_aa_exclusive_downloads'] = 1
    if modifier == 'scimag':
        targeted_seconds = 10
    # When changing the domains, don't forget to change md5_fast_download and md5_slow_download.
    for index in range(len(allthethings.utils.FAST_DOWNLOAD_DOMAINS)):
        additional['fast_partner_urls'].append((gettext("common.md5.servers.fast_partner", number=len(additional['fast_partner_urls'])+1), '/fast_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/' + str(index), gettext("common.md5.servers.no_browser_verification") if len(additional['fast_partner_urls']) == 0 else ''))
    for index in range(len(allthethings.utils.SLOW_DOWNLOAD_DOMAINS)):
        additional['slow_partner_urls'].append((gettext("common.md5.servers.slow_partner", number=len(additional['slow_partner_urls'])+1), '/slow_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/' + str(index), gettext("common.md5.servers.browser_verification_unlimited", a_browser=' href="/browser_verification" ') if len(additional['slow_partner_urls']) == 0 else ''))
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
    CODES_PRIORITY = ['isbn13', 'isbn10', 'csbn', 'doi', 'issn', 'udc', 'oclc', 'ol', 'ocaid', 'asin', 'duxiu_ssid', 'cadal_ssno']
    additional['codes'].sort(key=lambda item: (CODES_PRIORITY.index(item['key']) if item['key'] in CODES_PRIORITY else 100))

    md5_content_type_mapping = get_md5_content_type_mapping(allthethings.utils.get_base_lang_code(get_locale()))

    cover_url = (aarecord['file_unified_data'].get('cover_url_best', None) or '')
    if 'zlib' in cover_url or '1lib' in cover_url:
        non_zlib_covers = [url for url in (aarecord['file_unified_data'].get('cover_url_additional', None) or []) if ('zlib' not in url and '1lib' not in url)]
        if len(non_zlib_covers) > 0:
            cover_url = non_zlib_covers[0]
        else:
            cover_url = ""

    comments_multiple = '\n\n'.join(aarecord['file_unified_data'].get('comments_multiple') or [])

    additional['top_box'] = {
        'meta_information': [item for item in [
                aarecord['file_unified_data'].get('title_best', None) or '',
                aarecord['file_unified_data'].get('author_best', None) or '',
                (aarecord['file_unified_data'].get('stripped_description_best', None) or '')[0:100],
                aarecord['file_unified_data'].get('publisher_best', None) or '',
                aarecord['file_unified_data'].get('edition_varia_best', None) or '',
                aarecord['file_unified_data'].get('original_filename_best_name_only', None) or '',
            ] if item != ''],
        'cover_url': cover_url,
        'top_row': ", ".join([item for item in [
                additional['most_likely_language_name'],
                f".{aarecord['file_unified_data']['extension_best']}" if len(aarecord['file_unified_data']['extension_best']) > 0 else '',
                format_filesize(aarecord['file_unified_data'].get('filesize_best', None) or 0) if aarecord['file_unified_data'].get('filesize_best', None) else '',
                md5_content_type_mapping[aarecord['file_unified_data']['content_type']],
                (aarecord['file_unified_data'].get('original_filename_best_name_only', None) or '').rsplit('.', 1)[0],
                aarecord_id_split[1] if aarecord_id_split[0] in ['ia', 'ol'] else '',
                f"ISBNdb {aarecord_id_split[1]}" if aarecord_id_split[0] == 'isbn' else '',
                f"OCLC {aarecord_id_split[1]}" if aarecord_id_split[0] == 'oclc' else '',
                f"DuXiu SSID {aarecord_id_split[1]}" if aarecord_id_split[0] == 'duxiu_ssid' else '',
                f"CADAL SSNO {aarecord_id_split[1]}" if aarecord_id_split[0] == 'cadal_ssno' else '',
            ] if item != '']),
        'title': aarecord['file_unified_data'].get('title_best', None) or '',
        'publisher_and_edition': ", ".join([item for item in [
                aarecord['file_unified_data'].get('publisher_best', None) or '',
                aarecord['file_unified_data'].get('edition_varia_best', None) or '',
            ] if item != '']),
        'author': aarecord['file_unified_data'].get('author_best', None) or '',
        # TODO: Split out comments and style them better.
        'description': '\n\n'.join([item for item in [
                aarecord['file_unified_data'].get('stripped_description_best', None) or '',
                # TODO:TRANSLATE
                f"Metadata comments: {comments_multiple}" if (comments_multiple != '') else '',
            ] if item != ''])
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
    additional['torrent_paths'] = []
    shown_click_get = False
    linked_dois = set()

    torrents_json_aa_currently_seeding_by_torrent_path = allthethings.utils.get_torrents_json_aa_currently_seeding_by_torrent_path()

    temporarily_unavailable = gettext('page.md5.box.download.temporarily_unavailable') # Keeping translation

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
            additional['torrent_paths'].append([f"managed_by_aa/ia/annas-archive-ia-acsm-{directory}.tar.torrent"])
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
            additional['torrent_paths'].append([f"managed_by_aa/ia/annas-archive-ia-lcpdf-{directory}.tar.torrent"])
        elif ia_file_type == 'ia2_acsmpdf':
            partner_path = make_temp_anon_aac_path("o/ia2_acsmpdf_files", aarecord['ia_record']['aa_ia_file']['aacid'], aarecord['ia_record']['aa_ia_file']['data_folder'])
            additional['torrent_paths'].append([f"managed_by_aa/annas_archive_data__aacid/{aarecord['ia_record']['aa_ia_file']['data_folder']}.torrent"])
        else:
            raise Exception(f"Unknown ia_record file type: {ia_file_type}")
        add_partner_servers(partner_path, 'aa_exclusive', aarecord, additional)
    if (aarecord.get('duxiu') is not None) and (aarecord['duxiu'].get('duxiu_file') is not None):
        data_folder = aarecord['duxiu']['duxiu_file']['data_folder']
        # TODO: Add back when releasing DuXiu torrents.
        # additional['torrent_paths'].append([f"managed_by_aa/annas_archive_data__aacid/{data_folder}.torrent"])
        server = 'x'
        if data_folder <= 'annas_archive_data__aacid__duxiu_files__20240312T070549Z--20240312T070550Z':
            server = 'v'
        elif data_folder <= 'annas_archive_data__aacid__duxiu_files__20240312T105436Z--20240312T105437Z':
            server = 'w'
        partner_path = f"{server}/duxiu_files/20240312/{data_folder}/{aarecord['duxiu']['duxiu_file']['aacid']}"
        add_partner_servers(partner_path, 'aa_exclusive', aarecord, additional)
    if aarecord.get('lgrsnf_book') is not None:
        lgrsnf_thousands_dir = (aarecord['lgrsnf_book']['id'] // 1000) * 1000
        lgrsnf_torrent_path = f"external/libgen_rs_non_fic/r_{lgrsnf_thousands_dir:03}.torrent"
        if lgrsnf_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path:
            additional['torrent_paths'].append([lgrsnf_torrent_path])
            if torrents_json_aa_currently_seeding_by_torrent_path[lgrsnf_torrent_path]:
                lgrsnf_path = f"e/lgrsnf/{lgrsnf_thousands_dir}/{aarecord['lgrsnf_book']['md5'].lower()}"
                add_partner_servers(lgrsnf_path, '', aarecord, additional)

        additional['download_urls'].append((gettext('page.md5.box.download.lgrsnf'), f"http://library.lol/main/{aarecord['lgrsnf_book']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if aarecord.get('lgrsfic_book') is not None:
        lgrsfic_thousands_dir = (aarecord['lgrsfic_book']['id'] // 1000) * 1000
        lgrsfic_torrent_path = f"external/libgen_rs_fic/f_{lgrsfic_thousands_dir:03}.torrent"
        if lgrsfic_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path:
            additional['torrent_paths'].append([lgrsfic_torrent_path])
            if torrents_json_aa_currently_seeding_by_torrent_path[lgrsfic_torrent_path]:
                lgrsfic_path = f"e/lgrsfic/{lgrsfic_thousands_dir}/{aarecord['lgrsfic_book']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
                add_partner_servers(lgrsfic_path, '', aarecord, additional)

        additional['download_urls'].append((gettext('page.md5.box.download.lgrsfic'), f"http://library.lol/fiction/{aarecord['lgrsfic_book']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if aarecord.get('lgli_file') is not None:
        lglific_id = aarecord['lgli_file']['fiction_id']
        if lglific_id > 0:
            lglific_thousands_dir = (lglific_id // 1000) * 1000
            # Don't use torrents_json for this, because we have more files that don't get
            # torrented, because they overlap with our Z-Library torrents.
            # TODO: Verify overlap, and potentially add more torrents for what's missing?
            if lglific_thousands_dir >= 2201000 and lglific_thousands_dir <= 4259000:
                lglific_path = f"e/lglific/{lglific_thousands_dir}/{aarecord['lgli_file']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
                add_partner_servers(lglific_path, '', aarecord, additional)

            lglific_torrent_path = f"external/libgen_li_fic/f_{lglific_thousands_dir:03}.torrent"
            if lglific_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path:
                additional['torrent_paths'].append([lglific_torrent_path])

        scimag_id = aarecord['lgli_file']['scimag_id']
        if scimag_id > 0 and scimag_id <= 87599999: # 87637042 seems the max now in the libgenli db
            scimag_hundredthousand_dir = (scimag_id // 100000)
            scimag_torrent_path = f"external/scihub/sm_{scimag_hundredthousand_dir:03}00000-{scimag_hundredthousand_dir:03}99999.torrent"
            if scimag_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path:
                additional['torrent_paths'].append([scimag_torrent_path])

                if torrents_json_aa_currently_seeding_by_torrent_path[scimag_torrent_path]:
                    scimag_tenmillion_dir = (scimag_id // 10000000)
                    scimag_filename = urllib.parse.quote(aarecord['lgli_file']['scimag_archive_path'].replace('\\', '/'))
                    scimag_path = f"i/scimag/{scimag_tenmillion_dir}/{scimag_filename}"
                    add_partner_servers(scimag_path, 'scimag', aarecord, additional)

        lglicomics_id = aarecord['lgli_file']['comics_id']
        missing_ranges = [
            # Missing files (len(missing_nums)=6260 files):
            (840235, 840235), # (1)
            (840676, 840676), # (1)
            (840754, 840754), # (1)
            (875965, 875965), # (1)
            (1002491, 1002491), # (1)
            (1137603, 1137603), # (1)
            (1317000, 1317009), # (10)
            (1317011, 1317013), # (3)
            (1317017, 1317017), # (1)
            (1317019, 1317020), # (2)
            (1317022, 1317022), # (1)
            (1317024, 1317024), # (1)
            (1317028, 1317028), # (1)
            (1317031, 1317035), # (5)
            (1317037, 1317039), # (3)
            (1317041, 1317042), # (2)
            (1317044, 1317077), # (34)
            (1317079, 1317083), # (5)
            (1317085, 1317087), # (3)
            (1317090, 1317092), # (3)
            (1317096, 1317096), # (1)
            (1317104, 1317104), # (1)
            (1317106, 1317129), # (24)
            (1317131, 1317160), # (30)
            (1317162, 1317166), # (5)
            (1317168, 1317172), # (5)
            (1317177, 1317182), # (6)
            (1317184, 1317204), # (21)
            (1317209, 1317432), # (224)
            (1317434, 1317460), # (27)
            (1317462, 1317494), # (33)
            (1317496, 1317496), # (1)
            (1317499, 1317503), # (5)
            (1317505, 1317508), # (4)
            (1317510, 1317510), # (1)
            (1317512, 1317547), # (36)
            (1317549, 1317595), # (47)
            (1317597, 1317600), # (4)
            (1317602, 1317630), # (29)
            (1377196, 1377261), # (66)
            (1377264, 1377264), # (1)
            (1377273, 1377273), # (1)
            (1377701, 1377729), # (29)
            (1377776, 1377781), # (6)
            (1384625, 1384625), # (1)
            (1386458, 1386463), # (6)
            (1386465, 1386468), # (4)
            (1394013, 1394024), # (12)
            (1395318, 1395349), # (32)
            (1395351, 1395353), # (3)
            (1395389, 1395395), # (7)
            (1395402, 1395893), # (492)
            (1395901, 1396803), # (903)
            (1396830, 1396837), # (8)
            (1396847, 1397764), # (918)
            (1397801, 1397851), # (51)
            (1397898, 1397908), # (11)
            (1397961, 1397968), # (8)
            (1397984, 1399341), # (1358)
            (1399382, 1399471), # (90)
            (1399473, 1400491), # (1019)
            (1400493, 1400792), # (300)
            (1401572, 1401631), # (60)
            (1401643, 1401645), # (3)
            (1401655, 1401727), # (73)
            (1401742, 1401928), # (187)
            (1409447, 1409447), # (1)
            (1435415, 1435415), # (1)
            (1537056, 1537056), # (1)
            (1572053, 1572053), # (1)
            (1589229, 1589229), # (1)
            (1596172, 1596172), # (1)
            (1799256, 1799256), # (1)
            (1948998, 1948998), # (1)
            (1995329, 1995329), # (1)
            (2145511, 2145511), # (1)
            (2145628, 2145628), # (1)
            (2145689, 2145689), # (1)
            (2165899, 2165899), # (1)
            (2230639, 2230639), # (1)
            (2245466, 2245466), # (1)
            (2320395, 2320395), # (1)
            (2369229, 2369230), # (2)
            (2374217, 2374217), # (1)
            (2439649, 2439649), # (1)
            (2450484, 2450484), # (1)
            (2474293, 2474293), # (1)
            (2474297, 2474297), # (1)
            (2476920, 2476920), # (1)
            (2495587, 2495587), # (1)
            (2511592, 2511592), # (1)
            (2519421, 2519421), # (1)
            # Magz files (len(magz_nums)=2969 files):
            (137, 137), (24531, 24531), (24533, 24534), (24538, 24538), (24619, 24619), (24621, 24621), (24623, 24623), (24626, 24626), (24628, 24630), (24632, 24637), (24639, 24647), (24649, 24652), (24654, 24655), (24657, 24664), (24667, 24672), (24674, 24678), (24680, 24680), (24683, 24684), (24686, 24691), (24693, 24693), (24695, 24701), (24704, 24715), (24724, 24727), (24729, 24729), (24731, 24733), (24735, 24736), (24739, 24740), (24742, 24743), (24745, 24746), (24748, 24749), (24751, 24751), (24753, 24762), (24764, 24764), (24766, 24770), (24772, 24783), (24785, 24786), (24788, 24794), (24796, 24796), (24798, 24798), (24800, 24802), (24805, 24807), (24811, 24811), (24813, 24816), (24818, 24818), (24822, 24824), (24827, 24827), (24829, 24829), (24831, 24861), (24872, 24880), (24883, 24884), (24886, 24893), (24896, 24897), (24899, 24901), (24903, 24903), (24906, 24906), (24910, 24913), (24915, 24919), (24921, 24921), (24923, 24923), (24926, 24926), (24928, 24928), (24930, 24934), (24936, 24939), (47562, 47562), (271028, 271028), (271030, 271030), (271032, 271032), (271058, 271059), (271061, 271063), (271146, 271147), (271180, 271183), (339850, 339850), (362441, 362442), (386860, 386860), (448825, 448825), (448843, 448846), (448848, 448854), (547537, 547537), (547541, 547541), (547601, 547601), (547606, 547606), (547613, 547613), (547633, 547633), (547664, 547664), (547890, 547894), (547899, 547900), (547902, 547903), (547907, 547907), (547911, 547911), (547913, 547914), (547920, 547920), (547924, 547925), (547927, 547931), (547933, 547952), (547954, 547959), (547961, 547962), (547964, 547968), (547970, 547974), (547976, 547977), (547979, 547982), (547985, 548000), (548002, 548010), (548012, 548020), (548022, 548051), (548053, 548068), (548070, 548072), (548074, 548076), (548078, 548079), (548081, 548088), (548090, 548118), (548120, 548120), (548123, 548124), (571154, 571154), (571156, 571156), (571205, 571205), (579585, 579585), (587509, 587511), (587513, 587516), (587518, 587519), (587521, 587521), (587523, 587523), (587525, 587529), (587531, 587532), (587536, 587543), (587545, 587545), (587547, 587550), (587552, 587552), (587554, 587555), (587557, 587562), (587565, 587566), (587568, 587568), (587572, 587572), (587575, 587580), (587583, 587584), (587586, 587586), (587588, 587588), (587592, 587596), (587598, 587602), (587604, 587605), (587608, 587608), (587611, 587611), (587613, 587613), (587617, 587621), (587625, 587625), (587628, 587633), (587636, 587636), (587641, 587643), (587645, 587647), (590316, 590316), (604588, 604589), (604591, 604594), (604596, 604596), (607244, 607245), (607247, 607247), (607250, 607250), (607252, 607252), (607254, 607254), (607256, 607256), (607259, 607259), (607261, 607261), (627085, 627086), (627091, 627092), (627095, 627095), (627104, 627105), (627108, 627108), (633361, 633361), (645627, 645627), (646238, 646241), (648501, 648513), (648515, 648522), (651344, 651346), (654003, 654005), (654007, 654009), (654011, 654011), (654281, 654281), (654283, 654296), (654298, 654299), (654304, 654304), (654306, 654306), (654317, 654317), (654319, 654319), (654328, 654329), (654335, 654340), (654344, 654345), (654347, 654348), (686837, 686837), (686843, 686843), (686845, 686845), (686848, 686848), (686852, 686852), (686854, 686854), (686857, 686857), (686860, 686860), (686864, 686864), (686867, 686867), (686870, 686870), (686873, 686873), (686876, 686876), (686879, 686879), (686883, 686883), (686886, 686886), (686888, 686888), (686892, 686892), (686894, 686894), (686897, 686897), (686900, 686900), (686903, 686903), (686906, 686906), (686909, 686909), (686911, 686911), (686913, 686913), (686915, 686915), (686917, 686917), (686919, 686919), (686921, 686921), (686923, 686923), (686926, 686926), (686929, 686929), (686931, 686931), (686933, 686933), (686935, 686935), (686937, 686937), (686939, 686939), (686941, 686941), (686943, 686943), (686945, 686945), (686947, 686947), (686949, 686949), (686951, 686951), (686953, 686961), (686963, 686964), (686967, 686967), (686969, 686974), (686976, 686976), (686978, 686980), (686982, 686992), (686994, 686995), (686997, 686998), (687001, 687001), (756692, 756692), (756699, 756699), (756701, 756701), (756708, 756709), (756711, 756711), (756719, 756720), (756732, 756732), (756735, 756735), (801556, 801556), (802822, 802822), (809853, 809853), (825351, 825351), (829738, 829753), (829755, 829768), (829770, 829773), (829775, 829776), (829778, 829785), (829788, 829854), (829856, 829871), (829873, 829890), (829892, 829892), (829894, 829919), (829921, 829924), (829926, 829965), (829967, 829970), (829972, 829996), (829999, 829999), (830001, 830002), (830005, 830034), (830036, 830044), (830046, 830053), (830055, 830080), (830084, 830084), (830172, 830172), (830174, 830174), (830176, 830176), (830178, 830192), (830195, 830196), (830198, 830200), (830205, 830205), (830208, 830209), (830213, 830213), (830216, 830216), (830218, 830218), (830221, 830221), (830224, 830224), (830228, 830229), (830233, 830233), (830235, 830235), (830238, 830238), (830243, 830243), (830248, 830248), (830250, 830250), (830256, 830256), (830258, 830258), (830261, 830261), (830268, 830268), (831594, 831594), (834440, 834443), (835014, 835014), (835156, 835156), (835347, 835347), (835394, 835394), (835511, 835511), (835944, 835944), (836035, 836035), (836041, 836041), (836102, 836102), (836509, 836509), (836854, 836854), (837120, 837120), (837163, 837163), (837315, 837315), (837380, 837380), (837456, 837456), (837580, 837580), (838557, 838557), (838953, 838953), (838998, 838998), (839101, 839101), (839582, 839582), (839688, 839688), (839732, 839732), (840030, 840030), (840037, 840037), (840258, 840258), (840360, 840360), (840452, 840452), (840876, 840876), (841062, 841062), (841385, 841385), (841464, 841464), (841521, 841521), (841664, 841664), (841705, 841705), (841754, 841754), (841921, 841921), (841989, 841989), (842050, 842050), (842232, 842232), (842367, 842367), (842505, 842505), (842616, 842616), (842851, 842851), (842880, 842880), (842917, 842917), (842959, 842959), (843154, 843154), (843156, 843156), (843213, 843213), (843482, 843482), (844229, 844229), (844292, 844292), (844622, 844622), (845111, 845111), (845565, 845565), (845607, 845607), (846129, 846129), (846303, 846303), (847087, 847087), (847390, 847390), (847397, 847397), (847631, 847631), (847924, 847924), (847926, 847926), (847970, 847970), (848096, 848096), (848209, 848209), (848330, 848330), (848869, 848869), (848883, 848883), (848890, 848890), (849112, 849112), (849367, 849367), (849447, 849447), (849556, 849556), (849606, 849606), (849717, 849717), (850020, 850020), (850079, 850079), (850246, 850246), (850616, 850616), (851038, 851038), (851138, 851138), (851258, 851258), (851278, 851278), (851466, 851466), (851915, 851915), (852082, 852082), (852158, 852158), (852241, 852241), (852867, 852867), (852880, 852880), (852933, 852933), (853068, 853068), (853287, 853287), (853329, 853329), (853477, 853477), (853864, 853864), (854034, 854034), (854069, 854069), (854096, 854096), (854125, 854125), (854195, 854195), (854307, 854307), (854704, 854704), (854737, 854737), (855344, 855344), (855505, 855505), (855703, 855703), (856097, 856097), (856562, 856562), (856996, 856996), (858749, 858749), (858831, 858831), (858874, 858874), (859247, 859247), (859409, 859409), (859426, 859426), (859731, 859731), (860405, 860405), (860873, 860873), (860947, 860947), (861191, 861191), (861211, 861211), (861518, 861518), (861619, 861619), (861744, 861744), (861790, 861790), (862015, 862015), (862046, 862046), (862058, 862058), (862254, 862254), (862291, 862291), (862564, 862564), (862738, 862738), (862753, 862753), (862832, 862832), (862970, 862970), (863150, 863150), (863274, 863274), (863433, 863433), (863834, 863834), (863912, 863912), (863984, 863984), (864302, 864302), (864742, 864742), (864863, 864863), (864956, 864956), (865177, 865177), (865405, 865405), (865441, 865441), (865588, 865588), (865812, 865812), (866030, 866030), (866142, 866142), (866355, 866355), (866544, 866544), (866597, 866597), (866948, 866948), (867166, 867166), (867188, 867188), (867271, 867271), (867528, 867528), (867629, 867629), (867864, 867864), (867969, 867969), (868352, 868352), (868536, 868536), (868637, 868637), (868738, 868738), (868881, 868881), (869078, 869078), (869251, 869251), (869624, 869624), (869816, 869816), (870195, 870195), (870304, 870304), (870339, 870339), (870642, 870642), (870749, 870749), (871002, 871002), (871147, 871147), (871283, 871283), (871351, 871351), (871387, 871387), (871520, 871520), (871624, 871624), (871708, 871708), (871925, 871925), (872257, 872257), (872438, 872438), (872735, 872735), (872809, 872809), (873416, 873416), (873608, 873608), (874153, 874153), (874785, 874785), (874964, 874964), (875115, 875115), (875531, 875531), (875984, 875984), (876199, 876199), (876360, 876360), (876461, 876461), (876463, 876463), (876502, 876502), (876523, 876523), (876723, 876723), (876828, 876828), (877030, 877030), (877117, 877117), (877450, 877450), (877460, 877460), (878019, 878019), (878287, 878287), (878339, 878339), (878370, 878370), (878443, 878443), (878845, 878845), (879341, 879341), (879417, 879417), (879473, 879473), (879788, 879788), (880052, 880052), (880105, 880105), (880420, 880420), (880607, 880607), (880920, 880920), (881299, 881299), (881428, 881428), (881434, 881434), (881623, 881623), (882316, 882316), (882489, 882489), (882559, 882559), (882657, 882657), (882819, 882819), (882905, 882905), (882916, 882916), (883188, 883188), (883270, 883270), (883314, 883314), (883324, 883324), (883581, 883581), (883592, 883592), (883720, 883720), (883909, 883909), (884678, 884678), (884778, 884778), (884817, 884817), (885618, 885618), (885634, 885634), (886980, 886980), (887571, 887571), (887659, 887659), (887871, 887871), (888263, 888263), (888283, 888283), (888441, 888441), (888753, 888753), (889233, 889233), (889429, 889429), (889674, 889674), (889924, 889924), (889949, 889949), (890374, 890374), (890577, 890577), (890642, 890642), (890667, 890667), (890734, 890734), (890943, 890943), (891066, 891066), (891128, 891128), (891288, 891288), (891970, 891970), (892175, 892175), (892381, 892381), (892466, 892466), (893400, 893400), (893691, 893691), (894025, 894025), (894103, 894103), (894270, 894270), (894437, 894437), (894974, 894974), (895141, 895141), (895369, 895369), (895692, 895692), (895884, 895884), (896201, 896201), (896386, 896386), (897142, 897142), (897155, 897155), (897283, 897283), (897330, 897330), (897503, 897503), (897580, 897580), (898034, 898034), (898102, 898102), (898125, 898125), (898307, 898307), (898618, 898618), (898709, 898709), (898736, 898736), (898754, 898754), (898862, 898862), (899056, 899056), (899201, 899201), (899664, 899664), (899698, 899698), (899781, 899781), (899970, 899970), (900022, 900022), (900166, 900166), (900269, 900269), (900790, 900790), (900980, 900980), (901350, 901350), (901437, 901437), (901496, 901496), (901948, 901948), (902070, 902070), (902187, 902187), (902534, 902534), (902682, 902682), (902743, 902743), (902854, 902854), (903175, 903175), (903260, 903260), (903380, 903380), (903518, 903518), (903863, 903863), (903972, 903972), (904139, 904139), (904216, 904216), (904297, 904297), (904483, 904483), (904859, 904859), (905078, 905078), (905360, 905360), (905372, 905372), (905382, 905382), (905474, 905474), (905539, 905539), (905600, 905600), (905713, 905713), (905719, 905720), (906235, 906235), (906480, 906480), (906522, 906522), (906656, 906656), (906676, 906676), (906824, 906824), (907010, 907010), (907103, 907103), (907166, 907166), (907369, 907369), (907791, 907791), (907896, 907896), (907907, 907907), (907911, 907911), (907933, 907933), (907965, 907965), (908289, 908289), (908786, 908786), (908797, 908797), (908869, 908869), (909074, 909074), (909196, 909196), (909493, 909493), (909543, 909543), (909627, 909627), (909865, 909865), (909941, 909941), (910150, 910150), (910335, 910335), (910409, 910409), (910502, 910502), (910621, 910621), (910738, 910738), (910740, 910740), (911149, 911149), (911187, 911187), (911351, 911351), (911419, 911419), (912172, 912172), (912697, 912697), (912808, 912808), (912885, 912885), (913024, 913024), (913323, 913323), (913365, 913365), (913450, 913450), (913532, 913532), (913745, 913745), (913776, 913776), (913836, 913836), (914008, 914008), (914034, 914034), (914090, 914090), (914136, 914136), (914193, 914193), (914200, 914200), (914459, 914459), (914644, 914644), (914676, 914676), (914785, 914785), (915009, 915009), (915050, 915050), (915453, 915453), (915558, 915558), (915793, 915793), (915990, 915990), (916056, 916056), (916104, 916104), (916130, 916130), (916527, 916527), (917088, 917088), (918144, 918144), (918316, 918316), (918405, 918405), (918517, 918517), (918555, 918555), (918690, 918690), (918943, 918943), (918981, 918981), (919051, 919051), (919266, 919266), (919375, 919375), (919401, 919401), (919788, 919788), (919933, 919933), (920094, 920094), (920184, 920184), (920316, 920316), (920742, 920742), (920862, 920862), (921012, 921012), (921017, 921017), (921157, 921157), (921266, 921266), (921464, 921464), (921653, 921653), (921674, 921674), (921699, 921699), (922103, 922103), (922201, 922201), (922522, 922522), (922780, 922780), (922811, 922811), (922938, 922938), (922948, 922948), (923823, 923823), (924103, 924103), (924311, 924311), (924717, 924717), (924925, 924925), (924971, 924971), (925144, 925144), (925287, 925287), (925302, 925302), (925547, 925547), (925567, 925567), (925888, 925888), (925965, 925965), (926621, 926621), (926657, 926657), (926822, 926822), (926971, 926971), (927441, 927441), (982998, 982998), (989034, 989034), (990029, 990029), (990048, 990048), (990540, 990540), (990553, 990553), (990556, 990559), (993032, 993032), (998551, 998551), (999436, 999436), (1000081, 1000081), (1000088, 1000088), (1003693, 1003693), (1013485, 1013486), (1013492, 1013492), (1013496, 1013498), (1013509, 1013510), (1013519, 1013520), (1013523, 1013526), (1020274, 1020274), (1020276, 1020279), (1020281, 1020281), (1023255, 1023255), (1025618, 1025618), (1028154, 1028156), (1028158, 1028158), (1028171, 1028171), (1031468, 1031469), (1033341, 1033342), (1033799, 1033799), (1033824, 1033824), (1033834, 1033834), (1034067, 1034068), (1034595, 1034595), (1039355, 1039355), (1042096, 1042104), (1045874, 1045875), (1046863, 1046863), (1046866, 1046868), (1046872, 1046873), (1046875, 1046876), (1046879, 1046880), (1046883, 1046883), (1046885, 1046889), (1046891, 1046891), (1047108, 1047109), (1047112, 1047112), (1047114, 1047114), (1047217, 1047217), (1047223, 1047223), (1047232, 1047233), (1047235, 1047235), (1047245, 1047245), (1047253, 1047253), (1047262, 1047262), (1047264, 1047264), (1047279, 1047279), (1047287, 1047287), (1047293, 1047294), (1047362, 1047362), (1047438, 1047438), (1047505, 1047505), (1047507, 1047507), (1047509, 1047509), (1052988, 1052988), (1056093, 1056093), (1056456, 1056456), (1056537, 1056537), (1056539, 1056539), (1056541, 1056541), (1056543, 1056544), (1056546, 1056546), (1056548, 1056548), (1056550, 1056579), (1057520, 1057521), (1057524, 1057524), (1057526, 1057528), (1057535, 1057536), (1057538, 1057538), (1057540, 1057540), (1057542, 1057543), (1057547, 1057547), (1057550, 1057550), (1057552, 1057552), (1057558, 1057559), (1057562, 1057563), (1057567, 1057569), (1057573, 1057574), (1057576, 1057576), (1057580, 1057582), (1057618, 1057622), (1058045, 1058045), (1058927, 1058928), (1059892, 1059892), (1064872, 1064872), (1067491, 1067491), (1071454, 1071454), (1082225, 1082225), (1082227, 1082227), (1082504, 1082504), (1083242, 1083244), (1089334, 1089335), (1091735, 1091735), (1098981, 1098981), (1100494, 1100495), (1109444, 1109444), (1109464, 1109464), (1109552, 1109554), (1109574, 1109575), (1109661, 1109666), (1109671, 1109675), (1112618, 1112621), (1112630, 1112631), (1116542, 1116543), (1117102, 1117111), (1117409, 1117409), (1118286, 1118286), (1118289, 1118289), (1118293, 1118293), (1118296, 1118296), (1118315, 1118315), (1118317, 1118317), (1118319, 1118319), (1118322, 1118322), (1118324, 1118324), (1118326, 1118331), (1118333, 1118333), (1118335, 1118335), (1118337, 1118337), (1118339, 1118340), (1118342, 1118343), (1118347, 1118348), (1118350, 1118351), (1118353, 1118353), (1118355, 1118355), (1118357, 1118357), (1118359, 1118359), (1118361, 1118361), (1118363, 1118367), (1118608, 1118608), (1125185, 1125186), (1126966, 1126966), (1126974, 1126975), (1133180, 1133180), (1134125, 1134128), (1134955, 1134956), (1134958, 1134958), (1135778, 1135778), (1138000, 1138000), (1138392, 1138392), (1145682, 1145682), (1145685, 1145685), (1145719, 1145719), (1145725, 1145726), (1145728, 1145728), (1145737, 1145737), (1145742, 1145743), (1146445, 1146445), (1146603, 1146603), (1148017, 1148019), (1148233, 1148257), (1149383, 1149383), (1150179, 1150179), (1150322, 1150322), (1153500, 1153500), (1153576, 1153576), (1162675, 1162675), (1166518, 1166519), (1167333, 1167333), (1167536, 1167536), (1169555, 1169555), (1170220, 1170220), (1170457, 1170457), (1171852, 1171873), (1171909, 1171909), (1173096, 1173097), (1173118, 1173118), (1173121, 1173121), (1173123, 1173123), (1173153, 1173163), (1178976, 1178977), (1179521, 1179521), (1179523, 1179526), (1179945, 1179945), (1180177, 1180177), (1201365, 1201365), (1201367, 1201367), (1201369, 1201369), (1230903, 1230905), (1230907, 1230907), (1230910, 1230910), (1230915, 1230915), (1230922, 1230923), (1230925, 1230931), (1230935, 1230937), (1230939, 1230949), (1230951, 1230951), (1230953, 1230954), (1230956, 1230959), (1230965, 1230965), (1230969, 1230974), (1230979, 1230979), (1230988, 1230993), (1230995, 1230999), (1231003, 1231003), (1231005, 1231005), (1231007, 1231011), (1231013, 1231013), (1231015, 1231016), (1231019, 1231022), (1231024, 1231024), (1231028, 1231029), (1231031, 1231031), (1231033, 1231034), (1231036, 1231038), (1231040, 1231049), (1231051, 1231053), (1231061, 1231067), (1231070, 1231077), (1231079, 1231101), (1231104, 1231104), (1231108, 1231117), (1231119, 1231122), (1231124, 1231125), (1231127, 1231127), (1231129, 1231129), (1231134, 1231134), (1231143, 1231144), (1231169, 1231171), (1256272, 1256272), (1274863, 1274863), (1274877, 1274878), (1277584, 1277584), (1278371, 1278372), (1278673, 1278679), (1278681, 1278681), (1278684, 1278685), (1278687, 1278687), (1278689, 1278689), (1278693, 1278694), (1278696, 1278696), (1278698, 1278698), (1278700, 1278701), (1278703, 1278703), (1278707, 1278707), (1278709, 1278709), (1278711, 1278711), (1278713, 1278713), (1278721, 1278723), (1278730, 1278734), (1278736, 1278743), (1278745, 1278745), (1278750, 1278750), (1282766, 1282766), (1282769, 1282769), (1282772, 1282772), (1282775, 1282775), (1282777, 1282777), (1282780, 1282780), (1282783, 1282783), (1282785, 1282785), (1282787, 1282787), (1282789, 1282789), (1282791, 1282791), (1282793, 1282793), (1282795, 1282796), (1282798, 1282798), (1282800, 1282800), (1282802, 1282802), (1282804, 1282804), (1282806, 1282806), (1282809, 1282809), (1282811, 1282811), (1282813, 1282813), (1282815, 1282815), (1282817, 1282817), (1282819, 1282819), (1282821, 1282821), (1282823, 1282823), (1282825, 1282825), (1282827, 1282827), (1282829, 1282829), (1282831, 1282831), (1282833, 1282833), (1282835, 1282835), (1282837, 1282837), (1282839, 1282839), (1282841, 1282841), (1282843, 1282843), (1282845, 1282845), (1282847, 1282847), (1282849, 1282849), (1282851, 1282851), (1282853, 1282853), (1282855, 1282855), (1282857, 1282857), (1282859, 1282859), (1282861, 1282861), (1282863, 1282863), (1282865, 1282865), (1282867, 1282867), (1282869, 1282869), (1282871, 1282871), (1282873, 1282873), (1282875, 1282875), (1282877, 1282877), (1282879, 1282879), (1282881, 1282881), (1282883, 1282883), (1282885, 1282885), (1282887, 1282887), (1282889, 1282889), (1282891, 1282891), (1282893, 1282893), (1282895, 1282895), (1282897, 1282897), (1282899, 1282899), (1282901, 1282901), (1282903, 1282903), (1282905, 1282905), (1282907, 1282907), (1282909, 1282909), (1282911, 1282911), (1282913, 1282913), (1282915, 1282915), (1282920, 1282920), (1282923, 1282923), (1282925, 1282925), (1282928, 1282928), (1282931, 1282931), (1282934, 1282934), (1282938, 1282938), (1282940, 1282940), (1282943, 1282943), (1282945, 1282945), (1282948, 1282948), (1282951, 1282951), (1282954, 1282954), (1282957, 1282957), (1282960, 1282960), (1282964, 1282964), (1283279, 1283279), (1283282, 1283282), (1285822, 1285840), (1286259, 1286259), (1286987, 1286987), (1287053, 1287053), (1287057, 1287057), (1287462, 1287516), (1287598, 1287600), (1287602, 1287618), (1287620, 1287622), (1287625, 1287625), (1287628, 1287635), (1287639, 1287639), (1287641, 1287641), (1287643, 1287645), (1287647, 1287647), (1287650, 1287658), (1287661, 1287673), (1287675, 1287678), (1287911, 1287911), (1287935, 1287935), (1287944, 1287944), (1287964, 1287964), (1288031, 1288031), (1288041, 1288041), (1288049, 1288049), (1288066, 1288066), (1288973, 1288998), (1289000, 1289005), (1289008, 1289010), (1289013, 1289015), (1289021, 1289021), (1289027, 1289029), (1289032, 1289032), (1289035, 1289035), (1289143, 1289144), (1289151, 1289151), (1289153, 1289157), (1289162, 1289162), (1289193, 1289193), (1289199, 1289199), (1289248, 1289248), (1289251, 1289252), (1289258, 1289258), (1289260, 1289260), (1289262, 1289263), (1289267, 1289268), (1289271, 1289278), (1289280, 1289280), (1289283, 1289284), (1289287, 1289287), (1289292, 1289292), (1289298, 1289298), (1289302, 1289303), (1289305, 1289305), (1289312, 1289313), (1289315, 1289316), (1289318, 1289318), (1289321, 1289321), (1289326, 1289326), (1289332, 1289332), (1289334, 1289334), (1289341, 1289341), (1289353, 1289353), (1289371, 1289371), (1289380, 1289382), (1289385, 1289385), (1289387, 1289387), (1289389, 1289389), (1289393, 1289394), (1289399, 1289399), (1289402, 1289402), (1289405, 1289405), (1289407, 1289413), (1289429, 1289429), (1289432, 1289432), (1289434, 1289434), (1289450, 1289450), (1289464, 1289464), (1289480, 1289481), (1289483, 1289483), (1289485, 1289490), (1289493, 1289498), (1289500, 1289506), (1289508, 1289510), (1289512, 1289514), (1289516, 1289526), (1290086, 1290086), (1290237, 1290237), (1290239, 1290239), (1290241, 1290241), (1290255, 1290255), (1290257, 1290258), (1290260, 1290260), (1290267, 1290268), (1290272, 1290274), (1290379, 1290380), (1290382, 1290382), (1290384, 1290384), (1290386, 1290387), (1290389, 1290389), (1290391, 1290391), (1290393, 1290396), (1290412, 1290414), (1290425, 1290425), (1290427, 1290438), (1290440, 1290456), (1290464, 1290464), (1290657, 1290657), (1290659, 1290659), (1290661, 1290661), (1290664, 1290664), (1290666, 1290666), (1290668, 1290668), (1290670, 1290671), (1290676, 1290676), (1290678, 1290680), (1290683, 1290683), (1290685, 1290690), (1290692, 1290693), (1290696, 1290696), (1290698, 1290698), (1290700, 1290700), (1290702, 1290702), (1290704, 1290704), (1290709, 1290709), (1290720, 1290722), (1290727, 1290728), (1290730, 1290730), (1290732, 1290732), (1290734, 1290735), (1290737, 1290738), (1290740, 1290741), (1290743, 1290743), (1290746, 1290747), (1290749, 1290749), (1290751, 1290751), (1290754, 1290756), (1290758, 1290762), (1290764, 1290765), (1290767, 1290767), (1290770, 1290771), (1290774, 1290775), (1290777, 1290777), (1290779, 1290781), (1290783, 1290788), (1290790, 1290802), (1290805, 1290806), (1290808, 1290819), (1290821, 1290822), (1290824, 1290829), (1290836, 1290843), (1290846, 1290848), (1290850, 1290855), (1290869, 1290870), (1290881, 1290881), (1290883, 1290883), (1290886, 1290887), (1290890, 1290890), (1290902, 1290902), (1404642, 1404642), (1405296, 1405296), (1428792, 1428792), (1428953, 1428953), (1428982, 1428983), (1428985, 1428988), (1428990, 1428992), (1428994, 1428996), (1429007, 1429007), (1429025, 1429026), (1429029, 1429030), (1429033, 1429036), (1429106, 1429107), (1429117, 1429117), (1429176, 1429176), (1430898, 1430899), (1430928, 1430928), (1431087, 1431087), (1436737, 1436738), (1436740, 1436740), (1436742, 1436748), (1436750, 1436755), (1436757, 1436763), (1436765, 1436768), (1436772, 1436778), (1436780, 1436781), (1436783, 1436784), (1436786, 1436791), (1436794, 1436794), (1436799, 1436799), (1436808, 1436808), (1436819, 1436820), (1436822, 1436828), (1442426, 1442426), (1443023, 1443023), (1450032, 1450032), (1450034, 1450034), (1542724, 1542724), (1595863, 1595864), (1615874, 1615874), (1615877, 1615878), (1615880, 1615885), (1615888, 1615889), (1615891, 1615901), (1617864, 1617865), (1625555, 1625556), (1625560, 1625562), (1625566, 1625566), (1625570, 1625573), (1625576, 1625577), (1734253, 1734274), (1734276, 1734277), (1781585, 1781585), (1832677, 1832704), (1832713, 1832714), (1832720, 1832725), (1836276, 1836276), (1867217, 1867217), (1880927, 1880927), (1880935, 1880935), (1979663, 1979666), 
        ]
        if lglicomics_id > 0 and lglicomics_id < 2541000:
            comics_file_present = True
            for missing_range in missing_ranges:
                if lglicomics_id >= missing_range[0] and lglicomics_id <= missing_range[1]:
                    comics_file_present = False
                    break
            if comics_file_present:
                lglicomics_thousands_dir = (lglicomics_id // 1000) * 1000
                lglicomics_path = f"a/working/comics_new_layout/{lglicomics_thousands_dir}/{aarecord['lgli_file']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
                add_partner_servers(lglicomics_path, '', aarecord, additional)

                # TODO: Bring back.
                # additional['torrent_paths'].append([f"managed_by_aa/annas_archive_data__aacid/c_2022_12_thousand_dirs.torrent"])

        lglimagz_id = aarecord['lgli_file']['magz_id']
        if lglimagz_id > 0 and lglimagz_id < 1363000:
            lglimagz_thousands_dir = (lglimagz_id // 1000) * 1000
            lglimagz_path = f"y/magz/{lglimagz_thousands_dir}/{aarecord['lgli_file']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
            add_partner_servers(lglimagz_path, '', aarecord, additional)

            # TODO: Bring back.
            # additional['torrent_paths'].append([f"managed_by_aa/annas_archive_data__aacid/c_2022_12_thousand_dirs.torrent"])

        additional['download_urls'].append((gettext('page.md5.box.download.lgli'), f"http://libgen.li/ads.php?md5={aarecord['lgli_file']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if len(aarecord.get('ipfs_infos') or []) > 0:
        additional['download_urls'].append((gettext('page.md5.box.download.ipfs_gateway', num=1), f"https://cloudflare-ipfs.com/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename']}", gettext('page.md5.box.download.ipfs_gateway_extra')))
        additional['download_urls'].append((gettext('page.md5.box.download.ipfs_gateway', num=2), f"https://ipfs.io/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename']}", ""))
        additional['download_urls'].append((gettext('page.md5.box.download.ipfs_gateway', num=3), f"https://gateway.pinata.cloud/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename']}", ""))
    if aarecord.get('zlib_book') is not None and len(aarecord['zlib_book']['pilimi_torrent'] or '') > 0:
        zlib_path = make_temp_anon_zlib_path(aarecord['zlib_book']['zlibrary_id'], aarecord['zlib_book']['pilimi_torrent'])
        add_partner_servers(zlib_path, 'aa_exclusive' if (len(additional['fast_partner_urls']) == 0) else '', aarecord, additional)
        additional['torrent_paths'].append([f"managed_by_aa/zlib/{aarecord['zlib_book']['pilimi_torrent']}"])
    if (aarecord.get('aac_zlib3_book') is not None) and (aarecord['aac_zlib3_book']['file_aacid'] is not None):
        zlib_path = make_temp_anon_aac_path("o/zlib3_files", aarecord['aac_zlib3_book']['file_aacid'], aarecord['aac_zlib3_book']['file_data_folder'])
        add_partner_servers(zlib_path, 'aa_exclusive' if (len(additional['fast_partner_urls']) == 0) else '', aarecord, additional)
        additional['torrent_paths'].append([f"managed_by_aa/annas_archive_data__aacid/{aarecord['aac_zlib3_book']['file_data_folder']}.torrent"])
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
        for torrent_paths in additional['torrent_paths']:
            # path = "/torrents"
            # group = torrent_group_data_from_file_path(f"torrents/{torrent_paths[0]}")['group']
            # path += f"#{group}"
            files_html = " or ".join([f'<a href="/dyn/small_file/torrents/{torrent_path}">file</a>' for torrent_path in torrent_paths])
            additional['download_urls'].append((gettext('page.md5.box.download.bulk_torrents'), "/datasets", gettext('page.md5.box.download.experts_only') + f' <span class="text-sm text-gray-500">{files_html}</em></span>'))
        if len(additional['torrent_paths']) == 0:
            if additional['has_aa_downloads'] == 0:
                additional['download_urls'].append(("", "", 'Bulk torrents not yet available for this file. If you have this file, help out by <a href="/account/upload">uploading</a>.'))
            else:
                additional['download_urls'].append(("", "", 'Bulk torrents not yet available for this file.'))
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
        additional['download_urls'].append((gettext('page.md5.box.download.aa_oclc'), f'/search?q="oclc:{aarecord_id_split[1]}"', ""))
        additional['download_urls'].append((gettext('page.md5.box.download.original_oclc'), f"https://worldcat.org/title/{aarecord_id_split[1]}", ""))
    if aarecord_id_split[0] == 'duxiu_ssid':
        additional['download_urls'].append((gettext('page.md5.box.download.aa_duxiu'), f'/search?q="duxiu_ssid:{aarecord_id_split[1]}"', ""))
        additional['download_urls'].append((gettext('page.md5.box.download.original_duxiu'), f'https://www.duxiu.com/bottom/about.html', ""))
    if aarecord_id_split[0] == 'cadal_ssno':
        additional['download_urls'].append((gettext('page.md5.box.download.aa_cadal'), f'/search?q="cadal_ssno:{aarecord_id_split[1]}"', ""))
        additional['download_urls'].append((gettext('page.md5.box.download.original_cadal'), f'https://cadal.edu.cn/cardpage/bookCardPage?ssno={aarecord_id_split[1]}', ""))
    if aarecord_id_split[0] in ['duxiu_ssid', 'cadal_ssno']:
        if 'duxiu_dxid' in aarecord['file_unified_data']['identifiers_unified']:
            for duxiu_dxid in aarecord['file_unified_data']['identifiers_unified']['duxiu_dxid']:
                additional['download_urls'].append((gettext('page.md5.box.download.aa_dxid'), f'/search?q="duxiu_dxid:{duxiu_dxid}"', ""))
        # Not supported by BaiduYun anymore.
        # if aarecord.get('duxiu') is not None and len(aarecord['duxiu']['aa_duxiu_derived']['miaochuan_links_multiple']) > 0:
        #     for miaochuan_link in aarecord['duxiu']['aa_duxiu_derived']['miaochuan_links_multiple']:
        #         additional['download_urls'].append(('', '', f"Miaochuan link 秒传: {miaochuan_link} (for use with BaiduYun)"))

    scidb_info = allthethings.utils.scidb_info(aarecord, additional)
    if scidb_info is not None:
        additional['fast_partner_urls'] = [(gettext('page.md5.box.download.scidb'), f"/scidb/{scidb_info['doi']}", gettext('common.md5.servers.no_browser_verification'))] + additional['fast_partner_urls']
        additional['download_urls'] = [(gettext('page.md5.box.download.scidb'), f"/scidb/{scidb_info['doi']}", "")] + additional['download_urls']

    return additional

def add_additional_to_aarecord(aarecord):
    return { **aarecord['_source'], '_score': (aarecord.get('_score') or 0.0), 'additional': get_additional_for_aarecord(aarecord['_source']) }

@page.get("/md5/<string:md5_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def md5_page(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]
    return render_aarecord(f"md5:{canonical_md5}")

@page.get("/ia/<string:ia_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def isbn_page(isbn_input):
    return redirect(f"/isbndb/{isbn_input}", code=302)

@page.get("/isbndb/<string:isbn_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def isbndb_page(isbn_input):
    return render_aarecord(f"isbn:{isbn_input}")

@page.get("/ol/<string:ol_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def ol_page(ol_input):
    return render_aarecord(f"ol:{ol_input}")

@page.get("/doi/<path:doi_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def doi_page(doi_input):
    return render_aarecord(f"doi:{doi_input}")

@page.get("/oclc/<path:oclc_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def oclc_page(oclc_input):
    return render_aarecord(f"oclc:{oclc_input}")

@page.get("/duxiu_ssid/<path:duxiu_ssid_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def duxiu_ssid_page(duxiu_ssid_input):
    return render_aarecord(f"duxiu_ssid:{duxiu_ssid_input}")

@page.get("/cadal_ssno/<path:cadal_ssno_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*24)
def cadal_ssno_page(cadal_ssno_input):
    return render_aarecord(f"cadal_ssno:{cadal_ssno_input}")

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
                index=allthethings.utils.all_virtshards_for_index("aarecords") + allthethings.utils.all_virtshards_for_index("aarecords_journals"),
                size=50,
                query={ "term": { "search_only_fields.search_doi": doi_input } },
                timeout=ES_TIMEOUT_PRIMARY,
            )
        except Exception as err:
            return redirect(f"/search?q=doi:{doi_input}", code=302)
        aarecords = [add_additional_to_aarecord(aarecord) for aarecord in search_results_raw['hits']['hits']]
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
            minimum = 500
            maximum = 1000
            if fast_scidb:
                minimum = 1000
                maximum = 5000
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
                "lgrsnf_book": ("before", ["Source data at: https://annas-archive.org/db/lgrsnf/<id>.json"]),
                "lgrsfic_book": ("before", ["Source data at: https://annas-archive.org/db/lgrsfic/<id>.json"]),
                "lgli_file": ("before", ["Source data at: https://annas-archive.org/db/lgli/<f_id>.json"]),
                "zlib_book": ("before", ["Source data at: https://annas-archive.org/db/zlib/<zlibrary_id>.json"]),
                "aac_zlib3_book": ("before", ["Source data at: https://annas-archive.org/db/aac_zlib3/<zlibrary_id>.json"]),
                "ia_record": ("before", ["Source data at: https://annas-archive.org/db/ia/<ia_id>.json"]),
                "isbndb": ("before", ["Source data at: https://annas-archive.org/db/isbndb/<isbn13>.json"]),
                "ol": ("before", ["Source data at: https://annas-archive.org/db/ol/<ol_edition>.json"]),
                "scihub_doi": ("before", ["Source data at: https://annas-archive.org/db/scihub_doi/<doi>.json"]),
                "oclc": ("before", ["Source data at: https://annas-archive.org/db/oclc/<oclc>.json"]),
                "duxiu": ("before", ["Source data at: https://annas-archive.org/db/duxiu_ssid/<duxiu_ssid>.json or https://annas-archive.org/db/cadal_ssno/<cadal_ssno>.json or https://annas-archive.org/db/duxiu_md5/<md5>.json"]),
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
        canonical_md5=canonical_md5,
    )

def compute_download_speed(targeted_seconds, filesize, minimum, maximum):
    return min(maximum, max(minimum, int(filesize/1000/targeted_seconds)))

@page.get("/slow_download/<string:md5_input>/<int:path_index>/<int:domain_index>")
@allthethings.utils.no_cache()
def md5_slow_download(md5_input, path_index, domain_index):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if (request.headers.get('cf-worker') or '') != '':
        return render_template(
            "page/partner_download.html",
            header_active="search",
            only_official=True,
            canonical_md5=canonical_md5,
        )

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
            # minimum = 10
            # maximum = 100
            minimum = 10
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
                warning=warning,
                canonical_md5=canonical_md5,
            )

def search_query_aggs(search_index_long):
    return {
        "search_content_type": { "terms": { "field": "search_only_fields.search_content_type", "size": 200 } },
        "search_extension": { "terms": { "field": "search_only_fields.search_extension", "size": 9 } },
        "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "size": 100 } },
        "search_record_sources": { "terms": { "field": "search_only_fields.search_record_sources", "size": 100 } },
        "search_most_likely_language_code": { "terms": { "field": "search_only_fields.search_most_likely_language_code", "size": 50 } },
    }

@cachetools.cached(cache=cachetools.TTLCache(maxsize=30000, ttl=24*60*60))
def all_search_aggs(display_lang, search_index_long):
    try:
        search_results_raw = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[search_index_long].search(index=allthethings.utils.all_virtshards_for_index(search_index_long), size=0, aggs=search_query_aggs(search_index_long), timeout=ES_TIMEOUT_ALL_AGG)
    except:
        # Simple retry, just once.
        search_results_raw = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[search_index_long].search(index=allthethings.utils.all_virtshards_for_index(search_index_long), size=0, aggs=search_query_aggs(search_index_long), timeout=ES_TIMEOUT_ALL_AGG)

    all_aggregations = {}
    # Unfortunately we have to special case the "unknown language", which is currently represented with an empty string `bucket['key'] != ''`, otherwise this gives too much trouble in the UI.
    all_aggregations['search_most_likely_language_code'] = []
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def search_page():
    search_page_timer = time.perf_counter()
    had_es_timeout = False
    had_primary_es_timeout = False
    had_fatal_es_timeout = False
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
                            # The 3.0 is from the 3x "boost" of title/author/etc in search_text.
                            { "rank_feature": { "field": "search_only_fields.search_score_base_rank", "boost": 3.0*10000.0 } },
                            { 
                                "constant_score": {
                                    "filter": { "term": { "search_only_fields.search_most_likely_language_code": { "value": allthethings.utils.get_base_lang_code(get_locale()) } } },
                                    "boost": 3.0*50000.0,
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
                            { "rank_feature": { "field": "search_only_fields.search_score_base_rank", "boost": 3.0*10000.0/100000.0 } },
                            {
                                "constant_score": {
                                    "filter": { "term": { "search_only_fields.search_most_likely_language_code": { "value": allthethings.utils.get_base_lang_code(get_locale()) } } },
                                    "boost": 3.0*50000.0/100000.0,
                                },
                            },
                        ],
                        "must": [
                            {
                                "simple_query_string": {
                                    "query": search_input, "fields": ["search_only_fields.search_text"],
                                    "default_operator": "and",
                                    "boost": 1.0/100000.0,
                                },
                            },
                        ],
                    },
                },
            ],
        },
    }

    max_display_results = 150
    additional_display_results = 50

    es_handle = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[search_index_long]

    search_names = ['search1_primary']
    search_results_raw = {'responses': [{} for search_name in search_names]}
    for attempt in [1, 2]:
        try:
            search_results_raw = dict(es_handle.msearch(
                request_timeout=5,
                max_concurrent_searches=64,
                max_concurrent_shard_requests=64,
                searches=[
                    { "index": allthethings.utils.all_virtshards_for_index(search_index_long) },
                    {
                        "size": max_display_results, 
                        "query": search_query,
                        "aggs": search_query_aggs(search_index_long),
                        "post_filter": { "bool": { "filter": post_filter } },
                        "sort": custom_search_sorting+['_score'],
                        "track_total_hits": False,
                        "timeout": ES_TIMEOUT_PRIMARY,
                        # "knn": { "field": "search_only_fields.search_e5_small_query", "query_vector": list(map(float, get_e5_small_model().encode(f"query: {search_input}", normalize_embeddings=True))), "k": 10, "num_candidates": 1000 },
                    },
                ]
            ))
            break
        except Exception as err:
            if attempt < 2:
                print(f"Warning: another attempt during primary ES search {search_input=}")
            else:
                had_es_timeout = True
                had_primary_es_timeout = True
                had_fatal_es_timeout = True
                print(f"Exception during primary ES search {attempt=} {search_input=} ///// {repr(err)} ///// {traceback.format_exc()}\n")
                break
    for num, response in enumerate(search_results_raw['responses']):
        es_stats.append({ 'name': search_names[num], 'took': response.get('took'), 'timed_out': response.get('timed_out') })
        if response.get('timed_out') or (response == {}):
            had_es_timeout = True
            had_primary_es_timeout = True
    primary_response_raw = search_results_raw['responses'][0]

    display_lang = allthethings.utils.get_base_lang_code(get_locale())
    all_aggregations, all_aggregations_es_stat = all_search_aggs(display_lang, search_index_long)
    es_stats.append(all_aggregations_es_stat)

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
    elif 'aggregations' in primary_response_raw:
        if 'search_most_likely_language_code' in primary_response_raw['aggregations']:
            for bucket in primary_response_raw['aggregations']['search_most_likely_language_code']['buckets']:
                doc_counts['search_most_likely_language_code'][bucket['key'] if bucket['key'] != '' else '_empty'] = bucket['doc_count']
        for bucket in primary_response_raw['aggregations']['search_content_type']['buckets']:
            doc_counts['search_content_type'][bucket['key']] = bucket['doc_count']
        for bucket in primary_response_raw['aggregations']['search_extension']['buckets']:
            doc_counts['search_extension'][bucket['key'] if bucket['key'] != '' else '_empty'] = bucket['doc_count']
        for bucket in primary_response_raw['aggregations']['search_access_types']['buckets']:
            doc_counts['search_access_types'][bucket['key']] = bucket['doc_count']
        for bucket in primary_response_raw['aggregations']['search_record_sources']['buckets']:
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
    if 'hits' in primary_response_raw:
        search_aarecords = [add_additional_to_aarecord(aarecord_raw) for aarecord_raw in primary_response_raw['hits']['hits'] if aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]

    additional_search_aarecords = []
    if len(search_aarecords) < max_display_results:
        search_names2 = ['search2', 'search3', 'search4']
        search_results_raw2 = {'responses': [{} for search_name in search_names2]}
        for attempt in [1, 2]:
            try:
                search_results_raw2 = dict(es_handle.msearch(
                    request_timeout=4,
                    max_concurrent_searches=64,
                    max_concurrent_shard_requests=64,
                    searches=[
                        # For partial matches, first try our original query again but this time without filters.
                        { "index": allthethings.utils.all_virtshards_for_index(search_index_long) },
                        {
                            "size": additional_display_results,
                            "query": search_query,
                            "sort": custom_search_sorting+['_score'],
                            "track_total_hits": False,
                            "timeout": ES_TIMEOUT,
                        },
                        # Then do an "OR" query, but this time with the filters again.
                        { "index": allthethings.utils.all_virtshards_for_index(search_index_long) },
                        {
                            "size": additional_display_results,
                            # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                            "query": {"bool": { "must": { "match": { "search_only_fields.search_text": { "query": search_input } } }, "filter": post_filter } },
                            "sort": custom_search_sorting+['_score'],
                            "track_total_hits": False,
                            "timeout": ES_TIMEOUT,
                        },
                        # If we still don't have enough, do another OR query but this time without filters.
                        { "index": allthethings.utils.all_virtshards_for_index(search_index_long) },
                        {
                            "size": additional_display_results,
                            # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                            "query": {"bool": { "must": { "match": { "search_only_fields.search_text": { "query": search_input } } } } },
                            "sort": custom_search_sorting+['_score'],
                            "track_total_hits": False,
                            "timeout": ES_TIMEOUT,
                        },
                    ]
                ))
                break
            except Exception as err:
                if attempt < 2:
                    print(f"Warning: another attempt during secondary ES search {search_input=}")
                else:
                    had_es_timeout = True
                    print(f"Exception during secondary ES search {search_input=} ///// {repr(err)} ///// {traceback.format_exc()}\n")
        for num, response in enumerate(search_results_raw2['responses']):
            es_stats.append({ 'name': search_names2[num], 'took': response.get('took'), 'timed_out': response.get('timed_out') })
            if response.get('timed_out'):
                had_es_timeout = True

        seen_ids = set([aarecord['id'] for aarecord in search_aarecords])
        search_result2_raw = search_results_raw2['responses'][0]
        if 'hits' in search_result2_raw:
            additional_search_aarecords += [add_additional_to_aarecord(aarecord_raw) for aarecord_raw in search_result2_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]

        if len(additional_search_aarecords) < additional_display_results:
            seen_ids = seen_ids.union(set([aarecord['id'] for aarecord in additional_search_aarecords]))
            search_result3_raw = search_results_raw2['responses'][1]
            if 'hits' in search_result3_raw:
                additional_search_aarecords += [add_additional_to_aarecord(aarecord_raw) for aarecord_raw in search_result3_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]

            if len(additional_search_aarecords) < additional_display_results:
                seen_ids = seen_ids.union(set([aarecord['id'] for aarecord in additional_search_aarecords]))
                search_result4_raw = search_results_raw2['responses'][2]
                if 'hits' in search_result4_raw:
                    additional_search_aarecords += [add_additional_to_aarecord(aarecord_raw) for aarecord_raw in search_result4_raw['hits']['hits'] if aarecord_raw['_id'] not in seen_ids and aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]

    es_stats.append({ 'name': 'search_page_timer', 'took': (time.perf_counter() - search_page_timer) * 1000, 'timed_out': False })
    
    search_dict = {}
    search_dict['search_aarecords'] = search_aarecords[0:max_display_results]
    search_dict['additional_search_aarecords'] = additional_search_aarecords[0:additional_display_results]
    search_dict['max_search_aarecords_reached'] = (len(search_aarecords) >= max_display_results)
    search_dict['max_additional_search_aarecords_reached'] = (len(additional_search_aarecords) >= additional_display_results)
    search_dict['aggregations'] = aggregations
    search_dict['sort_value'] = sort_value
    search_dict['search_index_short'] = search_index_short
    search_dict['es_stats'] = es_stats
    search_dict['had_primary_es_timeout'] = had_primary_es_timeout
    search_dict['had_es_timeout'] = had_es_timeout
    search_dict['had_fatal_es_timeout'] = had_fatal_es_timeout

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
        ), 200))
    if had_es_timeout:
        r.headers.add('Cache-Control', 'no-cache')
    return r
