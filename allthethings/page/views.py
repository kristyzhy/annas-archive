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
from config.settings import AA_EMAIL, DOWNLOADS_SECRET_KEY, AACID_SMALL_DATA_IMPORTS

import allthethings.utils

HASHED_DOWNLOADS_SECRET_KEY = hashlib.sha256(DOWNLOADS_SECRET_KEY.encode()).digest()

page = Blueprint("page", __name__, template_folder="templates")

# Per https://software.annas-archive.gs/AnnaArchivist/annas-archive/-/issues/37
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

ES_TIMEOUT_PRIMARY = "200ms"
ES_TIMEOUT_ALL_AGG = "20s"
ES_TIMEOUT = "100ms"

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
    debug_from = []
    try:
        lang = str(langcodes.standardize_tag(langcodes.get(substr), macro=True))
        debug_from.append('langcodes.get')
    except langcodes.tag_parser.LanguageTagError:
        for country_name, language_name in country_lang_mapping.items():
            # Be careful not to use `in` here, or if we do then watch out for overlap, e.g. "Oman" in "Romania".
            if country_name.lower() == substr.lower():
                try:
                    lang = str(langcodes.standardize_tag(langcodes.find(language_name), macro=True))
                    debug_from.append(f"langcodes.find with country_lang_mapping {country_name.lower()=} == {substr.lower()=}")
                except LookupError:
                    pass
                break
        if lang == '':
            try:
                lang = str(langcodes.standardize_tag(langcodes.find(substr), macro=True))
                debug_from.append('langcodes.find WITHOUT country_lang_mapping')
            except LookupError:
                # In rare cases, disambiguate by saying that `substr` is written in English
                try:
                    lang = str(langcodes.standardize_tag(langcodes.find(substr, language='en'), macro=True))
                    debug_from.append('langcodes.find with language=en')
                except LookupError:
                    lang = ''
    # Further specification is unnecessary for most languages, except Traditional Chinese.
    if ('-' in lang) and (lang != 'zh-Hant'):
        lang = lang.split('-', 1)[0]
        debug_from.append('split on dash')
    # We have a bunch of weird data that gets interpreted as "Egyptian Sign Language" when it's
    # clearly all just Spanish..
    if lang == 'esl':
        lang = 'es'
        debug_from.append('esl to es')
    # Seems present within ISBNdb, and just means "en".
    if lang == 'us':
        lang = 'en'
        debug_from.append('us to en')
    # "urdu" not being converted to "ur" seems to be a bug in langcodes?
    if lang == 'urdu':
        lang = 'ur'
        debug_from.append('urdu to ur')
    # Same
    if lang == 'thai':
        lang = 'ur'
        debug_from.append('thai to ur')
    # Same
    if lang == 'esp':
        lang = 'eo'
        debug_from.append('esp to eo')
    if lang in ['und', 'mul', 'mis']:
        lang = ''
        debug_from.append('delete und/mul/mis')
    # print(f"{debug_from=}")
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def home_page():
    if allthethings.utils.DOWN_FOR_MAINTENANCE:
        return render_template("page/maintenance.html", header_active="")

    torrents_data = get_torrents_data()
    return render_template("page/home.html", header_active="home/home", torrents_data=torrents_data)

@page.get("/login")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def login_page():
    return redirect(f"/account", code=301)
    # return render_template("page/login.html", header_active="account")

@page.get("/about")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def about_page():
    return redirect(f"/faq", code=301)

@page.get("/faq")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def faq_page():
    popular_ids = [
        "md5:8336332bf5877e3adbfb60ac70720cd5", # Against intellectual monopoly
        "md5:61a1797d76fc9a511fb4326f265c957b", # Cryptonomicon
        "md5:0d9b713d0dcda4c9832fcb056f3e4102", # Aaron Swartz
        "md5:6963187473f4f037a28e2fe1153ca793", # How music got free
        "md5:6ed2d768ec1668c73e4fa742e3df78d6", # Physics
    ]
    with Session(engine) as session:
        aarecords = (get_aarecords_elasticsearch(popular_ids) or [])
        aarecords.sort(key=lambda aarecord: popular_ids.index(aarecord['id']))

        return render_template(
            "page/faq.html",
            header_active="home/faq",
            aarecords=aarecords,
        )

@page.get("/security")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def security_page():
    return redirect(f"/faq#security", code=301)

@page.get("/mobile")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def mobile_page():
    return redirect(f"/faq#mobile", code=301)

@page.get("/llm")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def llm_page():
    return render_template("page/llm.html", header_active="home/llm")

@page.get("/mirrors")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def mirrors_page():
    return render_template("page/mirrors.html", header_active="home/mirrors")

@page.get("/browser_verification")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
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
        # WARNING! Sorting by primary ID does a lexical sort, not numerical. Sorting by zlib3_records.aacid gets records from refreshes. zlib3_files.aacid is most reliable.
        cursor.execute('SELECT annas_archive_meta__aacid__zlib3_records.byte_offset, annas_archive_meta__aacid__zlib3_records.byte_length FROM annas_archive_meta__aacid__zlib3_records JOIN annas_archive_meta__aacid__zlib3_files USING (primary_id) ORDER BY annas_archive_meta__aacid__zlib3_files.aacid DESC LIMIT 1')
        zlib3_record = cursor.fetchone()
        zlib_date = ''
        if zlib3_record is not None:
            zlib_aac_lines = allthethings.utils.get_lines_from_aac_file(cursor, 'zlib3_records', [(zlib3_record['byte_offset'], zlib3_record['byte_length'])])
            if len(zlib_aac_lines) > 0:
                zlib_date = orjson.loads(zlib_aac_lines[0])['metadata']['date_modified']

        stats_data_es = dict(es.msearch(
            request_timeout=30,
            max_concurrent_searches=10,
            max_concurrent_shard_requests=10,
            searches=[
                { "index": allthethings.utils.all_virtshards_for_index("aarecords") },
                { "track_total_hits": True, "timeout": "20s", "size": 0, "aggs": { "total_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } } },
                { "index": allthethings.utils.all_virtshards_for_index("aarecords") },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": {
                        "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "include": "aa_download" } },
                        "search_bulk_torrents": { "terms": { "field": "search_only_fields.search_bulk_torrents", "include": "has_bulk_torrents" } },
                    },
                },
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
            ],
        ))
        stats_data_esaux = dict(es_aux.msearch(
            request_timeout=30,
            max_concurrent_searches=10,
            max_concurrent_shard_requests=10,
            searches=[
                { "index": allthethings.utils.all_virtshards_for_index("aarecords_journals") },
                { "track_total_hits": True, "timeout": "20s", "size": 0, "aggs": { "total_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } } },
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
                { "index": allthethings.utils.all_virtshards_for_index("aarecords_journals") },
                {
                    "track_total_hits": True,
                    "timeout": "20s",
                    "size": 0,
                    "aggs": { "search_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } },
                },
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
                { "index": allthethings.utils.all_virtshards_for_index("aarecords_digital_lending") },
                { "track_total_hits": True, "timeout": "20s", "size": 0, "aggs": { "total_filesize": { "sum": { "field": "search_only_fields.search_filesize" } } } },
            ],
        ))
        responses_without_timed_out = [response for response in (stats_data_es['responses'] + stats_data_esaux['responses']) if 'timed_out' not in response]
        if len(responses_without_timed_out) > 0:
            raise Exception(f"One of the 'get_stats_data' responses didn't have 'timed_out' field in it: {responses_without_timed_out=}")
        if any([response['timed_out'] for response in (stats_data_es['responses'] + stats_data_esaux['responses'])]):
            # WARNING: don't change this message because we match on 'timed out' below
            raise Exception("One of the 'get_stats_data' responses timed out")

        # print(f'{orjson.dumps(stats_data_es)=}')

        stats_by_group = {}
        for bucket in stats_data_es['responses'][2]['aggregations']['search_record_sources']['buckets']:
            stats_by_group[bucket['key']] = {
                'count': bucket['doc_count'],
                'filesize': bucket['search_filesize']['value'],
                'aa_count': bucket['search_access_types']['buckets'][0]['doc_count'],
                'torrent_count': bucket['search_bulk_torrents']['buckets'][0]['doc_count'] if len(bucket['search_bulk_torrents']['buckets']) > 0 else 0,
            }
        stats_by_group['journals'] = {
            'count': stats_data_esaux['responses'][2]['hits']['total']['value'],
            'filesize': stats_data_esaux['responses'][2]['aggregations']['search_filesize']['value'],
            'aa_count': stats_data_esaux['responses'][3]['aggregations']['search_access_types']['buckets'][0]['doc_count'],
            'torrent_count': stats_data_esaux['responses'][3]['aggregations']['search_bulk_torrents']['buckets'][0]['doc_count'] if len(stats_data_esaux['responses'][3]['aggregations']['search_bulk_torrents']['buckets']) > 0 else 0,
        }
        stats_by_group['total'] = {
            'count': stats_data_es['responses'][0]['hits']['total']['value']+stats_data_esaux['responses'][0]['hits']['total']['value'],
            'filesize': stats_data_es['responses'][0]['aggregations']['total_filesize']['value']+stats_data_esaux['responses'][0]['aggregations']['total_filesize']['value'],
            'aa_count': stats_data_es['responses'][1]['aggregations']['search_access_types']['buckets'][0]['doc_count']+stats_data_esaux['responses'][1]['aggregations']['search_access_types']['buckets'][0]['doc_count'],
            'torrent_count': (stats_data_es['responses'][1]['aggregations']['search_bulk_torrents']['buckets'][0]['doc_count']+stats_data_esaux['responses'][1]['aggregations']['search_bulk_torrents']['buckets'][0]['doc_count']) if (len(stats_data_es['responses'][1]['aggregations']['search_bulk_torrents']['buckets'])+len(stats_data_esaux['responses'][1]['aggregations']['search_bulk_torrents']['buckets'])) > 0 else 0,
        }
        stats_by_group['ia']['count'] += stats_data_esaux['responses'][4]['hits']['total']['value']
        stats_by_group['total']['count'] += stats_data_esaux['responses'][4]['hits']['total']['value']
        stats_by_group['ia']['filesize'] += stats_data_esaux['responses'][4]['aggregations']['total_filesize']['value']
        stats_by_group['total']['filesize'] += stats_data_esaux['responses'][4]['aggregations']['total_filesize']['value']

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
    if 'upload' in file_path:
        group = 'upload'

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
        small_file_dicts_grouped_other_aa = collections.defaultdict(list)
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
            elif toplevel == 'other_aa':
                list_to_add = small_file_dicts_grouped_other_aa[group]
            else:
                list_to_add = small_file_dicts_grouped_aa[group]
            display_name = small_file['file_path'].split('/')[-1]
            list_to_add.append({
                "created": small_file['created'].strftime("%Y-%m-%d"), # First, so it gets sorted by first. Also, only year-month-day, so it gets secondarily sorted by file path.
                "file_path": small_file['file_path'],
                "metadata": metadata, 
                "aa_currently_seeding": allthethings.utils.aa_currently_seeding(metadata),
                "size_string": format_filesize(metadata['data_size']), 
                "file_path_short": small_file['file_path'].replace('torrents/managed_by_aa/annas_archive_meta__aacid/', '').replace('torrents/managed_by_aa/annas_archive_data__aacid/', '').replace(f'torrents/managed_by_aa/{group}/', '').replace(f'torrents/external/{group}/', '').replace(f'torrents/other_aa/{group}/', ''),
                "display_name": display_name, 
                "scrape_metadata": scrape_metadata, 
                "scrape_created": scrape_created, 
                "is_metadata": (('annas_archive_meta__' in small_file['file_path']) or ('.sql' in small_file['file_path']) or ('-index-' in small_file['file_path']) or ('-derived' in small_file['file_path']) or ('isbndb' in small_file['file_path']) or ('covers-' in small_file['file_path']) or ('-metadata-' in small_file['file_path']) or ('-thumbs' in small_file['file_path']) or ('.csv' in small_file['file_path'])),
                "magnet_link": f"magnet:?xt=urn:btih:{metadata['btih']}&dn={urllib.parse.quote(display_name)}&tr=udp://tracker.opentrackr.org:1337/announce",
                "temp_uuid": shortuuid.uuid(),
                "partially_broken": (small_file['file_path'] in allthethings.utils.TORRENT_PATHS_PARTIALLY_BROKEN),
            })

        for key in small_file_dicts_grouped_external:
            small_file_dicts_grouped_external[key] = natsort.natsorted(small_file_dicts_grouped_external[key], key=lambda x: list(x.values()))
        for key in small_file_dicts_grouped_aa:
            small_file_dicts_grouped_aa[key] = natsort.natsorted(small_file_dicts_grouped_aa[key], key=lambda x: list(x.values()))
        for key in small_file_dicts_grouped_other_aa:
            small_file_dicts_grouped_other_aa[key] = natsort.natsorted(small_file_dicts_grouped_other_aa[key], key=lambda x: list(x.values()))

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
            'torrents/managed_by_aa/annas_archive_data__aacid/annas_archive_data__aacid__upload_files_duxiu_epub__20240510T045054Z--20240510T045055Z.torrent',
        ]
        for file_path_list in aac_meta_file_paths_grouped.values():
            obsolete_file_paths += file_path_list[0:-1]
        for item in small_file_dicts_grouped_other_aa['aa_derived_mirror_metadata'][0:-1]:
            obsolete_file_paths.append(item['file_path'])

        # Tack on "obsolete" fields, now that we have them
        for group in list(small_file_dicts_grouped_aa.values()) + list(small_file_dicts_grouped_external.values()) + list(small_file_dicts_grouped_other_aa.values()):
            for item in group:
                item['obsolete'] = (item['file_path'] in obsolete_file_paths)

        # TODO: exclude obsolete
        group_size_strings = { group: format_filesize(total) for group, total in group_sizes.items() }
        seeder_size_strings = { index: format_filesize(seeder_sizes[index]) for index in [0,1,2] }

        return {
            'small_file_dicts_grouped': {
                'managed_by_aa': dict(sorted(small_file_dicts_grouped_aa.items())),
                'external': dict(sorted(small_file_dicts_grouped_external.items())),
                'other_aa': dict(sorted(small_file_dicts_grouped_other_aa.items())),
            },
            'group_size_strings': group_size_strings,
            'seeder_size_strings': seeder_size_strings,
            'seeder_sizes': seeder_sizes,
            'seeder_size_total_string': format_filesize(sum(seeder_sizes.values())),
        }

@page.get("/datasets")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/ia")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_ia_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_ia.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/duxiu")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_duxiu_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_duxiu.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/zlib")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_zlib_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_zlib.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/isbndb")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_isbndb_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_isbndb.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/scihub")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_scihub_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_scihub.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/libgen_rs")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_libgen_rs_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_libgen_rs.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/libgen_li")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_libgen_li_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_libgen_li.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/openlib")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_openlib_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_openlib.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

@page.get("/datasets/worldcat")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def datasets_worldcat_page():
    try:
        stats_data = get_stats_data()
        return render_template("page/datasets_worldcat.html", header_active="home/datasets", stats_data=stats_data)
    except Exception as e:
        if 'timed out' in str(e):
            return "Error with datasets page, please try again.", 503
        raise

# @page.get("/datasets/isbn_ranges")
# @allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
# def datasets_isbn_ranges_page():
#     try:
#         stats_data = get_stats_data()
#     except Exception as e:
#         if 'timed out' in str(e):
#             return "Error with datasets page, please try again.", 503
#     return render_template("page/datasets_isbn_ranges.html", header_active="home/datasets", stats_data=stats_data)

@page.get("/copyright")
@allthethings.utils.no_cache()
def copyright_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return render_template("page/login_to_view.html", header_active="")
    return render_template("page/copyright.html", header_active="")

@page.get("/contact")
@allthethings.utils.no_cache()
def contact_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return render_template("page/login_to_view.html", header_active="")
    return render_template("page/contact.html", header_active="", AA_EMAIL=AA_EMAIL)

@page.get("/fast_download_no_more")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def fast_download_no_more_page():
    return render_template("page/fast_download_no_more.html", header_active="")

@page.get("/fast_download_not_member")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def fast_download_not_member_page():
    return render_template("page/fast_download_not_member.html", header_active="")

@page.get("/torrents")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def torrents_page():
    torrents_data = get_torrents_data()

    with mariapersist_engine.connect() as connection:
        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT * FROM mariapersist_torrent_scrapes_histogram WHERE day > DATE_FORMAT(NOW() - INTERVAL 60 DAY, "%Y-%m-%d") AND day < DATE_FORMAT(NOW() - INTERVAL 1 DAY, "%Y-%m-%d") ORDER BY day, seeder_group LIMIT 500')
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

@page.get("/codes")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def codes_page():
    with engine.connect() as connection:
        prefix_arg = request.args.get('prefix') or ''
        if len(prefix_arg) > 0:
            prefix_b64_redirect = base64.b64encode(prefix_arg.encode()).decode()
            return redirect(f"/codes?prefix_b64={prefix_b64_redirect}", code=301)

        prefix_b64 = request.args.get('prefix_b64') or ''
        try:
            prefix_bytes = base64.b64decode(prefix_b64)
        except:
            return "Invalid prefix_b64", 404

        connection.connection.ping(reconnect=True)
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)

        # TODO: Since 'code' and 'aarecord_id' are binary, this might not work with multi-byte UTF-8 chars. Test (and fix) that!

        cursor.execute("DROP FUNCTION IF EXISTS fn_get_next_codepoint")
        cursor.execute("""
            CREATE FUNCTION fn_get_next_codepoint(initial INT, prefix VARCHAR(200)) RETURNS INT
            NOT DETERMINISTIC
            READS SQL DATA
            BEGIN
                    DECLARE _next VARCHAR(200);
                    DECLARE EXIT HANDLER FOR NOT FOUND RETURN 0;
                    SELECT  ORD(SUBSTRING(code, LENGTH(prefix)+1, 1))
                    INTO    _next
                    FROM    aarecords_codes
                    WHERE   code LIKE CONCAT(REPLACE(REPLACE(prefix, "%%", "\\%%"), "_", "\\_"), "%%") AND code >= CONCAT(prefix, CHAR(initial + 1))
                    ORDER BY
                            code
                    LIMIT 1;
                    RETURN _next;
            END
        """)

        exact_matches = []
        cursor.execute('SELECT aarecord_id FROM aarecords_codes WHERE code = %(prefix)s ORDER BY code, aarecord_id LIMIT 1000', { "prefix": prefix_bytes })
        for row in cursor.fetchall():
            aarecord_id = row['aarecord_id'].decode()
            exact_matches.append({
                "label": aarecord_id,
                "link": allthethings.utils.path_for_aarecord_id(aarecord_id),
            })

        # cursor.execute('SELECT CONCAT(%(prefix)s, IF(@r > 0, CHAR(@r USING utf8), "")) AS new_prefix, @r := fn_get_next_codepoint(IF(@r > 0, @r, ORD(" ")), %(prefix)s) AS next_letter FROM (SELECT @r := ORD(SUBSTRING(code, LENGTH(%(prefix)s)+1, 1)) FROM aarecords_codes WHERE code >= %(prefix)s ORDER BY code LIMIT 1) vars, (SELECT 1 FROM aarecords_codes LIMIT 1000) iterator WHERE @r IS NOT NULL', { "prefix": prefix })
        cursor.execute('SELECT CONCAT(%(prefix)s, CHAR(@r USING binary)) AS new_prefix, @r := fn_get_next_codepoint(@r, %(prefix)s) AS next_letter FROM (SELECT @r := ORD(SUBSTRING(code, LENGTH(%(prefix)s)+1, 1)) FROM aarecords_codes WHERE code > %(prefix)s AND code LIKE CONCAT(REPLACE(REPLACE(%(prefix)s, "%%", "\\%%"), "_", "\\_"), "%%") ORDER BY code LIMIT 1) vars, (SELECT 1 FROM aarecords_codes LIMIT 1000) iterator WHERE @r != 0', { "prefix": prefix_bytes })
        new_prefixes_raw = cursor.fetchall()
        new_prefixes = [row['new_prefix'] for row in new_prefixes_raw]
        prefix_rows = []
        # print(f"{new_prefixes_raw=}")

        for new_prefix in new_prefixes:
            # TODO: more efficient? Though this is not that bad because we don't typically iterate through that many values.
            cursor.execute('SELECT code, row_number_order_by_code, dense_rank_order_by_code FROM aarecords_codes WHERE code LIKE CONCAT(REPLACE(REPLACE(%(new_prefix)s, "%%", "\\%%"), "_", "\\_"), "%%") ORDER BY code, aarecord_id LIMIT 1', { "new_prefix": new_prefix })
            first_record = cursor.fetchone()
            cursor.execute('SELECT code, row_number_order_by_code, dense_rank_order_by_code FROM aarecords_codes WHERE code LIKE CONCAT(REPLACE(REPLACE(%(new_prefix)s, "%%", "\\%%"), "_", "\\_"), "%%") ORDER BY code DESC, aarecord_id DESC LIMIT 1', { "new_prefix": new_prefix })
            last_record = cursor.fetchone()

            if first_record['code'] == last_record['code']:
                code = first_record["code"]
                code_label = code.decode(errors='replace')
                code_b64 = base64.b64encode(code).decode()
                prefix_rows.append({
                    "label": code_label,
                    "records": last_record["row_number_order_by_code"]-first_record["row_number_order_by_code"]+1,
                    "link": f'/codes?prefix_b64={code_b64}',
                })
            else:
                longest_prefix = os.path.commonprefix([first_record["code"], last_record["code"]])
                longest_prefix_label = longest_prefix.decode(errors='replace')
                longest_prefix_b64 = base64.b64encode(longest_prefix).decode()
                prefix_rows.append({
                    "label": f'{longest_prefix_label}⋯',
                    "codes": last_record["dense_rank_order_by_code"]-first_record["dense_rank_order_by_code"]+1,
                    "records": last_record["row_number_order_by_code"]-first_record["row_number_order_by_code"]+1,
                    "link": f'/codes?prefix_b64={longest_prefix_b64}',
                })

        bad_unicode = False
        try:
            prefix_bytes.decode()
        except:
            bad_unicode = True

        return render_template(
            "page/codes.html",
            header_active="",
            prefix_label=prefix_bytes.decode(errors='replace'),
            prefix_rows=prefix_rows,
            exact_matches=exact_matches,
            bad_unicode=bad_unicode,
        )

zlib_book_dict_comments = {
    **allthethings.utils.COMMON_DICT_COMMENTS,
    "zlibrary_id": ("before", ["This is a file from the Z-Library collection of Anna's Archive.",
                      "More details at https://annas-archive.gs/datasets/zlib",
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
    # return f"https://static.z-lib.gs/covers/books/{md5[0:2]}/{md5[2:4]}/{md5[4:6]}/{md5}.jpg"
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
        cursor.execute(f'SELECT annas_archive_meta__aacid__zlib3_records.byte_offset AS record_byte_offset, annas_archive_meta__aacid__zlib3_records.byte_length AS record_byte_length, annas_archive_meta__aacid__zlib3_files.byte_offset AS file_byte_offset, annas_archive_meta__aacid__zlib3_files.byte_length AS file_byte_length, annas_archive_meta__aacid__zlib3_records.primary_id AS primary_id FROM annas_archive_meta__aacid__zlib3_records LEFT JOIN annas_archive_meta__aacid__zlib3_files USING (primary_id) WHERE {aac_key} IN %(values)s', { "values": [str(value) for value in values] })
        
        zlib3_rows = []
        zlib3_records_indexes = []
        zlib3_records_offsets_and_lengths = []
        zlib3_files_indexes = []
        zlib3_files_offsets_and_lengths = []
        for row_index, row in enumerate(cursor.fetchall()):
            zlib3_records_indexes.append(row_index)
            zlib3_records_offsets_and_lengths.append((row['record_byte_offset'], row['record_byte_length']))
            if row.get('file_byte_offset') is not None:
                zlib3_files_indexes.append(row_index)
                zlib3_files_offsets_and_lengths.append((row['file_byte_offset'], row['file_byte_length']))
            zlib3_rows.append({ "primary_id": row['primary_id'] })
        for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'zlib3_records', zlib3_records_offsets_and_lengths)):
            zlib3_rows[zlib3_records_indexes[index]]['record'] = orjson.loads(line_bytes)
        for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'zlib3_files', zlib3_files_offsets_and_lengths)):
            zlib3_rows[zlib3_files_indexes[index]]['file'] = orjson.loads(line_bytes)

        raw_aac_zlib3_books_by_primary_id = collections.defaultdict(list)
        aac_zlib3_books_by_primary_id = collections.defaultdict(dict)
        # Merge different iterations of books, so even when a book gets "missing":1 later, we still use old
        # metadata where available (note: depends on the sorting below).
        for row in zlib3_rows:
            raw_aac_zlib3_books_by_primary_id[row['primary_id']].append(row),
            new_row = aac_zlib3_books_by_primary_id[row['primary_id']]
            new_row['primary_id'] = row['primary_id']
            if 'file' in row:
                new_row['file'] = row['file']
            new_row['record'] = {
                **(new_row.get('record') or {}),
                **row['record'],
                'metadata': {
                    **((new_row.get('record') or {}).get('metadata') or {}),
                    **row['record']['metadata'],
                }
            }
        aac_zlib3_books = list(aac_zlib3_books_by_primary_id.values())
    except Exception as err:
        print(f"Error in get_aac_zlib3_book_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    aac_zlib3_book_dicts = []
    for zlib_book in aac_zlib3_books:
        aac_zlib3_book_dict = zlib_book['record']['metadata']
        if 'file' in zlib_book:
            aac_zlib3_book_dict['md5'] = zlib_book['file']['metadata']['md5']
            if 'filesize' in zlib_book['file']['metadata']:
                aac_zlib3_book_dict['filesize'] = zlib_book['file']['metadata']['filesize']
            aac_zlib3_book_dict['file_aacid'] = zlib_book['file']['aacid']
            aac_zlib3_book_dict['file_data_folder'] = zlib_book['file']['data_folder']
        else:
            aac_zlib3_book_dict['md5'] = None
            aac_zlib3_book_dict['filesize'] = None
            aac_zlib3_book_dict['file_aacid'] = None
            aac_zlib3_book_dict['file_data_folder'] = None
        aac_zlib3_book_dict['record_aacid'] = zlib_book['record']['aacid']
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

        aac_zlib3_book_dict['raw_aac'] = raw_aac_zlib3_books_by_primary_id[str(aac_zlib3_book_dict['zlibrary_id'])]

        aac_zlib3_book_dicts.append(add_comments_to_dict(aac_zlib3_book_dict, zlib_book_dict_comments))
    return aac_zlib3_book_dicts

@page.get("/db/zlib/<int:zlib_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def zlib_book_json(zlib_id):
    with Session(engine) as session:
        zlib_book_dicts = get_zlib_book_dicts(session, "zlibrary_id", [zlib_id])
        if len(zlib_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(zlib_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/aac_zlib3/<int:zlib_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def aac_zlib3_book_json(zlib_id):
    with Session(engine) as session:
        aac_zlib3_book_dicts = get_aac_zlib3_book_dicts(session, "zlibrary_id", [zlib_id])
        if len(aac_zlib3_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(aac_zlib3_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

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

    ia_entries_combined = []
    ia2_records_indexes = []
    ia2_records_offsets_and_lengths = []
    ia2_acsmpdf_files_indexes = []
    ia2_acsmpdf_files_offsets_and_lengths = []
    index = 0
    # Prioritize ia_entries2 first, because their records are newer. This order matters
    # futher below.
    for ia_record, ia_file, ia2_acsmpdf_file in ia_entries2 + ia_entries:
        ia_record_dict = ia_record.to_dict()
        if ia_record_dict.get('byte_offset') is not None:
            ia2_records_indexes.append(index)
            ia2_records_offsets_and_lengths.append((ia_record_dict['byte_offset'], ia_record_dict['byte_length']))
        ia_file_dict = None
        if ia_file is not None:
            ia_file_dict = ia_file.to_dict()
        ia2_acsmpdf_file_dict = None
        if ia2_acsmpdf_file is not None:
            ia2_acsmpdf_file_dict = ia2_acsmpdf_file.to_dict()
            ia2_acsmpdf_files_indexes.append(index)
            ia2_acsmpdf_files_offsets_and_lengths.append((ia2_acsmpdf_file_dict['byte_offset'], ia2_acsmpdf_file_dict['byte_length']))
        ia_entries_combined.append([ia_record_dict, ia_file_dict, ia2_acsmpdf_file_dict])
        index += 1

    session.connection().connection.ping(reconnect=True)
    cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'ia2_records', ia2_records_offsets_and_lengths)):
        ia_entries_combined[ia2_records_indexes[index]][0] = orjson.loads(line_bytes)
    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'ia2_acsmpdf_files', ia2_acsmpdf_files_offsets_and_lengths)):
        ia_entries_combined[ia2_acsmpdf_files_indexes[index]][2] = orjson.loads(line_bytes)

    ia_record_dicts = []
    for ia_record_dict, ia_file_dict, ia2_acsmpdf_file_dict in ia_entries_combined:
        if 'aacid' in ia_record_dict:
            # Convert from AAC.
            ia_record_dict = {
                "ia_id": ia_record_dict["metadata"]["ia_id"],
                # "has_thumb" # We'd need to look at both ia_entries2 and ia_entries to get this, but not worth it.
                "libgen_md5": None,
                "json": ia_record_dict["metadata"]['metadata_json'],
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
            if ia_file_dict is not None:
                ia_record_dict['aa_ia_file'] = ia_file_dict
                ia_record_dict['aa_ia_file']['extension'] = 'pdf'
                added_date_unified_file = { "ia_file_scrape": "2023-06-28" }
            elif ia2_acsmpdf_file_dict is not None:
                ia_record_dict['aa_ia_file'] = {
                    'md5': ia2_acsmpdf_file_dict['metadata']['md5'],
                    'type': 'ia2_acsmpdf',
                    'filesize': ia2_acsmpdf_file_dict['metadata']['filesize'],
                    'ia_id': ia2_acsmpdf_file_dict['metadata']['ia_id'],
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
            "ia_id": ("before", ["This is an IA record, augmented by Anna's Archive.",
                              "More details at https://annas-archive.gs/datasets/ia",
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
            "ia_id": ("before", ["This is an IA record, augmented by Anna's Archive.",
                              "More details at https://annas-archive.gs/datasets/ia",
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def ia_record_json(ia_id):
    with Session(engine) as session:
        ia_record_dicts = get_ia_record_dicts(session, "ia_id", [ia_id])
        if len(ia_record_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(ia_record_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def ol_book_json(ol_edition):
    with Session(engine) as session:
        ol_book_dicts = get_ol_book_dicts(session, "ol_edition", [ol_edition])
        if len(ol_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(ol_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

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
        allthethings.utils.add_classification_unified(lgrs_book_dict, 'lgrsnf_topic', lgrs_book_dict.get('topic_descr') or '')
        for name, unified_name in allthethings.utils.LGRS_TO_UNIFIED_IDENTIFIERS_MAPPING.items():
            if name in lgrs_book_dict:
                allthethings.utils.add_identifier_unified(lgrs_book_dict, unified_name, lgrs_book_dict[name])
        for name, unified_name in allthethings.utils.LGRS_TO_UNIFIED_CLASSIFICATIONS_MAPPING.items():
            if name in lgrs_book_dict:
                allthethings.utils.add_classification_unified(lgrs_book_dict, unified_name, lgrs_book_dict[name])

        lgrs_book_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "id": ("before", ["This is a Libgen.rs Non-Fiction record, augmented by Anna's Archive.",
                              "More details at https://annas-archive.gs/datasets/libgen_rs",
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
                              "More details at https://annas-archive.gs/datasets/libgen_rs",
                              "Most of these fields are explained at https://wiki.mhut.org/content:bibliographic_data",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
        }
        lgrs_book_dicts.append(add_comments_to_dict(lgrs_book_dict, lgrs_book_dict_comments))

    return lgrs_book_dicts

@page.get("/db/lgrs/nf/<int:lgrsnf_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgrsnf_book_json_redirect(lgrsnf_book_id):
    return redirect(f"/db/lgrsnf/{lgrsnf_book_id}.json", code=301)
@page.get("/db/lgrs/fic/<int:lgrsfic_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgrsfic_book_json_redirect(lgrsfic_book_id):
    return redirect(f"/db/lgrsfic/{lgrsfic_book_id}.json", code=301)

@page.get("/db/lgrsnf/<int:lgrsnf_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgrsnf_book_json(lgrsnf_book_id):
    with Session(engine) as session:
        lgrs_book_dicts = get_lgrsnf_book_dicts(session, "ID", [lgrsnf_book_id])
        if len(lgrs_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(lgrs_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}
@page.get("/db/lgrsfic/<int:lgrsfic_book_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgrsfic_book_json(lgrsfic_book_id):
    with Session(engine) as session:
        lgrs_book_dicts = get_lgrsfic_book_dicts(session, "ID", [lgrsfic_book_id])
        if len(lgrs_book_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(lgrs_book_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

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
                edition_dict['issue_other_fields_json'] = allthethings.utils.nice_json(issue_other_fields)
            standard_info_fields = dict((key, edition_dict['descriptions_mapped'][key]) for key in allthethings.utils.LGLI_STANDARD_INFO_FIELDS if edition_dict['descriptions_mapped'].get(key) not in ['', '0', 0, None])
            if len(standard_info_fields) > 0:
                edition_dict['standard_info_fields_json'] = allthethings.utils.nice_json(standard_info_fields)
            date_info_fields = dict((key, edition_dict['descriptions_mapped'][key]) for key in allthethings.utils.LGLI_DATE_INFO_FIELDS if edition_dict['descriptions_mapped'].get(key) not in ['', '0', 0, None])
            if len(date_info_fields) > 0:
                edition_dict['date_info_fields_json'] = allthethings.utils.nice_json(date_info_fields)

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

        if lgli_file_dict['libgen_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_libgen_id', lgli_file_dict['libgen_id'])
        if lgli_file_dict['fiction_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_fiction_id', lgli_file_dict['fiction_id'])
        if lgli_file_dict['fiction_rus_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_fiction_rus_id', lgli_file_dict['fiction_rus_id'])
        if lgli_file_dict['comics_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_comics_id', lgli_file_dict['comics_id'])
        if lgli_file_dict['scimag_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_scimag_id', lgli_file_dict['scimag_id'])
        if lgli_file_dict['standarts_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_standarts_id', lgli_file_dict['standarts_id'])
        if lgli_file_dict['magz_id'] > 0:
            allthethings.utils.add_identifier_unified(lgli_file_dict, 'lgli_magz_id', lgli_file_dict['magz_id'])

        lgli_file_dict['added_date_unified'] = {}
        if lgli_file_dict['time_added'] != '0000-00-00 00:00:00':
            if not isinstance(lgli_file_dict['time_added'], datetime.datetime):
                raise Exception(f"Unexpected {lgli_file_dict['time_added']=} for {lgli_file_dict=}")
            lgli_file_dict['added_date_unified'] = { 'lgli_source': lgli_file_dict['time_added'].isoformat() }

        lgli_file_dict_comments = {
            **allthethings.utils.COMMON_DICT_COMMENTS,
            "f_id": ("before", ["This is a Libgen.li file record, augmented by Anna's Archive.",
                     "More details at https://annas-archive.gs/datasets/libgen_li",
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgli_file_json(lgli_file_id):
    return redirect(f"/db/lgli/{lgli_file_id}.json", code=301)

@page.get("/db/lgli/<int:lgli_file_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def lgli_json(lgli_file_id):
    with Session(engine) as session:
        lgli_file_dicts = get_lgli_file_dicts(session, "f_id", [lgli_file_id])
        if len(lgli_file_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(lgli_file_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

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
                               "More details at https://annas-archive.gs/datasets",
                               allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "isbndb": ("before", ["All matching records from the ISBNdb database."]),
        }
        isbn_dicts.append(add_comments_to_dict(isbn_dict, isbndb_wrapper_comments))

    return isbn_dicts

@page.get("/db/isbndb/<string:isbn>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def isbndb_json(isbn):
    with Session(engine) as session:
        isbndb_dicts = get_isbndb_dicts(session, [isbn])
        if len(isbndb_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(isbndb_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}


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
                              "More details at https://annas-archive.gs/datasets/scihub",
                              "The source URL is https://sci-hub.ru/datasets/dois-2022-02-12.7z",
                              allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
        }
        scihub_doi_dicts.append(add_comments_to_dict(scihub_doi_dict, scihub_doi_dict_comments))
    return scihub_doi_dicts

@page.get("/db/scihub_doi/<path:doi>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def scihub_doi_json(doi):
    with Session(engine) as session:
        scihub_doi_dicts = get_scihub_doi_dicts(session, 'doi', [doi])
        if len(scihub_doi_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(scihub_doi_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}


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
        # TODO: Replace with aarecords_codes
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
        # TODO: Replace with aarecords_codes
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def oclc_oclc_json(oclc):
    with Session(engine) as session:
        oclc_dicts = get_oclc_dicts(session, 'oclc', [oclc])
        if len(oclc_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(oclc_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

def get_duxiu_dicts(session, key, values):
    if len(values) == 0:
        return []
    if key not in ['duxiu_ssid', 'cadal_ssno', 'md5', 'filename_decoded_basename']:
        raise Exception(f"Unexpected 'key' in get_duxiu_dicts: '{key}'")

    primary_id_prefix = f"{key}_"

    aac_records_by_primary_id = collections.defaultdict(dict)
    try:
        session.connection().connection.ping(reconnect=True)
        cursor = session.connection().connection.cursor(pymysql.cursors.DictCursor)
        if key == 'md5':
            cursor.execute(f'SELECT annas_archive_meta__aacid__duxiu_records.byte_offset, annas_archive_meta__aacid__duxiu_records.byte_length, annas_archive_meta__aacid__duxiu_files.primary_id, annas_archive_meta__aacid__duxiu_files.byte_offset AS generated_file_byte_offset, annas_archive_meta__aacid__duxiu_files.byte_length AS generated_file_byte_length FROM annas_archive_meta__aacid__duxiu_records JOIN annas_archive_meta__aacid__duxiu_files ON (CONCAT("md5_", annas_archive_meta__aacid__duxiu_files.md5) = annas_archive_meta__aacid__duxiu_records.primary_id) WHERE annas_archive_meta__aacid__duxiu_files.primary_id IN %(values)s', { "values": values })
        elif key == 'filename_decoded_basename':
            cursor.execute(f'SELECT byte_offset, byte_length, filename_decoded_basename AS primary_id FROM annas_archive_meta__aacid__duxiu_records WHERE filename_decoded_basename IN %(values)s', { "values": values })
        else:
            cursor.execute(f'SELECT primary_id, byte_offset, byte_length FROM annas_archive_meta__aacid__duxiu_records WHERE primary_id IN %(values)s', { "values": [f'{primary_id_prefix}{value}' for value in values] })
    except Exception as err:
        print(f"Error in get_duxiu_dicts when querying {key}; {values}")
        print(repr(err))
        traceback.print_tb(err.__traceback__)

    top_level_records = []
    duxiu_records_indexes = []
    duxiu_records_offsets_and_lengths = []
    duxiu_files_indexes = []
    duxiu_files_offsets_and_lengths = []
    for row_index, row in enumerate(cursor.fetchall()):
        duxiu_records_indexes.append(row_index)
        duxiu_records_offsets_and_lengths.append((row['byte_offset'], row['byte_length']))
        if row.get('generated_file_byte_offset') is not None:
            duxiu_files_indexes.append(row_index)
            duxiu_files_offsets_and_lengths.append((row['generated_file_byte_offset'], row['generated_file_byte_length']))
        top_level_records.append([{ "primary_id": row['primary_id'] }, None])

    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'duxiu_records', duxiu_records_offsets_and_lengths)):
        top_level_records[duxiu_records_indexes[index]][0]["aac"] = orjson.loads(line_bytes)
    for index, line_bytes in enumerate(allthethings.utils.get_lines_from_aac_file(cursor, 'duxiu_files', duxiu_files_offsets_and_lengths)):
        top_level_records[duxiu_files_indexes[index]][1] = { "aac": orjson.loads(line_bytes) }

    for duxiu_record_dict, duxiu_file_dict in top_level_records:
        new_aac_record = {
            **duxiu_record_dict["aac"],
            "primary_id": duxiu_record_dict["primary_id"],
        }
        if duxiu_file_dict is not None:
            new_aac_record["generated_file_aacid"] = duxiu_file_dict["aac"]["aacid"]
            new_aac_record["generated_file_data_folder"] = duxiu_file_dict["aac"]["data_folder"]
            new_aac_record["generated_file_metadata"] = duxiu_file_dict["aac"]["metadata"]
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

        aac_records_by_primary_id[new_aac_record['primary_id']][new_aac_record['aacid']] = new_aac_record

    if key != 'filename_decoded_basename':
        aa_derived_duxiu_ssids_to_primary_ids = collections.defaultdict(list)
        for primary_id, aac_records in aac_records_by_primary_id.items():
            for aac_record in aac_records.values():
                if "aa_derived_duxiu_ssid" in aac_record["metadata"]["record"]:
                    aa_derived_duxiu_ssids_to_primary_ids[aac_record["metadata"]["record"]["aa_derived_duxiu_ssid"]].append(primary_id)
        if len(aa_derived_duxiu_ssids_to_primary_ids) > 0:
            # Careful! Make sure this recursion doesn't loop infinitely.
            for record in get_duxiu_dicts(session, 'duxiu_ssid', list(aa_derived_duxiu_ssids_to_primary_ids.keys())):
                for primary_id in aa_derived_duxiu_ssids_to_primary_ids[record['duxiu_ssid']]:
                    for aac_record in record['aac_records']:
                        # NOTE: It's important that we append these aac_records at the end, since we select the "best" records
                        # first, and any data we get directly from the fields associated with the file itself should take precedence.
                        if aac_record['aacid'] not in aac_records_by_primary_id[primary_id]:
                            aac_records_by_primary_id[primary_id][aac_record['aacid']] = {
                                "aac_record_added_because": "duxiu_ssid",
                                **aac_record
                            }

        filename_decoded_basename_to_primary_ids = collections.defaultdict(list)
        for primary_id, aac_records in aac_records_by_primary_id.items():
            for aac_record in aac_records.values():
                if "filename_decoded" in aac_record["metadata"]["record"]:
                    basename = aac_record["metadata"]["record"]["filename_decoded"].rsplit('.', 1)[0][0:250] # Same logic as in MySQL query.
                    if len(basename) >= 5: # Skip very short basenames as they might have too many hits.
                        filename_decoded_basename_to_primary_ids[basename].append(primary_id)
        if len(filename_decoded_basename_to_primary_ids) > 0:
            # Careful! Make sure this recursion doesn't loop infinitely.
            for record in get_duxiu_dicts(session, 'filename_decoded_basename', list(filename_decoded_basename_to_primary_ids.keys())):
                for primary_id in filename_decoded_basename_to_primary_ids[record['filename_decoded_basename']]:
                    for aac_record in record['aac_records']:
                        # NOTE: It's important that we append these aac_records at the end, since we select the "best" records
                        # first, and any data we get directly from the fields associated with the file itself should take precedence.
                        if aac_record['aacid'] not in aac_records_by_primary_id[primary_id]:
                            aac_records_by_primary_id[primary_id][aac_record['aacid']] = {
                                "aac_record_added_because": "filename_decoded_basename",
                                **aac_record
                            }

    duxiu_dicts = []
    for primary_id, aac_records in aac_records_by_primary_id.items():
        # print(f"{primary_id=}, {aac_records=}")

        duxiu_dict = {}

        if key == 'duxiu_ssid':
            duxiu_dict['duxiu_ssid'] = primary_id.replace('duxiu_ssid_', '')
        elif key == 'cadal_ssno':
            duxiu_dict['cadal_ssno'] = primary_id.replace('cadal_ssno_', '')
        elif key == 'md5':
            duxiu_dict['md5'] = primary_id
        elif key == 'filename_decoded_basename':
            duxiu_dict['filename_decoded_basename'] = primary_id
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
        duxiu_dict['aa_duxiu_derived']['problems_infos'] = []
        duxiu_dict['aac_records'] = list(aac_records.values())

        if key == 'duxiu_ssid':
            duxiu_dict['aa_duxiu_derived']['duxiu_ssid_multiple'].append(duxiu_dict['duxiu_ssid'])
        elif key == 'cadal_ssno':
            duxiu_dict['aa_duxiu_derived']['cadal_ssno_multiple'].append(duxiu_dict['cadal_ssno'])
        elif key == 'md5':
            duxiu_dict['aa_duxiu_derived']['md5_multiple'].append(duxiu_dict['md5'])

        for aac_record in aac_records.values():
            duxiu_dict['aa_duxiu_derived']['added_date_unified']['duxiu_meta_scrape'] = max(duxiu_dict['aa_duxiu_derived']['added_date_unified'].get('duxiu_meta_scrape') or '', datetime.datetime.strptime(aac_record['aacid'].split('__')[2], "%Y%m%dT%H%M%SZ").isoformat())

            if aac_record['metadata']['type'] == 'dx_20240122__books':
                # 512w_final_csv has a bunch of incorrect records from dx_20240122__books deleted, so skip these entirely.
                # if len(aac_record['metadata']['record'].get('source') or '') > 0:
                #     duxiu_dict['aa_duxiu_derived']['source_multiple'].append(['dx_20240122__books', aac_record['metadata']['record']['source']])
                pass
            elif aac_record['metadata']['type'] in ['512w_final_csv', 'DX_corrections240209_csv']:
                if aac_record['metadata']['type'] == '512w_final_csv' and any([record['metadata']['type'] == 'DX_corrections240209_csv' for record in aac_records.values()]):
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

                    # Only check for problems when we have generated_file_aacid, since that indicates this is the main file record.
                    if len(aac_record['metadata']['record']['pdg_broken_files']) > 3:
                        duxiu_dict['aa_duxiu_derived']['problems_infos'].append({
                            'duxiu_problem_type': 'pdg_broken_files',
                            'pdg_broken_files_len': len(aac_record['metadata']['record']['pdg_broken_files']),
                        })

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
                                "More details at https://annas-archive.gs/datasets/duxiu",
                                allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "cadal_ssno": ("before", ["This is a CADAL metadata record.",
                                "More details at https://annas-archive.gs/datasets/duxiu",
                                allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
            "md5": ("before", ["This is a DuXiu/related metadata record.",
                                "More details at https://annas-archive.gs/datasets/duxiu",
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def duxiu_ssid_json(duxiu_ssid):
    with Session(engine) as session:
        duxiu_dicts = get_duxiu_dicts(session, 'duxiu_ssid', [duxiu_ssid])
        if len(duxiu_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(duxiu_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/cadal_ssno/<path:cadal_ssno>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def cadal_ssno_json(cadal_ssno):
    with Session(engine) as session:
        duxiu_dicts = get_duxiu_dicts(session, 'cadal_ssno', [cadal_ssno])
        if len(duxiu_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(duxiu_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

@page.get("/db/duxiu_md5/<path:md5>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def duxiu_md5_json(md5):
    with Session(engine) as session:
        duxiu_dicts = get_duxiu_dicts(session, 'md5', [md5])
        if len(duxiu_dicts) == 0:
            return "{}", 404
        return allthethings.utils.nice_json(duxiu_dicts[0]), {'Content-Type': 'text/json; charset=utf-8'}

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

number_of_get_aarecords_elasticsearch_exceptions = 0
def get_aarecords_elasticsearch(aarecord_ids):
    global number_of_get_aarecords_elasticsearch_exceptions

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
        for attempt in [1,2,3]:
            try:
                search_results_raw += es_handle.mget(docs=docs)['docs']
                break
            except:
                print(f"Warning: another attempt during get_aarecords_elasticsearch {aarecord_ids=}")
                if attempt >= 3:
                    number_of_get_aarecords_elasticsearch_exceptions += 1
                    if number_of_get_aarecords_elasticsearch_exceptions > 5:
                        raise
                    else:
                        print("Haven't reached number_of_get_aarecords_elasticsearch_exceptions limit yet, so not raising")
                        return None
        number_of_get_aarecords_elasticsearch_exceptions = 0
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

def aarecord_sources(aarecord):
    aarecord_id_split = aarecord['id'].split(':', 1)
    return list(dict.fromkeys([
        *(['duxiu']     if aarecord['duxiu'] is not None else []),
        *(['ia']        if aarecord['ia_record'] is not None else []),
        *(['isbndb']    if (aarecord_id_split[0] == 'isbn' and len(aarecord['isbndb'] or []) > 0) else []),
        *(['lgli']      if aarecord['lgli_file'] is not None else []),
        *(['lgrs']      if aarecord['lgrsfic_book'] is not None else []),
        *(['lgrs']      if aarecord['lgrsnf_book'] is not None else []),
        *(['oclc']      if (aarecord_id_split[0] == 'oclc' and len(aarecord['oclc'] or []) > 0) else []),
        *(['ol']        if (aarecord_id_split[0] == 'ol' and len(aarecord['ol'] or []) > 0) else []),
        *(['scihub']    if len(aarecord['scihub_doi']) > 0 else []),
        *(['zlib']      if aarecord['aac_zlib3_book'] is not None else []),
        *(['zlib']      if aarecord['zlib_book'] is not None else []),
    ]))

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
    ia_record_dicts2 = dict(('ia:' + item['ia_id'], item) for item in get_ia_record_dicts(session, "ia_id", split_ids['ia']) if item.get('aa_ia_file') is None)
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
            # For now, keep out cover urls from zlib entirely, and only add them ad-hoc from aac_zlib3_book.cover_path.
            # cover_url_multiple.append(((aarecord['aac_zlib3_book'] or {}).get('cover_url_guess') or '').strip())
            # cover_url_multiple.append(((aarecord['zlib_book'] or {}).get('cover_url_guess') or '').strip())
            cover_url_multiple_processed = list(dict.fromkeys(filter(len, cover_url_multiple)))
            aarecord['file_unified_data']['cover_url_best'] = (cover_url_multiple_processed + [''])[0]
            aarecord['file_unified_data']['cover_url_additional'] = [s for s in cover_url_multiple_processed if s != aarecord['file_unified_data']['cover_url_best']]
        if len(aarecord['file_unified_data']['cover_url_additional']) == 0:
            del aarecord['file_unified_data']['cover_url_additional']

        extension_multiple = [
            (((aarecord['ia_record'] or {}).get('aa_ia_file') or {}).get('extension') or '').strip().lower(),
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
        if len(((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('problems_infos') or []) > 0:
            for duxiu_problem_info in (((aarecord['duxiu'] or {}).get('aa_duxiu_derived') or {}).get('problems_infos') or []):
                if duxiu_problem_info['duxiu_problem_type'] == 'pdg_broken_files':
                    # TODO:TRANSLATE
                    aarecord['file_unified_data']['problems'].append({ 'type': 'duxiu_pdg_broken_files', 'descr': f"{duxiu_problem_info['pdg_broken_files_len']} affected pages", 'better_md5': '' })
                else:
                    raise Exception(f"Unknown duxiu_problem_type: {duxiu_problem_info=}")
        # TODO: Reindex and use "removal reason" properly, and do some statistics to remove spurious removal reasons.
        # For now we only mark it as a problem on the basis of aac_zlib3 if there is no libgen record.
        if (((aarecord['aac_zlib3_book'] or {}).get('removed') or 0) == 1) and (aarecord['lgrsnf_book'] is None) and (aarecord['lgrsfic_book'] is None) and (aarecord['lgli_file'] is None):
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
                'cover_path': (aarecord['aac_zlib3_book'].get('cover_path') or ''),
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

        search_content_type = aarecord['file_unified_data']['content_type']
        # Once we have the content type.
        aarecord['indexes'] = [allthethings.utils.get_aarecord_search_index(aarecord_id_split[0], search_content_type)]

        # Even though `additional` is only for computing real-time stuff,
        # we'd like to cache some fields for in the search results.
        with force_locale('en'):
            additional = get_additional_for_aarecord(aarecord)
            aarecord['file_unified_data']['has_aa_downloads'] = additional['has_aa_downloads']
            aarecord['file_unified_data']['has_aa_exclusive_downloads'] = additional['has_aa_exclusive_downloads']
            aarecord['file_unified_data']['has_torrent_paths'] = (1 if (len(additional['torrent_paths']) > 0) else 0)
            aarecord['file_unified_data']['has_scidb'] = additional['has_scidb']
            for torrent_path in additional['torrent_paths']:
                allthethings.utils.add_identifier_unified(aarecord['file_unified_data'], 'torrent', torrent_path['torrent_path'])

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
                *(['aa_scidb'] if aarecord['file_unified_data']['has_scidb'] == 1 else []),
                *(['meta_explore'] if allthethings.utils.get_aarecord_id_prefix_is_metadata(aarecord_id_split[0]) else []),
                *(['torrents_available'] if aarecord['file_unified_data']['has_torrent_paths'] == 1 else []),
            ],
            'search_record_sources': aarecord_sources(aarecord),
            # Used in external system, check before changing.
            'search_bulk_torrents': 'has_bulk_torrents' if aarecord['file_unified_data']['has_torrent_paths'] else 'no_bulk_torrents',
        }

        if len(aarecord['search_only_fields']['search_record_sources']) == 0:
            raise Exception(f"Missing search_record_sources; phantom record? {aarecord=}")
        if len(aarecord['search_only_fields']['search_access_types']) == 0:
            raise Exception(f"Missing search_access_types; phantom record? {aarecord=}")
        
        # At the very end
        aarecord['search_only_fields']['search_score_base_rank'] = float(aarecord_score_base(aarecord))

    # embeddings = get_embeddings_for_aarecords(session, aarecords)
    # for embedding, aarecord in zip(embeddings, aarecords):
    #     aarecord['search_only_fields']['search_e5_small_query'] = embedding['e5_small_query']
    
    return aarecords

def get_md5_problem_type_mapping():
    return { 
        "lgrsnf_visible":         gettext("common.md5_problem_type_mapping.lgrsnf_visible"),
        "lgrsfic_visible":        gettext("common.md5_problem_type_mapping.lgrsfic_visible"),
        "lgli_visible":           gettext("common.md5_problem_type_mapping.lgli_visible"),
        "lgli_broken":            gettext("common.md5_problem_type_mapping.lgli_broken"),
        "zlib_missing":           gettext("common.md5_problem_type_mapping.zlib_missing"),
        "duxiu_pdg_broken_files": "Not all pages could be converted to PDF", # TODO:TRANSLATE
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
            "aa_scidb": "🧬 SciDB", # TODO:TRANSLATE
            "external_download": gettext("common.access_types_mapping.external_download"),
            "external_borrow": gettext("common.access_types_mapping.external_borrow"),
            "external_borrow_printdisabled": gettext("common.access_types_mapping.external_borrow_printdisabled"),
            "meta_explore": gettext("common.access_types_mapping.meta_explore"),
            "torrents_available": gettext("common.access_types_mapping.torrents_available"),
        }

def get_record_sources_mapping(display_lang):
    with force_locale(display_lang):
        return {
            "lgrs": gettext("common.record_sources_mapping.lgrs"),
            "lgli": gettext("common.record_sources_mapping.lgli"),
            "zlib": gettext("common.record_sources_mapping.zlib"),
            "ia": "IA", # TODO:TRANSLATE
            # "ia": gettext("common.record_sources_mapping.ia"),
            "isbndb": gettext("common.record_sources_mapping.isbndb"),
            "ol": gettext("common.record_sources_mapping.ol"),
            "scihub": gettext("common.record_sources_mapping.scihub"),
            "oclc": gettext("common.record_sources_mapping.oclc"),
            "duxiu": gettext("common.record_sources_mapping.duxiu"),
        }

def get_specific_search_fields_mapping(display_lang):
    with force_locale(display_lang):
        return {
            'title': gettext('common.specific_search_fields.title'),
            'author': gettext('common.specific_search_fields.author'),
            'publisher': gettext('common.specific_search_fields.publisher'),
            'edition_varia': gettext('common.specific_search_fields.edition_varia'),
            'year': "Year published", # TODO:TRANSLATE
            'original_filename': gettext('common.specific_search_fields.original_filename'),
            'description_comments': gettext('common.specific_search_fields.description_comments'),
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
        gettext("common.md5.servers.no_browser_verification")
        additional['fast_partner_urls'].append((gettext("common.md5.servers.fast_partner", number=len(additional['fast_partner_urls'])+1), '/fast_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/' + str(index), '(no browser verification or waitlists)' if len(additional['fast_partner_urls']) == 0 else ''))
    for index in range(len(allthethings.utils.SLOW_DOWNLOAD_DOMAINS)):
        if allthethings.utils.SLOW_DOWNLOAD_DOMAINS_SLIGHTLY_FASTER[index]:
            # TODO:TRANSLATE
            additional['slow_partner_urls'].append((gettext("common.md5.servers.slow_partner", number=len(additional['slow_partner_urls'])+1), '/slow_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/' + str(index), '(slightly faster but with waitlist)'))
        else:
            additional['slow_partner_urls'].append((gettext("common.md5.servers.slow_partner", number=len(additional['slow_partner_urls'])+1), '/slow_download/' + aarecord['id'][len("md5:"):] + '/' + str(len(additional['partner_url_paths'])) + '/' + str(index), '(no waitlist, but can be very slow)'))
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
    additional['path'] = allthethings.utils.path_for_aarecord_id(aarecord['id'])
    additional['most_likely_language_name'] = (get_display_name_for_lang(aarecord['file_unified_data'].get('most_likely_language_code', None) or '', allthethings.utils.get_base_lang_code(get_locale())) if aarecord['file_unified_data'].get('most_likely_language_code', None) else '')

    additional['added_date_best'] = ''
    added_date_best = aarecord['file_unified_data'].get('added_date_best') or ''
    if len(added_date_best) > 0:
        additional['added_date_best'] = added_date_best.split('T', 1)[0]
    added_date_unified = aarecord['file_unified_data'].get('added_date_unified') or {}
    if (len(added_date_unified) > 0) and (len(additional['added_date_best']) > 0):
        additional['added_date_best'] += ' (' + ', '.join([label + ': ' + date.split('T', 1)[0] for label, date in added_date_unified.items()]) + ')'


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
    zlib3_cover_path = ((aarecord.get('aac_zlib3_book') or {}).get('cover_path') or '')
    if '/collections/' in zlib3_cover_path:
        cover_url = f"https://s3proxy.cdn-zlib.se/{zlib3_cover_path}"
    elif 'zlib' in cover_url or '1lib' in cover_url: # Remove old zlib cover_urls.
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
        'cover_missing_hue_deg': int(hashlib.md5(aarecord['id'].encode()).hexdigest(), 16) % 360,
        'cover_url': cover_url,
        'top_row': ", ".join([item for item in [
                additional['most_likely_language_name'],
                f".{aarecord['file_unified_data']['extension_best']}" if len(aarecord['file_unified_data']['extension_best']) > 0 else '',
                "/".join(filter(len,["🚀" if (aarecord['file_unified_data'].get('has_aa_downloads') == 1) else "", *aarecord_sources(aarecord)])),
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
        'description': aarecord['file_unified_data'].get('stripped_description_best', None) or '',
        'metadata_comments': comments_multiple,
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
        if code['key'] in ['isbn13', 'isbn10', 'doi', 'issn', 'duxiu_ssid', 'cadal_ssno']:
            filename_code = f" -- {code['value']}"
            break
    filename_base = f"{filename_slug}{filename_code} -- {aarecord['id'].split(':', 1)[1]}".replace('.', '_')
    additional['filename_without_annas_archive'] = urllib.parse.quote(f"{filename_base}.{filename_extension}", safe='')
    additional['filename'] = urllib.parse.quote(f"{filename_base} -- Anna’s Archive.{filename_extension}", safe='')

    additional['download_urls'] = []
    additional['fast_partner_urls'] = []
    additional['slow_partner_urls'] = []
    additional['partner_url_paths'] = []
    additional['has_aa_downloads'] = 0
    additional['has_aa_exclusive_downloads'] = 0
    additional['torrent_paths'] = []
    additional['ipfs_urls'] = []
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
            partner_path = f"u/ia/annas-archive-ia-2023-06-acsm/{directory}/{ia_id}.{extension}"
            additional['torrent_paths'].append({ "collection": "ia", "torrent_path": f"managed_by_aa/ia/annas-archive-ia-acsm-{directory}.tar.torrent", "file_level1": f"annas-archive-ia-acsm-{directory}.tar", "file_level2": f"{ia_id}.{extension}" })
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
            partner_path = f"u/ia/annas-archive-ia-2023-06-lcpdf/{directory}/{ia_id}.{extension}"
            additional['torrent_paths'].append({ "collection": "ia", "torrent_path": f"managed_by_aa/ia/annas-archive-ia-lcpdf-{directory}.tar.torrent", "file_level1": f"annas-archive-ia-lcpdf-{directory}.tar", "file_level2": f"{ia_id}.{extension}" })
        elif ia_file_type == 'ia2_acsmpdf':
            partner_path = make_temp_anon_aac_path("i/ia2_acsmpdf_files", aarecord['ia_record']['aa_ia_file']['aacid'], aarecord['ia_record']['aa_ia_file']['data_folder'])
            additional['torrent_paths'].append({ "collection": "ia", "torrent_path": f"managed_by_aa/annas_archive_data__aacid/{aarecord['ia_record']['aa_ia_file']['data_folder']}.torrent", "file_level1": aarecord['ia_record']['aa_ia_file']['aacid'], "file_level2": "" })
        else:
            raise Exception(f"Unknown ia_record file type: {ia_file_type}")
        add_partner_servers(partner_path, 'aa_exclusive', aarecord, additional)
    if (aarecord.get('duxiu') is not None) and (aarecord['duxiu'].get('duxiu_file') is not None):
        data_folder = aarecord['duxiu']['duxiu_file']['data_folder']
        additional['torrent_paths'].append({ "collection": "duxiu", "torrent_path": f"managed_by_aa/annas_archive_data__aacid/{data_folder}.torrent", "file_level1": aarecord['duxiu']['duxiu_file']['aacid'], "file_level2": "" })
        server = None
        if data_folder >= 'annas_archive_data__aacid__duxiu_files__20240613T170516Z--20240613T170517Z' and data_folder <= 'annas_archive_data__aacid__duxiu_files__20240613T171624Z--20240613T171625Z':
            server = 'w'
        elif data_folder >= 'annas_archive_data__aacid__duxiu_files__20240613T171757Z--20240613T171758Z' and data_folder <= 'annas_archive_data__aacid__duxiu_files__20240613T190311Z--20240613T190312Z':
            server = 'v'
        elif data_folder >= 'annas_archive_data__aacid__duxiu_files__20240613T190428Z--20240613T190429Z' and data_folder <= 'annas_archive_data__aacid__duxiu_files__20240613T204954Z--20240613T204955Z':
            server = 'w'
        elif data_folder >= 'annas_archive_data__aacid__duxiu_files__20240613T205835Z--20240613T205836Z' and data_folder <= 'annas_archive_data__aacid__duxiu_files__20240613T223234Z--20240613T223235Z':
            server = 'w'
        else:
            if AACID_SMALL_DATA_IMPORTS:
                server = 'w'
            else:
                raise Exception(f"Warning: Unknown duxiu range: {data_folder=}")

        date = data_folder.split('__')[3][0:8]
        partner_path = f"{server}/duxiu_files/{date}/{data_folder}/{aarecord['duxiu']['duxiu_file']['aacid']}"
        add_partner_servers(partner_path, 'aa_exclusive', aarecord, additional)
    if aarecord.get('lgrsnf_book') is not None:
        lgrsnf_thousands_dir = (aarecord['lgrsnf_book']['id'] // 1000) * 1000
        lgrsnf_torrent_path = f"external/libgen_rs_non_fic/r_{lgrsnf_thousands_dir:03}.torrent"
        lgrsnf_manually_synced = (lgrsnf_thousands_dir <= 4337000)
        lgrsnf_filename = aarecord['lgrsnf_book']['md5'].lower()
        if lgrsnf_manually_synced or (lgrsnf_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path):
            additional['torrent_paths'].append({ "collection": "libgen_rs_non_fic", "torrent_path": lgrsnf_torrent_path, "file_level1": lgrsnf_filename, "file_level2": "" })
        if lgrsnf_manually_synced or ((lgrsnf_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path) and (torrents_json_aa_currently_seeding_by_torrent_path[lgrsnf_torrent_path])):
            lgrsnf_path = f"e/lgrsnf/{lgrsnf_thousands_dir}/{lgrsnf_filename}"
            add_partner_servers(lgrsnf_path, '', aarecord, additional)

        additional['download_urls'].append((gettext('page.md5.box.download.lgrsnf'), f"http://library.lol/main/{aarecord['lgrsnf_book']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if aarecord.get('lgrsfic_book') is not None:
        lgrsfic_thousands_dir = (aarecord['lgrsfic_book']['id'] // 1000) * 1000
        lgrsfic_torrent_path = f"external/libgen_rs_fic/f_{lgrsfic_thousands_dir}.torrent" # Note: no leading zeroes
        lgrsfic_manually_synced = (lgrsfic_thousands_dir <= 3001000)
        lgrsfic_filename = f"{aarecord['lgrsfic_book']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
        if lgrsfic_manually_synced or (lgrsfic_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path):
            additional['torrent_paths'].append({ "collection": "libgen_rs_fic", "torrent_path": lgrsfic_torrent_path, "file_level1": lgrsfic_filename, "file_level2": "" })
        if lgrsfic_manually_synced or ((lgrsfic_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path) and (torrents_json_aa_currently_seeding_by_torrent_path[lgrsfic_torrent_path])):
            lgrsfic_path = f"e/lgrsfic/{lgrsfic_thousands_dir}/{lgrsfic_filename}"
            add_partner_servers(lgrsfic_path, '', aarecord, additional)

        additional['download_urls'].append((gettext('page.md5.box.download.lgrsfic'), f"http://library.lol/fiction/{aarecord['lgrsfic_book']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if aarecord.get('lgli_file') is not None:
        lglific_id = aarecord['lgli_file']['fiction_id']
        if lglific_id > 0:
            lglific_thousands_dir = (lglific_id // 1000) * 1000
            lglific_filename = f"{aarecord['lgli_file']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
            # Don't use torrents_json for this, because we have more files that don't get
            # torrented, because they overlap with our Z-Library torrents.
            # TODO: Verify overlap, and potentially add more torrents for what's missing?
            if lglific_thousands_dir >= 2201000 and lglific_thousands_dir <= 4259000:
                lglific_path = f"e/lglific/{lglific_thousands_dir}/{lglific_filename}"
                add_partner_servers(lglific_path, '', aarecord, additional)

            lglific_torrent_path = f"external/libgen_li_fic/f_{lglific_thousands_dir}.torrent" # Note: no leading zeroes
            if lglific_torrent_path in torrents_json_aa_currently_seeding_by_torrent_path:
                additional['torrent_paths'].append({ "collection": "libgen_li_fic", "torrent_path": lglific_torrent_path, "file_level1": lglific_filename, "file_level2": "" })

        scimag_id = aarecord['lgli_file']['scimag_id']
        if scimag_id > 0 and scimag_id <= 87599999: # 87637042 seems the max now in the libgenli db
            scimag_hundredthousand_dir = (scimag_id // 100000)
            scimag_thousand_dir = (scimag_id // 1000)
            scimag_filename = urllib.parse.quote(aarecord['lgli_file']['scimag_archive_path'].replace('\\', '/'))
            
            scimag_torrent_path = f"external/scihub/sm_{scimag_hundredthousand_dir:03}00000-{scimag_hundredthousand_dir:03}99999.torrent"
            additional['torrent_paths'].append({ "collection": "scihub", "torrent_path": scimag_torrent_path, "file_level1": f"libgen.scimag{scimag_thousand_dir:05}000-{scimag_thousand_dir:05}999.zip", "file_level2": scimag_filename })

            scimag_path = f"i/scimag/{scimag_hundredthousand_dir:03}00000/{scimag_thousand_dir:05}000/{scimag_filename}"
            add_partner_servers(scimag_path, 'scimag', aarecord, additional)

        lglicomics_id = aarecord['lgli_file']['comics_id']
        if lglicomics_id > 0 and lglicomics_id < 2566000:
            lglicomics_thousands_dir = (lglicomics_id // 1000) * 1000
            lglicomics_filename = f"{aarecord['lgli_file']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
            lglicomics_path = f"a/comics/{lglicomics_thousands_dir}/{lglicomics_filename}"
            add_partner_servers(lglicomics_path, '', aarecord, additional)
            additional['torrent_paths'].append({ "collection": "libgen_li_comics", "torrent_path": f"external/libgen_li_comics/c_{lglicomics_thousands_dir}.torrent", "file_level1": lglicomics_filename, "file_level2": "" }) # Note: no leading zero

        lglimagz_id = aarecord['lgli_file']['magz_id']
        if lglimagz_id > 0 and lglimagz_id < 1363000:
            lglimagz_thousands_dir = (lglimagz_id // 1000) * 1000
            lglimagz_filename = f"{aarecord['lgli_file']['md5'].lower()}.{aarecord['file_unified_data']['extension_best']}"
            lglimagz_path = f"y/magz/{lglimagz_thousands_dir}/{lglimagz_filename}"
            add_partner_servers(lglimagz_path, '', aarecord, additional)
            if lglimagz_id < 1000000:
                additional['torrent_paths'].append({ "collection": "libgen_li_magazines", "torrent_path": f"external/libgen_li_magazines/m_{lglimagz_thousands_dir}.torrent", "file_level1": lglimagz_filename, "file_level2": "" }) # Note: no leading zero

        # TODO:TRANSLATE
        additional['download_urls'].append((gettext('page.md5.box.download.lgli'), f"http://libgen.li/ads.php?md5={aarecord['lgli_file']['md5'].lower()}", (gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')) + ' <div style="margin-left: 24px" class="text-sm text-gray-500">their ads are known to contain malicious software, so use an ad blocker or don’t click ads</div>'))
        shown_click_get = True
    if (len(aarecord.get('ipfs_infos') or []) > 0) and (aarecord_id_split[0] == 'md5'):
        # additional['download_urls'].append((gettext('page.md5.box.download.ipfs_gateway', num=1), f"https://ipfs.eth.aragon.network/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}", gettext('page.md5.box.download.ipfs_gateway_extra')))

        additional['ipfs_urls'].append(f"https://cf-ipfs.com/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://ipfs.eth.aragon.network/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://zerolend.myfilebase.com/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://ccgateway.infura-ipfs.io/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://knownorigin.mypinata.cloud/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://storry.tv/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://ipfs-stg.fleek.co/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://cloudflare-ipfs.com/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://ipfs.io/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://snapshot.4everland.link/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://gateway.pinata.cloud/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://dweb.link/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://gw3.io/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://public.w3ipfs.aioz.network/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://ipfsgw.com/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://magic.decentralized-content.com/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://ipfs.raribleuserdata.com/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://www.gstop-content.com/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")
        additional['ipfs_urls'].append(f"https://atomichub-ipfs.com/ipfs/{aarecord['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={additional['filename_without_annas_archive']}")

        additional['download_urls'].append(("IPFS", f"/ipfs_downloads/{aarecord_id_split[1]}", ""))
    if aarecord.get('zlib_book') is not None and len(aarecord['zlib_book']['pilimi_torrent'] or '') > 0:
        zlib_path = make_temp_anon_zlib_path(aarecord['zlib_book']['zlibrary_id'], aarecord['zlib_book']['pilimi_torrent'])
        add_partner_servers(zlib_path, 'aa_exclusive' if (len(additional['fast_partner_urls']) == 0) else '', aarecord, additional)
        if "-zlib2-" in aarecord['zlib_book']['pilimi_torrent']:
            additional['torrent_paths'].append({ "collection": "zlib", "torrent_path": f"managed_by_aa/zlib/{aarecord['zlib_book']['pilimi_torrent']}", "file_level1": aarecord['zlib_book']['pilimi_torrent'].replace('.torrent', '.tar'), "file_level2": str(aarecord['zlib_book']['zlibrary_id']) })
        else:
            additional['torrent_paths'].append({ "collection": "zlib", "torrent_path": f"managed_by_aa/zlib/{aarecord['zlib_book']['pilimi_torrent']}", "file_level1": str(aarecord['zlib_book']['zlibrary_id']), "file_level2": "" })
    if (aarecord.get('aac_zlib3_book') is not None) and (aarecord['aac_zlib3_book']['file_aacid'] is not None):
        zlib_path = make_temp_anon_aac_path("u/zlib3_files", aarecord['aac_zlib3_book']['file_aacid'], aarecord['aac_zlib3_book']['file_data_folder'])
        add_partner_servers(zlib_path, 'aa_exclusive' if (len(additional['fast_partner_urls']) == 0) else '', aarecord, additional)
        additional['torrent_paths'].append({ "collection": "zlib", "torrent_path": f"managed_by_aa/annas_archive_data__aacid/{aarecord['aac_zlib3_book']['file_data_folder']}.torrent", "file_level1": aarecord['aac_zlib3_book']['file_aacid'], "file_level2": "" })
    if aarecord.get('aac_zlib3_book') is not None:
        # additional['download_urls'].append((gettext('page.md5.box.download.zlib_tor'), f"http://loginzlib2vrak5zzpcocc3ouizykn6k5qecgj2tzlnab5wcbqhembyd.onion/md5/{aarecord['aac_zlib3_book']['md5_reported'].lower()}", gettext('page.md5.box.download.zlib_tor_extra')))
        additional['download_urls'].append(("Z-Library", f"https://z-lib.gs/md5/{aarecord['aac_zlib3_book']['md5_reported'].lower()}", ""))
    if (aarecord.get('zlib_book') is not None) and (aarecord.get('aac_zlib3_book') is None):
        # additional['download_urls'].append((gettext('page.md5.box.download.zlib_tor'), f"http://loginzlib2vrak5zzpcocc3ouizykn6k5qecgj2tzlnab5wcbqhembyd.onion/md5/{aarecord['zlib_book']['md5_reported'].lower()}", gettext('page.md5.box.download.zlib_tor_extra')))
        additional['download_urls'].append(("Z-Library", f"https://z-lib.gs/md5/{aarecord['zlib_book']['md5_reported'].lower()}", ""))
    if aarecord.get('ia_record') is not None:
        ia_id = aarecord['ia_record']['ia_id']
        printdisabled_only = aarecord['ia_record']['aa_ia_derived']['printdisabled_only']
        additional['download_urls'].append((gettext('page.md5.box.download.ia_borrow'), f"https://archive.org/details/{ia_id}", gettext('page.md5.box.download.print_disabled_only') if printdisabled_only else ''))
    for doi in (aarecord['file_unified_data']['identifiers_unified'].get('doi') or []):
        if doi not in linked_dois:
            additional['download_urls'].append((gettext('page.md5.box.download.scihub', doi=doi), f"https://sci-hub.ru/{doi}", gettext('page.md5.box.download.scihub_maybe')))
    if aarecord_id_split[0] == 'md5':
        for torrent_path in additional['torrent_paths']:
            # path = "/torrents"
            # group = torrent_group_data_from_file_path(f"torrents/{torrent_path}")['group']
            # path += f"#{group}"
            # TODO:TRANSLATE
            files_html = f'collection <a href="/torrents#{torrent_path["collection"]}">“{torrent_path["collection"]}”</a> → torrent <a href="/dyn/small_file/torrents/{torrent_path["torrent_path"]}">“{torrent_path["torrent_path"].rsplit("/", 1)[-1]}”</a>'
            if len(torrent_path['file_level1']) > 0:
                files_html += f" →&nbsp;file&nbsp;“{torrent_path['file_level1']}”"
            if len(torrent_path['file_level2']) > 0:
                files_html += f"&nbsp;(extract) →&nbsp;file&nbsp;“{torrent_path['file_level2']}”"
            additional['download_urls'].append((gettext('page.md5.box.download.bulk_torrents'), f"/torrents#{torrent_path['collection']}", gettext('page.md5.box.download.experts_only') + f' <div style="margin-left: 24px" class="text-sm text-gray-500">{files_html}</em></div>'))
        if len(additional['torrent_paths']) == 0:
            if additional['has_aa_downloads'] == 0:
                additional['download_urls'].append(("", "", 'Bulk torrents not yet available for this file. If you have this file, help out by <a href="/faq#upload">uploading</a>.'))
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

    additional['has_scidb'] = 0
    scidb_info = allthethings.utils.scidb_info(aarecord, additional)
    if scidb_info is not None:
        additional['fast_partner_urls'] = [(gettext('page.md5.box.download.scidb'), f"/scidb?doi={scidb_info['doi']}", gettext('common.md5.servers.no_browser_verification'))] + additional['fast_partner_urls']
        additional['slow_partner_urls'] = [(gettext('page.md5.box.download.scidb'), f"/scidb?doi={scidb_info['doi']}", gettext('common.md5.servers.no_browser_verification'))] + additional['slow_partner_urls']
        additional['has_scidb'] = 1

    return additional

def add_additional_to_aarecord(aarecord):
    return { **aarecord['_source'], '_score': (aarecord.get('_score') or 0.0), 'additional': get_additional_for_aarecord(aarecord['_source']) }

@page.get("/md5/<string:md5_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def md5_page(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]
    return render_aarecord(f"md5:{canonical_md5}")

@page.get("/ia/<string:ia_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def isbn_page(isbn_input):
    return redirect(f"/isbndb/{isbn_input}", code=302)

@page.get("/isbndb/<string:isbn_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def isbndb_page(isbn_input):
    return render_aarecord(f"isbn:{isbn_input}")

@page.get("/ol/<string:ol_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def ol_page(ol_input):
    return render_aarecord(f"ol:{ol_input}")

@page.get("/doi/<path:doi_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def doi_page(doi_input):
    return render_aarecord(f"doi:{doi_input}")

@page.get("/oclc/<path:oclc_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def oclc_page(oclc_input):
    return render_aarecord(f"oclc:{oclc_input}")

@page.get("/duxiu_ssid/<path:duxiu_ssid_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def duxiu_ssid_page(duxiu_ssid_input):
    return render_aarecord(f"duxiu_ssid:{duxiu_ssid_input}")

@page.get("/cadal_ssno/<path:cadal_ssno_input>")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def cadal_ssno_page(cadal_ssno_input):
    return render_aarecord(f"cadal_ssno:{cadal_ssno_input}")

def render_aarecord(record_id):
    if allthethings.utils.DOWN_FOR_MAINTENANCE:
        return render_template("page/maintenance.html", header_active="")

    with Session(engine) as session:
        ids = [record_id]
        if not allthethings.utils.validate_aarecord_ids(ids):
            return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=record_id), 404

        aarecords = get_aarecords_elasticsearch(ids)
        if aarecords is None:
            return render_template("page/aarecord_issue.html", header_active="search"), 500
        if len(aarecords) == 0:
            code = record_id.replace('isbn:', 'isbn13:')
            return redirect(f'/search?q="{code}"', code=301)
            # return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=record_id), 404

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

@page.get("/scidb")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60*3)
def scidb_home_page():
    return render_template("page/scidb_home.html", header_active="home/scidb", doi_input=request.args.get('doi'))

@page.post("/scidb")
@allthethings.utils.no_cache()
def scidb_redirect_page():
    doi_input = request.args.get("doi", "").strip()
    return redirect(f"/scidb/{doi_input}", code=302)

@page.get("/scidb/<path:doi_input>")
@page.post("/scidb/<path:doi_input>")
@allthethings.utils.no_cache()
def scidb_page(doi_input):
    # account_id = allthethings.utils.get_account_id(request.cookies)
    # if account_id is None:
    #     return render_template("page/login_to_view.html", header_active="")

    doi_input = doi_input.strip()

    if not doi_input.startswith('10.'):
        if '10.' in doi_input:
            return redirect(f"/scidb/{doi_input[doi_input.find('10.'):].strip()}", code=302)    
        return redirect(f"/search?index=journals&q={doi_input}", code=302)

    if allthethings.utils.doi_is_isbn(doi_input):
        return redirect(f'/search?index=journals&q="doi:{doi_input}"', code=302)

    fast_scidb = False
    # verified = False
    # if str(request.args.get("scidb_verified") or "") == "1":
    #     verified = True
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is not None:
        with Session(mariapersist_engine) as mariapersist_session:
            account_fast_download_info = allthethings.utils.get_account_fast_download_info(mariapersist_session, account_id)
            if account_fast_download_info is not None:
                fast_scidb = True
            # verified = True
    # if not verified:
    #     return redirect(f"/scidb/{doi_input}?scidb_verified=1", code=302)

    with Session(engine) as session:
        try:
            search_results_raw1 = es_aux.search(
                index=allthethings.utils.all_virtshards_for_index("aarecords_journals"),
                size=50,
                query={ "term": { "search_only_fields.search_doi": doi_input } },
                timeout="2s",
            )
            search_results_raw2 = es.search(
                index=allthethings.utils.all_virtshards_for_index("aarecords"),
                size=50,
                query={ "term": { "search_only_fields.search_doi": doi_input } },
                timeout="2s",
            )
        except Exception as err:
            return redirect(f'/search?index=journals&q="doi:{doi_input}"', code=302)
        aarecords = [add_additional_to_aarecord(aarecord) for aarecord in search_results_raw1['hits']['hits']+search_results_raw2['hits']['hits']]
        aarecords_and_infos = [(aarecord, allthethings.utils.scidb_info(aarecord)) for aarecord in aarecords if allthethings.utils.scidb_info(aarecord) is not None]
        aarecords_and_infos.sort(key=lambda aarecord_and_info: aarecord_and_info[1]['priority'])

        if len(aarecords_and_infos) == 0:
            return redirect(f'/search?index=journals&q="doi:{doi_input}"', code=302)

        aarecord, scidb_info = aarecords_and_infos[0]

        pdf_url = None
        download_url = None
        path_info = scidb_info['path_info']
        if path_info:
            domain = random.choice(allthethings.utils.SCIDB_SLOW_DOWNLOAD_DOMAINS)
            targeted_seconds_multiplier = 1.0
            minimum = 100
            maximum = 500
            if fast_scidb:
                domain = random.choice(allthethings.utils.SCIDB_FAST_DOWNLOAD_DOMAINS)
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
            "fast_scidb": fast_scidb,
        }
        return render_template("page/scidb.html", **render_fields)

@page.get("/db/aarecord/<path:aarecord_id>.json")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def md5_json(aarecord_id):
    aarecords = get_aarecords_elasticsearch([aarecord_id])
    if aarecords is None:
        return '"Page loading issue"', 500
    if len(aarecords) == 0:
        return "{}", 404
    
    aarecord_comments = {
        "id": ("before", ["File from the combined collections of Anna's Archive.",
                           "More details at https://annas-archive.gs/datasets",
                           allthethings.utils.DICT_COMMENTS_NO_API_DISCLAIMER]),
        "lgrsnf_book": ("before", ["Source data at: https://annas-archive.gs/db/lgrsnf/<id>.json"]),
        "lgrsfic_book": ("before", ["Source data at: https://annas-archive.gs/db/lgrsfic/<id>.json"]),
        "lgli_file": ("before", ["Source data at: https://annas-archive.gs/db/lgli/<f_id>.json"]),
        "zlib_book": ("before", ["Source data at: https://annas-archive.gs/db/zlib/<zlibrary_id>.json"]),
        "aac_zlib3_book": ("before", ["Source data at: https://annas-archive.gs/db/aac_zlib3/<zlibrary_id>.json"]),
        "ia_record": ("before", ["Source data at: https://annas-archive.gs/db/ia/<ia_id>.json"]),
        "isbndb": ("before", ["Source data at: https://annas-archive.gs/db/isbndb/<isbn13>.json"]),
        "ol": ("before", ["Source data at: https://annas-archive.gs/db/ol/<ol_edition>.json"]),
        "scihub_doi": ("before", ["Source data at: https://annas-archive.gs/db/scihub_doi/<doi>.json"]),
        "oclc": ("before", ["Source data at: https://annas-archive.gs/db/oclc/<oclc>.json"]),
        "duxiu": ("before", ["Source data at: https://annas-archive.gs/db/duxiu_ssid/<duxiu_ssid>.json or https://annas-archive.gs/db/cadal_ssno/<cadal_ssno>.json or https://annas-archive.gs/db/duxiu_md5/<md5>.json"]),
        "file_unified_data": ("before", ["Combined data by Anna's Archive from the various source collections, attempting to get pick the best field where possible."]),
        "ipfs_infos": ("before", ["Data about the IPFS files."]),
        "search_only_fields": ("before", ["Data that is used during searching."]),
        "additional": ("before", ["Data that is derived at a late stage, and not stored in the search index."]),
    }
    aarecord = add_comments_to_dict(aarecords[0], aarecord_comments)

    aarecord['additional'].pop('fast_partner_urls')
    aarecord['additional'].pop('slow_partner_urls')

    return allthethings.utils.nice_json(aarecord), {'Content-Type': 'text/json; charset=utf-8'}

# IMPORTANT: Keep in sync with api_md5_fast_download.
@page.get("/fast_download/<string:md5_input>/<int:path_index>/<int:domain_index>")
@allthethings.utils.no_cache()
def md5_fast_download(md5_input, path_index, domain_index):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]) or canonical_md5 != md5_input:
        return redirect(f"/md5/{md5_input}", code=302)
    with Session(engine) as session:
        aarecords = get_aarecords_elasticsearch([f"md5:{canonical_md5}"])
        if aarecords is None:
            return render_template("page/aarecord_issue.html", header_active="search"), 500
        if len(aarecords) == 0:
            return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=md5_input), 404
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
    return redirect(url, code=302)

def compute_download_speed(targeted_seconds, filesize, minimum, maximum):
    return min(maximum, max(minimum, int(filesize/1000/targeted_seconds)))

@page.get("/slow_download/<string:md5_input>/<int:path_index>/<int:domain_index>")
@page.post("/slow_download/<string:md5_input>/<int:path_index>/<int:domain_index>")
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

    # We blocked Cloudflare because otherwise VPN users circumvent the CAPTCHA.
    # But it also blocks some TOR users who get Cloudflare exit nodes.
    # Perhaps not as necessary anymore now that we have waitlists, and extra throttling by IP.
    # if allthethings.utils.is_canonical_ip_cloudflare(data_ip):
    #     return render_template(
    #         "page/partner_download.html",
    #         header_active="search",
    #         no_cloudflare=True,
    #         canonical_md5=canonical_md5,
    #     )

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]) or canonical_md5 != md5_input:
        return redirect(f"/md5/{md5_input}", code=302)

    data_pseudo_ipv4 = allthethings.utils.pseudo_ipv4_bytes(request.remote_addr)
    account_id = allthethings.utils.get_account_id(request.cookies)
    data_hour_since_epoch = int(time.time() / 3600)

    with Session(engine) as session:
        with Session(mariapersist_engine) as mariapersist_session:
            aarecords = get_aarecords_elasticsearch([f"md5:{canonical_md5}"])
            if aarecords is None:
                return render_template("page/aarecord_issue.html", header_active="search"), 500
            if len(aarecords) == 0:
                return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=md5_input), 404
            aarecord = aarecords[0]
            try:
                domain_slow = allthethings.utils.SLOW_DOWNLOAD_DOMAINS[domain_index]
                domain_slowest = allthethings.utils.SLOWEST_DOWNLOAD_DOMAINS[domain_index]
                path_info = aarecord['additional']['partner_url_paths'][path_index]
            except:
                return redirect(f"/md5/{md5_input}", code=302)

            cursor = mariapersist_session.connection().connection.cursor(pymysql.cursors.DictCursor)
            cursor.execute('SELECT SUM(count) AS count FROM mariapersist_slow_download_access_pseudo_ipv4_hourly WHERE pseudo_ipv4 = %(pseudo_ipv4)s AND hour_since_epoch > %(hour_since_epoch)s', { "pseudo_ipv4": data_pseudo_ipv4, "hour_since_epoch": data_hour_since_epoch-24 })
            daily_download_count_from_ip = ((cursor.fetchone() or {}).get('count') or 0)
            # minimum = 10
            # maximum = 100
            # minimum = 100
            # maximum = 300
            # targeted_seconds_multiplier = 1.0
            warning = False
            # These waitlist_max_wait_time_seconds values must be multiples, under the current modulo scheme.
            # Also WAITLIST_DOWNLOAD_WINDOW_SECONDS gets subtracted from it.
            waitlist_max_wait_time_seconds = 10*60
            domain = domain_slow
            if daily_download_count_from_ip >= 100:
                # targeted_seconds_multiplier = 2.0
                # minimum = 20
                # maximum = 100
                waitlist_max_wait_time_seconds *= 2
                # warning = True
                domain = domain_slowest
            elif daily_download_count_from_ip >= 30:
                domain = domain_slowest

            if allthethings.utils.SLOW_DOWNLOAD_DOMAINS_SLIGHTLY_FASTER[domain_index]:
                WAITLIST_DOWNLOAD_WINDOW_SECONDS = 2*60
                hashed_md5_bytes = int.from_bytes(hashlib.sha256(bytes.fromhex(canonical_md5) + HASHED_DOWNLOADS_SECRET_KEY).digest(), byteorder='big')
                seconds_since_epoch = int(time.time())
                wait_seconds = ((hashed_md5_bytes-seconds_since_epoch) % waitlist_max_wait_time_seconds) - WAITLIST_DOWNLOAD_WINDOW_SECONDS
                if wait_seconds > 1:
                    return render_template(
                        "page/partner_download.html",
                        header_active="search",
                        wait_seconds=wait_seconds,
                        canonical_md5=canonical_md5,
                        daily_download_count_from_ip=daily_download_count_from_ip,
                    )

            # speed = compute_download_speed(path_info['targeted_seconds']*targeted_seconds_multiplier, aarecord['file_unified_data']['filesize_best'], minimum, maximum)
            speed = 10000

            url = 'https://' + domain + '/' + allthethings.utils.make_anon_download_uri(True, speed, path_info['path'], aarecord['additional']['filename'], domain)

            data_md5 = bytes.fromhex(canonical_md5)
            mariapersist_session.connection().execute(text('INSERT IGNORE INTO mariapersist_slow_download_access (md5, ip, account_id, pseudo_ipv4) VALUES (:md5, :ip, :account_id, :pseudo_ipv4)').bindparams(md5=data_md5, ip=data_ip, account_id=account_id, pseudo_ipv4=data_pseudo_ipv4))
            mariapersist_session.commit()
            mariapersist_session.connection().execute(text('INSERT INTO mariapersist_slow_download_access_pseudo_ipv4_hourly (pseudo_ipv4, hour_since_epoch, count) VALUES (:pseudo_ipv4, :hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(hour_since_epoch=data_hour_since_epoch, pseudo_ipv4=data_pseudo_ipv4))
            mariapersist_session.commit()

            return render_template(
                "page/partner_download.html",
                header_active="search",
                url=url,
                warning=warning,
                canonical_md5=canonical_md5,
                daily_download_count_from_ip=daily_download_count_from_ip,
                # pseudo_ipv4=f"{data_pseudo_ipv4[0]}.{data_pseudo_ipv4[1]}.{data_pseudo_ipv4[2]}.{data_pseudo_ipv4[3]}",
            )

@page.get("/ipfs_downloads/<string:md5_input>")
@allthethings.utils.no_cache()
def ipfs_downloads(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if (request.headers.get('cf-worker') or '') != '':
        return redirect(f"/md5/{md5_input}", code=302)

    data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
    if allthethings.utils.is_canonical_ip_cloudflare(data_ip):
        return redirect(f"/md5/{md5_input}", code=302)

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]) or canonical_md5 != md5_input:
        return redirect(f"/md5/{md5_input}", code=302)

    aarecords = get_aarecords_elasticsearch([f"md5:{canonical_md5}"])
    if aarecords is None:
        return render_template("page/aarecord_issue.html", header_active="search"), 500
    if len(aarecords) == 0:
        return render_template("page/aarecord_not_found.html", header_active="search", not_found_field=md5_input), 404
    aarecord = aarecords[0]
    try:
        ipfs_urls = aarecord['additional']['ipfs_urls']
    except:
        return redirect(f"/md5/{md5_input}", code=302)

    return render_template(
        "page/ipfs_downloads.html",
        header_active="search",
        ipfs_urls=ipfs_urls,
        canonical_md5=canonical_md5,
    )

def search_query_aggs(search_index_long):
    return {
        "search_content_type": { "terms": { "field": "search_only_fields.search_content_type", "size": 200 } },
        "search_extension": { "terms": { "field": "search_only_fields.search_extension", "size": 9 } },
        "search_access_types": { "terms": { "field": "search_only_fields.search_access_types", "size": 100 } },
        "search_record_sources": { "terms": { "field": "search_only_fields.search_record_sources", "size": 100 } },
        "search_most_likely_language_code": { "terms": { "field": "search_only_fields.search_most_likely_language_code", "size": 70 } },
    }

@cachetools.cached(cache=cachetools.TTLCache(maxsize=30000, ttl=60*60))
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

number_of_search_primary_exceptions = 0
@page.get("/search")
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
def search_page():
    global number_of_search_primary_exceptions

    if allthethings.utils.DOWN_FOR_MAINTENANCE:
        return render_template("page/maintenance.html", header_active="")

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
    search_desc = (request.args.get("desc", "").strip() == "1")
    page_value_str = request.args.get("page", "").strip()
    page_value = 1
    try:
        page_value = int(page_value_str)
    except:
        pass
    sort_value = request.args.get("sort", "").strip()
    search_index_short = request.args.get("index", "").strip()
    if search_index_short not in allthethings.utils.SEARCH_INDEX_SHORT_LONG_MAPPING:
        search_index_short = ""
    search_index_long = allthethings.utils.SEARCH_INDEX_SHORT_LONG_MAPPING[search_index_short]
    if search_index_short == 'digital_lending':
        filter_values['search_extension'] = []

    # Correct ISBN by removing spaces so our search for them actually works.
    potential_isbn = search_input.replace('-', '')
    if search_input != potential_isbn and (isbnlib.is_isbn13(potential_isbn) or isbnlib.is_isbn10(potential_isbn)):
        return redirect(f"/search?q={potential_isbn}", code=302)

    post_filter = []
    for key, values in filter_values.items():
        if values != []:
            post_filter.append({ "terms": { f"search_only_fields.{key}": [value if value != '_empty' else '' for value in values] } })

    custom_search_sorting = ['_score']
    if sort_value == "newest":
        custom_search_sorting = [{ "search_only_fields.search_year": "desc" }, '_score']
    if sort_value == "oldest":
        custom_search_sorting = [{ "search_only_fields.search_year": "asc" }, '_score']
    if sort_value == "largest":
        custom_search_sorting = [{ "search_only_fields.search_filesize": "desc" }, '_score']
    if sort_value == "smallest":
        custom_search_sorting = [{ "search_only_fields.search_filesize": "asc" }, '_score']
    if sort_value == "newest_added":
        custom_search_sorting = [{ "search_only_fields.search_added_date": "desc" }, '_score']
    if sort_value == "oldest_added":
        custom_search_sorting = [{ "search_only_fields.search_added_date": "asc" }, '_score']

    main_search_fields = []
    if len(search_input) > 0:
        main_search_fields.append(('search_only_fields.search_text', search_input))
        if search_desc:
            main_search_fields.append(('search_only_fields.search_description_comments', search_input))

    specific_search_fields_mapping = get_specific_search_fields_mapping(get_locale())

    specific_search_fields = []
    for number in range(1,10):
        term_type = request.args.get(f"termtype_{number}") or ""
        term_val = request.args.get(f"termval_{number}") or ""
        if (len(term_val) > 0) and (term_type in specific_search_fields_mapping):
            specific_search_fields.append((term_type, term_val))

    if (len(main_search_fields) == 0) and (len(specific_search_fields) == 0):
        search_query = { "match_all": {} }
        if custom_search_sorting == ['_score']:
            custom_search_sorting = [{ "search_only_fields.search_added_date": "desc" }, '_score']
    else:
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
                                { 
                                    "bool": {
                                        "must": [
                                            {
                                                "bool": {
                                                    "should": [{ "match_phrase": { field_name: { "query": field_value } } } for field_name, field_value in main_search_fields ],
                                                },
                                            },
                                            *[{ "match_phrase": { f'search_only_fields.search_{field_name}': { "query": field_value } } } for field_name, field_value in specific_search_fields ],
                                        ],
                                    },
                                },
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
                                    "bool": {
                                        "must": [
                                            {
                                                "bool": {
                                                    "should": [{ "simple_query_string": { "query": field_value, "fields": [field_name], "default_operator": "and" } } for field_name, field_value in main_search_fields ],
                                                },
                                            },
                                            *[{ "simple_query_string": { "query": field_value, "fields": [f'search_only_fields.search_{field_name}'], "default_operator": "and" } } for field_name, field_value in specific_search_fields ],
                                        ],
                                        "boost": 1.0/100000.0,
                                    },
                                },
                            ],
                        },
                    },
                ],
            },
        }

    max_display_results = 100

    es_handle = allthethings.utils.SEARCH_INDEX_TO_ES_MAPPING[search_index_long]

    primary_search_searches = [
        { "index": allthethings.utils.all_virtshards_for_index(search_index_long) },
        {
            "size": max_display_results, 
            "from": (page_value-1)*max_display_results,
            "query": search_query,
            "aggs": search_query_aggs(search_index_long),
            "post_filter": { "bool": { "filter": post_filter } },
            "sort": custom_search_sorting,
            # "track_total_hits": False, # Set to default
            "timeout": ES_TIMEOUT_PRIMARY,
            # "knn": { "field": "search_only_fields.search_e5_small_query", "query_vector": list(map(float, get_e5_small_model().encode(f"query: {search_input}", normalize_embeddings=True))), "k": 10, "num_candidates": 1000 },
        },
    ]

    search_names = ['search1_primary']
    search_results_raw = {'responses': [{} for search_name in search_names]}
    for attempt in [1, 2]:
        try:
            search_results_raw = dict(es_handle.msearch(
                request_timeout=5,
                max_concurrent_searches=64,
                max_concurrent_shard_requests=64,
                searches=primary_search_searches,
            ))
            number_of_search_primary_exceptions = 0
            break
        except Exception as err:
            print(f"Warning: another attempt during primary ES search {search_input=}")
            if attempt >= 2:
                had_es_timeout = True
                had_primary_es_timeout = True
                had_fatal_es_timeout = True

                number_of_search_primary_exceptions += 1
                if number_of_search_primary_exceptions > 5:
                    print(f"Exception during primary ES search {attempt=} {search_input=} ///// {repr(err)} ///// {traceback.format_exc()}\n")
                else:
                    print("Haven't reached number_of_search_primary_exceptions limit yet, so not raising")
                break
    for num, response in enumerate(search_results_raw['responses']):
        es_stats.append({ 'name': search_names[num], 'took': response.get('took'), 'timed_out': response.get('timed_out'), 'searches': primary_search_searches })
        if response.get('timed_out') or (response == {}):
            had_es_timeout = True
            had_primary_es_timeout = True
    primary_response_raw = search_results_raw['responses'][0]

    display_lang = allthethings.utils.get_base_lang_code(get_locale())
    try:
        all_aggregations, all_aggregations_es_stat = all_search_aggs(display_lang, search_index_long)
    except:
        return 'Page loading issue', 500
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
    primary_hits_total_obj = { 'value': 0, 'relation': 'eq' }
    if 'hits' in primary_response_raw:
        search_aarecords = [add_additional_to_aarecord(aarecord_raw) for aarecord_raw in primary_response_raw['hits']['hits'] if aarecord_raw['_id'] not in search_filtered_bad_aarecord_ids]
        primary_hits_total_obj = primary_response_raw['hits']['total']

    additional_search_aarecords = []
    additional_display_results = max(0, max_display_results-len(search_aarecords))
    if (page_value == 1) and (additional_display_results > 0) and (len(specific_search_fields) == 0):
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
                            "sort": custom_search_sorting,
                            "track_total_hits": False,
                            "timeout": ES_TIMEOUT,
                        },
                        # Then do an "OR" query, but this time with the filters again.
                        { "index": allthethings.utils.all_virtshards_for_index(search_index_long) },
                        {
                            "size": additional_display_results,
                            "query": {"bool": { "must": { "multi_match": { "query": search_input, "fields": "search_only_fields.search_text" }  }, "filter": post_filter } },
                            # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                            "sort": ['_score'],
                            "track_total_hits": False,
                            "timeout": ES_TIMEOUT,
                        },
                        # If we still don't have enough, do another OR query but this time without filters.
                        { "index": allthethings.utils.all_virtshards_for_index(search_index_long) },
                        {
                            "size": additional_display_results,
                            "query": {"bool": { "must": { "multi_match": { "query": search_input, "fields": "search_only_fields.search_text" }  } } },
                            # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                            "sort": ['_score'],
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
                    print(f"Warning: issue during secondary ES search {search_input=}")
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

    primary_hits_pages = 1 + (max(0, primary_hits_total_obj['value'] - 1) // max_display_results)

    search_dict = {}
    search_dict['search_aarecords'] = search_aarecords[0:max_display_results]
    search_dict['additional_search_aarecords'] = additional_search_aarecords[0:additional_display_results]
    search_dict['max_search_aarecords_reached'] = (len(search_aarecords) >= max_display_results)
    search_dict['max_additional_search_aarecords_reached'] = (len(additional_search_aarecords) >= additional_display_results)
    search_dict['aggregations'] = aggregations
    search_dict['sort_value'] = sort_value
    search_dict['search_index_short'] = search_index_short
    search_dict['es_stats_json'] = es_stats
    search_dict['had_primary_es_timeout'] = had_primary_es_timeout
    search_dict['had_es_timeout'] = had_es_timeout
    search_dict['had_fatal_es_timeout'] = had_fatal_es_timeout
    search_dict['page_value'] = page_value
    search_dict['primary_hits_pages'] = primary_hits_pages
    search_dict['pagination_pages_with_dots_large'] = allthethings.utils.build_pagination_pages_with_dots(primary_hits_pages, page_value, True)
    search_dict['pagination_pages_with_dots_small'] = allthethings.utils.build_pagination_pages_with_dots(primary_hits_pages, page_value, False)
    search_dict['pagination_base_url'] = request.path + '?' + urllib.parse.urlencode([(k,v) for k,v in request.args.items() if k != 'page'] + [('page', '')])
    search_dict['primary_hits_total_obj'] = primary_hits_total_obj
    search_dict['max_display_results'] = max_display_results
    search_dict['search_desc'] = search_desc
    search_dict['specific_search_fields'] = specific_search_fields
    search_dict['specific_search_fields_mapping'] = specific_search_fields_mapping

    g.hide_search_bar = True

    r = make_response((render_template(
            "page/search.html",
            header_active="home/search",
            search_input=search_input,
            search_dict=search_dict,
        ), 200))
    if had_es_timeout:
        r.headers.add('Cache-Control', 'no-cache')
    return r
