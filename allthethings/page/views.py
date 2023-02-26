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

from flask import g, Blueprint, __version__, render_template, make_response, redirect, request
from allthethings.extensions import engine, es, babel, ZlibBook, ZlibIsbn, IsbndbIsbns, LibgenliEditions, LibgenliEditionsAddDescr, LibgenliEditionsToFiles, LibgenliElemDescr, LibgenliFiles, LibgenliFilesAddDescr, LibgenliPublishers, LibgenliSeries, LibgenliSeriesAddDescr, LibgenrsDescription, LibgenrsFiction, LibgenrsFictionDescription, LibgenrsFictionHashes, LibgenrsHashes, LibgenrsTopics, LibgenrsUpdated, OlBase, ComputedAllMd5s
from sqlalchemy import select, func, text
from sqlalchemy.dialects.mysql import match
from sqlalchemy.orm import defaultload, Session
from flask_babel import gettext, ngettext, get_translations, force_locale, get_locale

import allthethings.utils

page = Blueprint("page", __name__, template_folder="templates")

# Per https://annas-software.org/AnnaArchivist/annas-archive/-/issues/37
search_filtered_bad_md5s = [
    "b0647953a182171074873b61200c71dd",
    "820a4f8961ae0a76ad265f1678b7dfa5",

    # Likely CSAM
    "d897ffc4e64cbaeae53a6005b6f155cc",
    "8ae28a86719e3a4400145ac18b621efd",
    "285171dbb2d1d56aa405ad3f5e1bc718",
    "8ac4facd6562c28d7583d251aa2c9020",
    "6c1b1ea486960a1ad548cd5c02c465a1",
    "414e8f3a8bc0f63de37cd52bd6d8701e",
    "c6cddcf83c558b758094e06b97067c89",
    "5457b152ef9a91ca3e2d8b3a2309a106",
    "02973f6d111c140510fcdf84b1d00c35",
    "d4c01f9370c5ac93eb5ee5c2037ac794",
    "08499f336fbf8d31f8e7fadaaa517477",
]

ES_TIMEOUT = "5s"

# Retrieved from https://openlibrary.org/config/edition.json on 2022-10-11
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
# * http://localhost:8000/lgrs/nf/288054
# * http://localhost:8000/lgrs/nf/3175616
# * http://localhost:8000/lgrs/nf/2933905
# * http://localhost:8000/lgrs/nf/1125703
# * http://localhost:8000/lgrs/nf/59
# * http://localhost:8000/lgrs/nf/1195487
# * http://localhost:8000/lgrs/nf/1360257
# * http://localhost:8000/lgrs/nf/357571
# * http://localhost:8000/lgrs/nf/2425562
# * http://localhost:8000/lgrs/nf/3354081
# * http://localhost:8000/lgrs/nf/3357578
# * http://localhost:8000/lgrs/nf/3357145
# * http://localhost:8000/lgrs/nf/2040423
# * http://localhost:8000/lgrs/fic/1314135
# * http://localhost:8000/lgrs/fic/25761
# * http://localhost:8000/lgrs/fic/2443846
# * http://localhost:8000/lgrs/fic/2473252
# * http://localhost:8000/lgrs/fic/2340232
# * http://localhost:8000/lgrs/fic/1122239
# * http://localhost:8000/lgrs/fic/6862
# * http://localhost:8000/lgli/file/100
# * http://localhost:8000/lgli/file/1635550
# * http://localhost:8000/lgli/file/94069002
# * http://localhost:8000/lgli/file/40122
# * http://localhost:8000/lgli/file/21174
# * http://localhost:8000/lgli/file/91051161
# * http://localhost:8000/lgli/file/733269
# * http://localhost:8000/lgli/file/156965
# * http://localhost:8000/lgli/file/10000000
# * http://localhost:8000/lgli/file/933304
# * http://localhost:8000/lgli/file/97559799
# * http://localhost:8000/lgli/file/3756440
# * http://localhost:8000/lgli/file/91128129
# * http://localhost:8000/lgli/file/44109
# * http://localhost:8000/lgli/file/2264591
# * http://localhost:8000/lgli/file/151611
# * http://localhost:8000/lgli/file/1868248
# * http://localhost:8000/lgli/file/1761341
# * http://localhost:8000/lgli/file/4031847
# * http://localhost:8000/lgli/file/2827612
# * http://localhost:8000/lgli/file/2096298
# * http://localhost:8000/lgli/file/96751802
# * http://localhost:8000/lgli/file/5064830
# * http://localhost:8000/lgli/file/1747221
# * http://localhost:8000/lgli/file/1833886
# * http://localhost:8000/lgli/file/3908879
# * http://localhost:8000/lgli/file/41752
# * http://localhost:8000/lgli/file/97768237
# * http://localhost:8000/lgli/file/4031335
# * http://localhost:8000/lgli/file/1842179
# * http://localhost:8000/lgli/file/97562793
# * http://localhost:8000/lgli/file/4029864
# * http://localhost:8000/lgli/file/2834701
# * http://localhost:8000/lgli/file/97562143
# * http://localhost:8000/isbn/9789514596933
# * http://localhost:8000/isbn/9780000000439
# * http://localhost:8000/isbn/9780001055506
# * http://localhost:8000/isbn/9780316769174
# * http://localhost:8000/md5/8fcb740b8c13f202e89e05c4937c09ac


def looks_like_doi(string):
    return string.startswith('10.') and ('/' in string) and (' ' not in string)

# Example: http://193.218.118.109/zlib2/pilimi-zlib2-0-14679999-extra/11078831.pdf
def make_temp_anon_zlib_link(domain, zlibrary_id, pilimi_torrent, extension):
    prefix = "zlib1"
    if "-zlib2-" in pilimi_torrent:
        prefix = "zlib2"
    return f"{domain}/{prefix}/{pilimi_torrent.replace('.torrent', '')}/{zlibrary_id}.{extension}"

def make_normalized_filename(slug_info, extension, collection, id):
    slug = slugify.slugify(slug_info, allow_unicode=True, max_length=50, word_boundary=True)
    return f"{slug}--annas-archive--{collection}-{id}.{extension}"


def make_sanitized_isbns(potential_isbns):
    sanitized_isbns = set()
    for potential_isbn in potential_isbns:
        isbn = potential_isbn.replace('-', '').replace(' ', '')
        if isbnlib.is_isbn10(isbn):
            sanitized_isbns.add(isbn)
            sanitized_isbns.add(isbnlib.to_isbn13(isbn))
        if isbnlib.is_isbn13(isbn):
            sanitized_isbns.add(isbn)
            isbn10 = isbnlib.to_isbn10(isbn)
            if isbnlib.is_isbn10(isbn10 or ''):
                sanitized_isbns.add(isbn10)
    return list(sanitized_isbns)

def make_isbns_rich(sanitized_isbns):
    rich_isbns = []
    for isbn in sanitized_isbns:
        if len(isbn) == 13:
            potential_isbn10 = isbnlib.to_isbn10(isbn)
            if isbnlib.is_isbn10(potential_isbn10):
                rich_isbns.append((isbn, potential_isbn10, isbnlib.mask(isbn), isbnlib.mask(potential_isbn10)))
            else:
                rich_isbns.append((isbn, '', isbnlib.mask(isbn), ''))
    return rich_isbns

def strip_description(description):
    return re.sub('<[^<]+?>', '', description.replace('</p>', '\n\n').replace('</P>', '\n\n').replace('<br>', '\n').replace('<BR>', '\n'))

def nice_json(some_dict):
    return orjson.dumps(some_dict, option=orjson.OPT_INDENT_2 | orjson.OPT_NON_STR_KEYS, default=str).decode('utf-8')

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

@babel.localeselector
def localeselector():
    potential_locale = request.headers['Host'].split('.')[0]
    if potential_locale in [locale.language for locale in babel.list_translations()]:
        return potential_locale
    return 'en'

@functools.cache
def last_data_refresh_date():
    with engine.connect() as conn:
        libgenrs_time = conn.execute(select(LibgenrsUpdated.TimeLastModified).order_by(LibgenrsUpdated.ID.desc()).limit(1)).scalars().first()
        libgenli_time = conn.execute(select(LibgenliFiles.time_last_modified).order_by(LibgenliFiles.f_id.desc()).limit(1)).scalars().first()
        latest_time = max([libgenrs_time, libgenli_time])
        return latest_time.date()

translations_with_english_fallback = set()
@page.before_request
def before_req():
    # Add English as a fallback language to all translations.
    translations = get_translations()
    if translations not in translations_with_english_fallback:
        with force_locale('en'):
            translations.add_fallback(get_translations())
        translations_with_english_fallback.add(translations)

    g.current_lang_code = get_locale().language

    g.languages = [(locale.language, locale.get_display_name()) for locale in babel.list_translations()]
    g.languages.sort()

    g.last_data_refresh_date = last_data_refresh_date()


@page.get("/")
def home_page():
    popular_md5s = [
        "8336332bf5877e3adbfb60ac70720cd5", # Against intellectual monopoly
        "f0a0beca050610397b9a1c2604c1a472", # Harry Potter
        "61a1797d76fc9a511fb4326f265c957b", # Cryptonomicon
        "4b3cd128c0cc11c1223911336f948523", # Subtle art of not giving a f*ck
        "6d6a96f761636b11f7e397b451c62506", # Game of thrones
        "0d9b713d0dcda4c9832fcb056f3e4102", # Aaron Swartz
        "45126b536bbdd32c0484bd3899e10d39", # Three-body problem
        "6963187473f4f037a28e2fe1153ca793", # How music got free
        "6db7e0c1efc227bc4a11fac3caff619b", # It ends with us
        "7849ad74f44619db11c17b85f1a7f5c8", # Lord of the rings
        "6ed2d768ec1668c73e4fa742e3df78d6", # Physics
    ]
    with Session(engine) as session:
        md5_dicts = get_md5_dicts_elasticsearch(session, popular_md5s)
        md5_dicts.sort(key=lambda md5_dict: popular_md5s.index(md5_dict['md5']))

        return render_template(
            "page/home.html",
            header_active="home",
            md5_dicts=md5_dicts,
        )


@page.get("/about")
def about_page():
    return render_template("page/about.html", header_active="about")


@page.get("/donate")
def donate_page():
    return render_template("page/donate.html", header_active="donate")


@page.get("/datasets")
def datasets_page():
    with engine.connect() as conn:
        libgenrs_time = conn.execute(select(LibgenrsUpdated.TimeLastModified).order_by(LibgenrsUpdated.ID.desc()).limit(1)).scalars().first()
        libgenrs_date = str(libgenrs_time.date())
        libgenli_time = conn.execute(select(LibgenliFiles.time_last_modified).order_by(LibgenliFiles.f_id.desc()).limit(1)).scalars().first()
        libgenli_date = str(libgenli_time.date())
        # OpenLibrary author keys seem randomly distributed, so some random prefix is good enough.
        openlib_time = conn.execute(select(OlBase.last_modified).where(OlBase.ol_key.like("/authors/OL11%")).order_by(OlBase.last_modified.desc()).limit(1)).scalars().first()
        openlib_date = str(openlib_time.date())

    return render_template(
        "page/datasets.html",
        header_active="datasets",
        libgenrs_date=libgenrs_date,
        libgenli_date=libgenli_date,
        openlib_date=openlib_date,
    )

@page.get("/datasets/libgen_aux")
def datasets_libgen_aux_page():
    return render_template("page/datasets_libgen_aux.html", header_active="datasets")

@page.get("/datasets/zlib_scrape")
def datasets_zlib_scrape_page():
    return render_template("page/datasets_zlib_scrape.html", header_active="datasets")

@page.get("/datasets/isbndb_scrape")
def datasets_isbndb_scrape_page():
    return render_template("page/datasets_isbndb_scrape.html", header_active="datasets")

@page.get("/datasets/libgen_rs")
def datasets_libgen_rs_page():
    with engine.connect() as conn:
        libgenrs_time = conn.execute(select(LibgenrsUpdated.TimeLastModified).order_by(LibgenrsUpdated.ID.desc()).limit(1)).scalars().first()
        libgenrs_date = str(libgenrs_time.date())
    return render_template("page/datasets_libgen_rs.html", header_active="datasets", libgenrs_date=libgenrs_date)

@page.get("/datasets/libgen_li")
def datasets_libgen_li_page():
    with engine.connect() as conn:
        libgenli_time = conn.execute(select(LibgenliFiles.time_last_modified).order_by(LibgenliFiles.f_id.desc()).limit(1)).scalars().first()
        libgenli_date = str(libgenli_time.date())
    return render_template("page/datasets_libgen_li.html", header_active="datasets", libgenli_date=libgenli_date)

@page.get("/datasets/openlib")
def datasets_openlib_page():
    with engine.connect() as conn:
        # OpenLibrary author keys seem randomly distributed, so some random prefix is good enough.
        openlib_time = conn.execute(select(OlBase.last_modified).where(OlBase.ol_key.like("/authors/OL11%")).order_by(OlBase.last_modified.desc()).limit(1)).scalars().first()
        openlib_date = str(openlib_time.date())
    return render_template("page/datasets_openlib.html", header_active="datasets", openlib_date=openlib_date)

@page.get("/datasets/isbn_ranges")
def datasets_isbn_ranges_page():
    return render_template("page/datasets_isbn_ranges.html", header_active="datasets")


def get_zlib_book_dicts(session, key, values):
    # Filter out bad data
    if key.lower() in ['md5', 'md5_reported']:
        values = [val for val in values if val not in search_filtered_bad_md5s]

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
        zlib_book_dict['sanitized_isbns'] = [record.isbn for record in zlib_book.isbns]
        zlib_book_dict['isbns_rich'] = make_isbns_rich(zlib_book_dict['sanitized_isbns'])
        zlib_book_dict['stripped_description'] = strip_description(zlib_book_dict['description'])
        zlib_book_dict['language_codes'] = get_bcp47_lang_codes(zlib_book_dict['language'] or '')
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
        zlib_book_dict['ipfs_cid'] = ''
        if len(zlib_book.ipfs) > 0:
            zlib_book_dict['ipfs_cid'] = zlib_book.ipfs[0].ipfs_cid
        zlib_book_dict['normalized_filename'] = make_normalized_filename(f"{zlib_book_dict['title']} {zlib_book_dict['author']} {zlib_book_dict['edition_varia_normalized']}", zlib_book_dict['extension'], "zlib", zlib_book_dict['zlibrary_id'])
        zlib_book_dicts.append(zlib_book_dict)

    return zlib_book_dicts

@page.get("/zlib/<int:zlib_id>")
def zlib_book_page(zlib_id):
    with Session(engine) as session:
        zlib_book_dicts = get_zlib_book_dicts(session, "zlibrary_id", [zlib_id])

        if len(zlib_book_dicts) == 0:
            return render_template("page/zlib_book.html", header_active="search", zlib_id=zlib_id), 404

        zlib_book_dict = zlib_book_dicts[0]
        return render_template(
            "page/zlib_book.html",
            header_active="search",
            zlib_id=zlib_id,
            zlib_book_dict=zlib_book_dict,
            zlib_book_json=nice_json(zlib_book_dict),
        )

@page.get("/ol/<string:ol_book_id>")
def ol_book_page(ol_book_id):
    ol_book_id = ol_book_id[0:20]

    with engine.connect() as conn:
        ol_book = conn.execute(select(OlBase).where(OlBase.ol_key == f"/books/{ol_book_id}").limit(1)).first()

        if ol_book == None:
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
        elif ol_book_dict['work'] and 'authors' in ol_book_dict['work']['json'] and len(ol_book_dict['work']['json']['authors']) > 0:
            unredirected_ol_authors = conn.execute(select(OlBase).where(OlBase.ol_key.in_([author['author']['key'] for author in ol_book_dict['work']['json']['authors']])).limit(10)).all()
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

        ol_book_dict['sanitized_isbns'] = make_sanitized_isbns((ol_book_dict['json'].get('isbn_10') or []) + (ol_book_dict['json'].get('isbn_13') or []))
        ol_book_dict['isbns_rich'] = make_isbns_rich(ol_book_dict['sanitized_isbns'])

        ol_book_dict['classifications_normalized'] = []
        for item in (ol_book_dict['json'].get('lc_classifications') or []):
            ol_book_dict['classifications_normalized'].append(('lc_classifications', item))
        for item in (ol_book_dict['json'].get('dewey_decimal_class') or []):
            ol_book_dict['classifications_normalized'].append(('dewey_decimal_class', item))
        for item in (ol_book_dict['json'].get('dewey_number') or []):
            ol_book_dict['classifications_normalized'].append(('dewey_decimal_class', item))
        for classification_type, items in (ol_book_dict['json'].get('classifications') or {}).items():
            for item in items:
                ol_book_dict['classifications_normalized'].append((classification_type, item))

        if ol_book_dict['work']:
            ol_book_dict['work']['classifications_normalized'] = []
            for item in (ol_book_dict['work']['json'].get('lc_classifications') or []):
                ol_book_dict['work']['classifications_normalized'].append(('lc_classifications', item))
            for item in (ol_book_dict['work']['json'].get('dewey_decimal_class') or []):
                ol_book_dict['work']['classifications_normalized'].append(('dewey_decimal_class', item))
            for item in (ol_book_dict['work']['json'].get('dewey_number') or []):
                ol_book_dict['work']['classifications_normalized'].append(('dewey_decimal_class', item))
            for classification_type, items in (ol_book_dict['work']['json'].get('classifications') or {}).items():
                for item in items:
                    ol_book_dict['work']['classifications_normalized'].append((classification_type, item))

        ol_book_dict['identifiers_normalized'] = []
        for item in (ol_book_dict['json'].get('lccn') or []):
            ol_book_dict['identifiers_normalized'].append(('lccn', item.strip()))
        for item in (ol_book_dict['json'].get('oclc_numbers') or []):
            ol_book_dict['identifiers_normalized'].append(('oclc_numbers', item.strip()))
        for identifier_type, items in (ol_book_dict['json'].get('identifiers') or {}).items():
            for item in items:
                ol_book_dict['identifiers_normalized'].append((identifier_type, item.strip()))

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


# See https://wiki.mhut.org/content:bibliographic_data for some more information.
def get_lgrsnf_book_dicts(session, key, values):
    # Filter out bad data
    if key.lower() == 'md5':
        values = [val for val in values if val not in search_filtered_bad_md5s]

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
        lgrs_book_dict['sanitized_isbns'] = make_sanitized_isbns(lgrsnf_book.Identifier.split(",") + lgrsnf_book.IdentifierWODash.split(","))
        lgrs_book_dict['isbns_rich'] = make_isbns_rich(lgrs_book_dict['sanitized_isbns'])
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

        lgrs_book_dict['normalized_filename'] = make_normalized_filename(f"{lgrs_book_dict['title']} {lgrs_book_dict['author']} {lgrs_book_dict['edition_varia_normalized']}", lgrs_book_dict['extension'], "libgenrs-nf", lgrs_book_dict['id'])

        lgrs_book_dicts.append(lgrs_book_dict)

    return lgrs_book_dicts


@page.get("/lgrs/nf/<int:lgrsnf_book_id>")
def lgrsnf_book_page(lgrsnf_book_id):
    with Session(engine) as session:
        lgrs_book_dicts = get_lgrsnf_book_dicts(session, "ID", [lgrsnf_book_id])

        if len(lgrs_book_dicts) == 0:
            return render_template("page/lgrs_book.html", header_active="search", lgrs_type='nf', lgrs_book_id=lgrsnf_book_id), 404

        return render_template(
            "page/lgrs_book.html",
            header_active="search",
            lgrs_type='nf',
            lgrs_book_id=lgrsnf_book_id,
            lgrs_book_dict=lgrs_book_dicts[0],
            lgrs_book_dict_json=nice_json(lgrs_book_dicts[0]),
        )


def get_lgrsfic_book_dicts(session, key, values):
    # Filter out bad data
    if key.lower() == 'md5':
        values = [val for val in values if val not in search_filtered_bad_md5s]

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
        lgrs_book_dict['sanitized_isbns'] = make_sanitized_isbns(lgrsfic_book.Identifier.split(","))
        lgrs_book_dict['isbns_rich'] = make_isbns_rich(lgrs_book_dict['sanitized_isbns'])
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

        lgrs_book_dict['normalized_filename'] = make_normalized_filename(f"{lgrs_book_dict['title']} {lgrs_book_dict['author']} {lgrs_book_dict['edition_varia_normalized']}", lgrs_book_dict['extension'], "libgenrs-fic", lgrs_book_dict['id'])

        lgrs_book_dicts.append(lgrs_book_dict)

    return lgrs_book_dicts


@page.get("/lgrs/fic/<int:lgrsfic_book_id>")
def lgrsfic_book_page(lgrsfic_book_id):
    with Session(engine) as session:
        lgrs_book_dicts = get_lgrsfic_book_dicts(session, "ID", [lgrsfic_book_id])

        if len(lgrs_book_dicts) == 0:
            return render_template("page/lgrs_book.html", header_active="search", lgrs_type='fic', lgrs_book_id=lgrsfic_book_id), 404

        return render_template(
            "page/lgrs_book.html",
            header_active="search",
            lgrs_type='fic',
            lgrs_book_id=lgrsfic_book_id,
            lgrs_book_dict=lgrs_book_dicts[0],
            lgrs_book_dict_json=nice_json(lgrs_book_dicts[0]),
        )

libgenli_elem_descr_output = None
def libgenli_elem_descr(conn):
    global libgenli_elem_descr_output
    if libgenli_elem_descr_output == None:
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
        normalized_base_field_first = normalized_base_field + '_first'
        normalized_base_field_multiple = normalized_base_field + '_multiple'
        if normalized_base_field not in descrs_mapped:
            descrs_mapped[normalized_base_field_first] = descr['value']
        if normalized_base_field_multiple in descrs_mapped:
            descrs_mapped[normalized_base_field_multiple].append(descr['value'])
        else:
            descrs_mapped[normalized_base_field_multiple] = [descr['value']]
        for i in [1,2,3]:
            add_field_name = f"name_add{i}_en"
            add_field_value = f"value_add{i}"
            if len(descr['meta'][add_field_name]) > 0:
                normalized_add_field = normalized_base_field + "_" + lgli_normalize_meta_field(descr['meta'][add_field_name])
                normalized_add_field_first = normalized_add_field + '_first'
                normalized_add_field_multiple = normalized_add_field + '_multiple'
                if normalized_add_field not in descrs_mapped:
                    descrs_mapped[normalized_add_field_first] = descr[add_field_value]
                if normalized_add_field_multiple in descrs_mapped:
                    descrs_mapped[normalized_add_field_multiple].append(descr[add_field_value])
                else:
                    descrs_mapped[normalized_add_field_multiple] = [descr[add_field_value]]
        if len(descr.get('publisher_title') or '') > 0:
            normalized_base_field = 'publisher_title'
            normalized_base_field_first = normalized_base_field + '_first'
            normalized_base_field_multiple = normalized_base_field + '_multiple'
            if normalized_base_field not in descrs_mapped:
                descrs_mapped[normalized_base_field_first] = descr['publisher_title']
            if normalized_base_field_multiple in descrs_mapped:
                descrs_mapped[normalized_base_field_multiple].append(descr['publisher_title'])
            else:
                descrs_mapped[normalized_base_field_multiple] = [descr['publisher_title']]

    return descrs_mapped

lgli_topic_mapping = {
    'l': 'Non-fiction ("libgen")',
    's': 'Standards document',
    'm': 'Magazine',
    'c': 'Comic',
    'f': 'Fiction',
    'r': 'Russian Fiction',
    'a': 'Journal article (Sci-Hub/scimag)'
}
# Hardcoded from the `descr_elems` table.
lgli_edition_type_mapping = {
    "b":"book",
    "ch":"book-chapter",
    "bpart":"book-part",
    "bsect":"book-section",
    "bs":"book-series",
    "bset":"book-set",
    "btrack":"book-track",
    "component":"component",
    "dataset":"dataset",
    "diss":"dissertation",
    "j":"journal",
    "a":"journal-article",
    "ji":"journal-issue",
    "jv":"journal-volume",
    "mon":"monograph",
    "oth":"other",
    "peer-review":"peer-review",
    "posted-content":"posted-content",
    "proc":"proceedings",
    "proca":"proceedings-article",
    "ref":"reference-book",
    "refent":"reference-entry",
    "rep":"report",
    "repser":"report-series",
    "s":"standard",
    "fnz":"Fanzine",
    "m":"Magazine issue",
    "col":"Collection",
    "chb":"Chapbook",
    "nonfict":"Nonfiction",
    "omni":"Omnibus",
    "nov":"Novel",
    "ant":"Anthology",
    "c":"Comics issue",
}
lgli_issue_other_fields = [
    "issue_number_in_year",
    "issue_year_number",
    "issue_number",
    "issue_volume",
    "issue_split",
    "issue_total_number",
    "issue_first_page",
    "issue_last_page",
    "issue_year_end",
    "issue_month_end",
    "issue_day_end",
    "issue_closed",
]
lgli_standard_info_fields = [
    "standardtype",
    "standardtype_standartnumber",
    "standardtype_standartdate",
    "standartnumber",
    "standartstatus",
    "standartstatus_additionalstandartstatus",
]
lgli_date_info_fields = [
    "datepublication",
    "dateintroduction",
    "dateactualizationtext",
    "dateregistration",
    "dateactualizationdescr",
    "dateexpiration",
    "datelastedition",
]
# Hardcoded from the `libgenli_elem_descr` table.
lgli_identifiers = {
    "doi": { "label": "DOI", "url": "https://doi.org/%s", "description": "Digital Object Identifier"},
    "issn_multiple": { "label": "ISSN", "url": "https://urn.issn.org/urn:issn:%s", "description": "International Standard Serial Number"},
    "pii_multiple": { "label": "PII", "url": "", "description": "Publisher Item Identifier", "website": "https://en.wikipedia.org/wiki/Publisher_Item_Identifier"},
    "pmcid_multiple": { "label": "PMC ID", "url": "https://www.ncbi.nlm.nih.gov/pmc/articles/%s/", "description": "PubMed Central ID"},
    "pmid_multiple": { "label": "PMID", "url": "https://pubmed.ncbi.nlm.nih.gov/%s/", "description": "PubMed ID"},
    "asin_multiple": { "label": "ASIN", "url": "https://www.amazon.com/dp/%s", "description": "Amazon Standard Identification Number"},
    "bl_multiple": { "label": "BL", "url": "http://explore.bl.uk/primo_library/libweb/action/dlDisplay.do?vid=BLVU1&amp;docId=BLL01%s", "description": "The British Library"},
    "bnb_multiple": { "label": "BNB", "url": "http://search.bl.uk/primo_library/libweb/action/search.do?fn=search&vl(freeText0)=%s", "description": "The British National Bibliography"},
    "bnf_multiple": { "label": "BNF", "url": "http://catalogue.bnf.fr/ark:/12148/%s", "description": "Bibliotheque nationale de France"},
    "copac_multiple": { "label": "COPAC", "url": "http://copac.jisc.ac.uk/id/%s?style=html", "description": "UK/Irish union catalog"},
    "dnb_multiple": { "label": "DNB", "url": "http://d-nb.info/%s", "description": "Deutsche Nationalbibliothek"},
    "fantlabeditionid_multiple": { "label": "FantLab Edition ID", "url": "https://fantlab.ru/edition%s", "description": "Лаболатория фантастики"},
    "goodreads_multiple": { "label": "Goodreads", "url": "http://www.goodreads.com/book/show/%s", "description": "Goodreads social cataloging site"},
    "jnbjpno_multiple": { "label": "JNB/JPNO", "url": "https://iss.ndl.go.jp/api/openurl?ndl_jpno=%s&amp;locale=en", "description": "The Japanese National Bibliography"},
    "lccn_multiple": { "label": "LCCN", "url": "http://lccn.loc.gov/%s", "description": "Library of Congress Control Number"},
    "ndl_multiple": { "label": "NDL", "url": "http://id.ndl.go.jp/bib/%s/eng", "description": "National Diet Library"},
    "oclcworldcat_multiple": { "label": "OCLC/WorldCat", "url": "https://www.worldcat.org/oclc/%s", "description": "Online Computer Library Center"},
    "openlibrary_multiple": { "label": "Open Library", "url": "https://openlibrary.org/books/%s", "description": ""},
    "sfbg_multiple": { "label": "SFBG", "url": "http://www.sfbg.us/book/%s", "description": "Catalog of books published in Bulgaria"},
    "bn_multiple": { "label": "BN", "url": "http://www.barnesandnoble.com/s/%s", "description": "Barnes and Noble"},
    "ppn_multiple": { "label": "PPN", "url": "http://picarta.pica.nl/xslt/DB=3.9/XMLPRS=Y/PPN?PPN=%s", "description": "De Nederlandse Bibliografie Pica Productie Nummer"},
    "audibleasin_multiple": { "label": "Audible-ASIN", "url": "https://www.audible.com/pd/%s", "description": "Audible ASIN"},
    "ltf_multiple": { "label": "LTF", "url": "http://www.tercerafundacion.net/biblioteca/ver/libro/%s", "description": "La Tercera Fundaci&#243;n"},
    "kbr_multiple": { "label": "KBR", "url": "https://opac.kbr.be/Library/doc/SYRACUSE/%s/", "description": "De Belgische Bibliografie/La Bibliographie de Belgique"},
    "reginald1_multiple": { "label": "Reginald-1", "url": "", "description": "R. Reginald. Science Fiction and Fantasy Literature: A Checklist, 1700-1974, with Contemporary Science Fiction Authors II. Gale Research Co., 1979, 1141p."},
    "reginald3_multiple": { "label": "Reginald-3", "url": "", "description": "Robert Reginald. Science Fiction and Fantasy Literature, 1975-1991: A Bibliography of Science Fiction, Fantasy, and Horror Fiction Books and Nonfiction Monographs. Gale Research Inc., 1992, 1512 p."},
    "bleilergernsback_multiple": { "label": "Bleiler Gernsback", "url": "", "description": "Everett F. Bleiler, Richard Bleiler. Science-Fiction: The Gernsback Years. Kent State University Press, 1998, xxxii+730pp"},
    "bleilersupernatural_multiple": { "label": "Bleiler Supernatural", "url": "", "description": "Everett F. Bleiler. The Guide to Supernatural Fiction. Kent State University Press, 1983, xii+723 p."},
    "bleilerearlyyears_multiple": { "label": "Bleiler Early Years", "url": "", "description": "Richard Bleiler, Everett F. Bleiler. Science-Fiction: The Early Years. Kent State University Press, 1991, xxiii+998 p."},
    "nilf_multiple": { "label": "NILF", "url": "http://nilf.it/%s/", "description": "Numero Identificativo della Letteratura Fantastica / Fantascienza"},
    "noosfere_multiple": { "label": "NooSFere", "url": "https://www.noosfere.org/livres/niourf.asp?numlivre=%s", "description": "NooSFere"},
    "sfleihbuch_multiple": { "label": "SF-Leihbuch", "url": "http://www.sf-leihbuch.de/index.cfm?bid=%s", "description": "Science Fiction-Leihbuch-Datenbank"},
    "nla_multiple": { "label": "NLA", "url": "https://nla.gov.au/nla.cat-vn%s", "description": "National Library of Australia"},
    "porbase_multiple": { "label": "PORBASE", "url": "http://id.bnportugal.gov.pt/bib/porbase/%s", "description": "Biblioteca Nacional de Portugal"},
    "isfdbpubideditions_multiple": { "label": "ISFDB (editions)", "url": "http://www.isfdb.org/cgi-bin/pl.cgi?%s", "description": ""},
    "googlebookid_multiple": { "label": "Google Books", "url": "https://books.google.com/books?id=%s", "description": ""},
    "jstorstableid_multiple": { "label": "JSTOR Stable", "url": "https://www.jstor.org/stable/%s", "description": ""},
    "crossrefbookid_multiple": { "label": "Crossref", "url": "https://data.crossref.org/depositorreport?pubid=%s", "description":""},
}
# Hardcoded from the `libgenli_elem_descr` table.
lgli_classifications = {
    "classification_multiple": { "label": "Classification", "url": "", "description": "" },
    "classificationokp_multiple": { "label": "OKP", "url": "https://classifikators.ru/okp/%s", "description": "" },
    "classificationgostgroup_multiple": { "label": "GOST group", "url": "", "description": "", "website": "https://en.wikipedia.org/wiki/GOST" },
    "classificationoks_multiple": { "label": "OKS", "url": "", "description": "" },
    "libraryofcongressclassification_multiple": { "label": "LCC", "url": "", "description": "Library of Congress Classification", "website": "https://en.wikipedia.org/wiki/Library_of_Congress_Classification" },
    "udc_multiple": { "label": "UDC", "url": "https://libgen.li/biblioservice.php?value=%s&type=udc", "description": "Universal Decimal Classification", "website": "https://en.wikipedia.org/wiki/Universal_Decimal_Classification" },
    "ddc_multiple": { "label": "DDC", "url": "https://libgen.li/biblioservice.php?value=%s&type=ddc", "description": "Dewey Decimal", "website": "https://en.wikipedia.org/wiki/List_of_Dewey_Decimal_classes" },
    "lbc_multiple": { "label": "LBC", "url": "https://libgen.li/biblioservice.php?value=%s&type=bbc", "description": "Library-Bibliographical Classification", "website": "https://www.isko.org/cyclo/lbc" },
}

# See https://libgen.li/community/app.php/article/new-database-structure-published-o%CF%80y6%D0%BB%D0%B8%C4%B8o%D0%B2a%D0%BDa-%D0%BDo%D0%B2a%D1%8F-c%D1%82py%C4%B8%D1%82ypa-6a%D0%B7%C6%85i-%D0%B4a%D0%BD%D0%BD%C6%85ix
def get_lgli_file_dicts(session, key, values):
    # Filter out bad data
    if key.lower() == 'md5':
        values = [val for val in values if val not in search_filtered_bad_md5s]

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
            if len(edition_dict['authors_normalized']) == 0 and len(edition_dict['descriptions_mapped'].get('author_multiple') or []) > 0:
                edition_dict['authors_normalized'] = ", ".join(author.strip() for author in edition_dict['descriptions_mapped']['author_multiple'])

            edition_dict['cover_url_guess'] = edition_dict['cover_url']
            if len(edition_dict['descriptions_mapped'].get('coverurl_first') or '') > 0:
                edition_dict['cover_url_guess'] = edition_dict['descriptions_mapped']['coverurl_first']
            if edition_dict['cover_exists'] > 0:
                edition_dict['cover_url_guess'] = f"https://libgen.li/editioncovers/{(edition_dict['e_id'] // 1000) * 1000}/{edition_dict['e_id']}.jpg"

            issue_other_fields = dict((key, edition_dict[key]) for key in lgli_issue_other_fields if edition_dict[key] not in ['', '0', 0, None])
            if len(issue_other_fields) > 0:
                edition_dict['issue_other_fields_json'] = nice_json(issue_other_fields)
            standard_info_fields = dict((key, edition_dict['descriptions_mapped'][key + '_multiple']) for key in lgli_standard_info_fields if edition_dict['descriptions_mapped'].get(key + '_multiple') not in ['', '0', 0, None])
            if len(standard_info_fields) > 0:
                edition_dict['standard_info_fields_json'] = nice_json(standard_info_fields)
            date_info_fields = dict((key, edition_dict['descriptions_mapped'][key + '_multiple']) for key in lgli_date_info_fields if edition_dict['descriptions_mapped'].get(key + '_multiple') not in ['', '0', 0, None])
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

            edition_dict['publisher_normalized'] = ''
            if len((edition_dict['publisher'] or '').strip()) > 0:
                edition_dict['publisher_normalized'] = edition_dict['publisher'].strip()
            elif len((edition_dict['descriptions_mapped'].get('publisher_title_first') or '').strip()) > 0:
                edition_dict['publisher_normalized'] = edition_dict['descriptions_mapped']['publisher_title_first'].strip()
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

            language_multiple_codes = [get_bcp47_lang_codes(language_code) for language_code in (edition_dict['descriptions_mapped'].get('language_multiple') or [])]
            edition_dict['language_codes'] = combine_bcp47_lang_codes(language_multiple_codes)
            languageoriginal_multiple_codes = [get_bcp47_lang_codes(language_code) for language_code in (edition_dict['descriptions_mapped'].get('languageoriginal_multiple') or [])]
            edition_dict['languageoriginal_codes'] = combine_bcp47_lang_codes(languageoriginal_multiple_codes)

            edition_dict['identifiers_normalized'] = []
            if len(edition_dict['doi'].strip()) > 0:
                edition_dict['identifiers_normalized'].append(('doi', edition_dict['doi'].strip()))
            for key, values in edition_dict['descriptions_mapped'].items():
                if key in lgli_identifiers:
                    for value in values:
                        edition_dict['identifiers_normalized'].append((key, value.strip()))

            edition_dict['classifications_normalized'] = []
            for key, values in edition_dict['descriptions_mapped'].items():
                if key in lgli_classifications:
                    for value in values:
                        edition_dict['classifications_normalized'].append((key, value.strip()))

            edition_dict['sanitized_isbns'] = make_sanitized_isbns(edition_dict['descriptions_mapped'].get('isbn_multiple') or [])
            edition_dict['isbns_rich'] = make_isbns_rich(edition_dict['sanitized_isbns'])

            edition_dict['stripped_description'] = ''
            if len(edition_dict['descriptions_mapped'].get('description_multiple') or []) > 0:
                edition_dict['stripped_description'] = strip_description("\n\n".join(edition_dict['descriptions_mapped']['description_multiple']))

            lgli_file_dict['editions'].append(edition_dict)

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

        lgli_file_dicts.append(lgli_file_dict)

    return lgli_file_dicts


@page.get("/lgli/file/<int:lgli_file_id>")
def lgli_file_page(lgli_file_id):
    with Session(engine) as session:
        lgli_file_dicts = get_lgli_file_dicts(session, "f_id", [lgli_file_id])

        if len(lgli_file_dicts) == 0:
            return render_template("page/lgli_file.html", header_active="search", lgli_file_id=lgli_file_id), 404

        lgli_file_dict = lgli_file_dicts[0]

        lgli_file_top = { 'title': '', 'author': '', 'description': '' }
        if len(lgli_file_dict['editions']) > 0:
            for edition_dict in lgli_file_dict['editions']:
                if len(edition_dict['title'].strip()) > 0:
                    lgli_file_top['title'] = edition_dict['title'].strip()
                    break
            if len(lgli_file_top['title'].strip()) == 0:
                lgli_file_top['title'] = lgli_file_dict['locator'].split('\\')[-1].strip()
            else:
                lgli_file_top['description'] = lgli_file_dict['locator'].split('\\')[-1].strip()
            for edition_dict in lgli_file_dict['editions']:
                if len(edition_dict['authors_normalized']) > 0:
                    lgli_file_top['author'] = edition_dict['authors_normalized']
                    break
            for edition_dict in lgli_file_dict['editions']:
                if len(edition_dict['descriptions_mapped'].get('description_multiple') or []) > 0:
                    lgli_file_top['description'] = strip_description("\n\n".join(edition_dict['descriptions_mapped']['description_multiple']))
            for edition_dict in lgli_file_dict['editions']:
                if len(edition_dict['edition_varia_normalized']) > 0:
                    lgli_file_top['description'] = strip_description(edition_dict['edition_varia_normalized']) + ('\n\n' if len(lgli_file_top['description']) > 0 else '') + lgli_file_top['description']
                    break
        if len(lgli_file_dict['scimag_archive_path']) > 0:
            lgli_file_top['title'] = lgli_file_dict['scimag_archive_path']

        return render_template(
            "page/lgli_file.html",
            header_active="search",
            lgli_file_id=lgli_file_id,
            lgli_file_dict=lgli_file_dict,
            lgli_file_top=lgli_file_top,
            lgli_file_dict_json=nice_json(lgli_file_dict),
            lgli_topic_mapping=lgli_topic_mapping,
            lgli_edition_type_mapping=lgli_edition_type_mapping,
            lgli_identifiers=lgli_identifiers,
            lgli_classifications=lgli_classifications,
        )

@page.get("/isbn/<string:isbn_input>")
def isbn_page(isbn_input):
    isbn_input = isbn_input[0:20]

    canonical_isbn13 = isbnlib.get_canonical_isbn(isbn_input, output='isbn13')
    if len(canonical_isbn13) != 13 or len(isbnlib.info(canonical_isbn13)) == 0:
        # TODO, check if a different prefix would help, like in
        # https://github.com/inventaire/isbn3/blob/d792973ac0e13a48466d199b39326c96026b7fc3/lib/audit.js
        return render_template("page/isbn.html", header_active="search", isbn_input=isbn_input)

    if canonical_isbn13 != isbn_input:
        return redirect(f"/isbn/{canonical_isbn13}", code=301)

    barcode_svg = ''
    try:
        barcode_bytesio = io.BytesIO()
        barcode.ISBN13(canonical_isbn13, writer=barcode.writer.SVGWriter()).write(barcode_bytesio)
        barcode_bytesio.seek(0)
        barcode_svg = barcode_bytesio.read().decode('utf-8').replace('fill:white', 'fill:transparent').replace(canonical_isbn13, '')
    except Exception as err:
        print(f"Error generating barcode: {err}")

    isbn13_mask = isbnlib.mask(canonical_isbn13)
    isbn_dict = {
        "ean13": isbnlib.ean13(canonical_isbn13),
        "isbn10": isbnlib.to_isbn10(canonical_isbn13),
        "doi": isbnlib.doi(canonical_isbn13),
        "info": isbnlib.info(canonical_isbn13),
        "mask": isbn13_mask,
        "mask_split": isbn13_mask.split('-'),
        "barcode_svg": barcode_svg,
    }
    if isbn_dict['isbn10']:
        isbn_dict['mask10'] = isbnlib.mask(isbn_dict['isbn10'])

    with engine.connect() as conn:
        isbndb_books = {}
        if isbn_dict['isbn10']:
            isbndb10_all = conn.execute(select(IsbndbIsbns).where(IsbndbIsbns.isbn10 == isbn_dict['isbn10']).limit(100)).all()
            for isbndb10 in isbndb10_all:
                # ISBNdb has a bug where they just chop off the prefix of ISBN-13, which is incorrect if the prefix is anything
                # besides "978"; so we double-check on this.
                if isbndb10['isbn13'][0:3] == '978':
                    isbndb_books[isbndb10['isbn13'] + '-' + isbndb10['isbn10']] = { **isbndb10, 'source_isbn': isbn_dict['isbn10'], 'matchtype': 'ISBN-10' }
        isbndb13_all = conn.execute(select(IsbndbIsbns).where(IsbndbIsbns.isbn13 == canonical_isbn13).limit(100)).all()
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
            isbndb_dict['languages_and_codes'] = [(get_display_name_for_lang(lang_code, get_locale().language), lang_code) for lang_code in isbndb_dict['language_codes']]

        if len(isbn_dict['isbndb']) > 0:
            isbn_dict['top_box'] = {
                'cover_url': isbn_dict['isbndb'][0]['json'].get('image', None) or '',
                'top_row': isbn_dict['isbndb'][0]['languages_and_codes'][0][0] if len(isbn_dict['isbndb'][0]['languages_and_codes']) > 0 else '',
                'title': isbn_dict['isbndb'][0]['json'].get('title', None) or '',
                'publisher_and_edition': ", ".join([item for item in [
                        str(isbn_dict['isbndb'][0]['json'].get('publisher', None) or '').strip(),
                        str(isbn_dict['isbndb'][0]['json'].get('edition', None) or '').strip(),
                        str(isbn_dict['isbndb'][0]['json'].get('date_published', None) or '').strip(),
                    ] if item != '']),
                'author': ', '.join(isbn_dict['isbndb'][0]['json'].get('authors', None) or []),
                'description': '\n\n'.join([strip_description(isbndb_dict['json'].get('synopsis') or ''), strip_description(isbndb_dict['json'].get('overview') or '')]).strip(),
            }

        # TODO: sort the results again by best matching language. But we should maybe also look at other matches like title, author, etc, in case we have mislabeled ISBNs.
        # Get the language codes from the first match.
        # language_codes_probs = {}
        # if len(isbn_dict['isbndb']) > 0:
        #     for lang_code in isbn_dict['isbndb'][0]['language_codes']:
        #         language_codes_probs[lang_code] = 1.0

        search_results_raw = es.search(
            index="md5_dicts",
            size=100,
            query={ "term": { "file_unified_data.sanitized_isbns": canonical_isbn13 } },
            sort={ "search_only_fields.score_base": "desc" },
            timeout=ES_TIMEOUT,
        )
        search_md5_dicts = [add_additional_to_md5_dict({'md5': md5_dict['_id'], **md5_dict['_source']}) for md5_dict in search_results_raw['hits']['hits'] if md5_dict['_id'] not in search_filtered_bad_md5s]
        isbn_dict['search_md5_dicts'] = search_md5_dicts
        
        return render_template(
            "page/isbn.html",
            header_active="search",
            isbn_input=isbn_input,
            isbn_dict=isbn_dict,
            isbn_dict_json=nice_json(isbn_dict),
        )

@page.get("/doi/<path:doi_input>")
def doi_page(doi_input):
    doi_input = doi_input[0:100]

    if not looks_like_doi(doi_input):
        return render_template("page/doi.html", header_active="search", doi_input=doi_input), 404

    search_results_raw = es.search(
        index="md5_dicts",
        size=100,
        query={ "term": { "file_unified_data.doi_multiple": doi_input } },
        sort={ "search_only_fields.score_base": "desc" },
        timeout=ES_TIMEOUT,
    )
    search_md5_dicts = [add_additional_to_md5_dict({'md5': md5_dict['_id'], **md5_dict['_source']}) for md5_dict in search_results_raw['hits']['hits'] if md5_dict['_id'] not in search_filtered_bad_md5s]

    doi_dict = {}
    doi_dict['search_md5_dicts'] = search_md5_dicts
    
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

def get_md5_dicts_elasticsearch(session, canonical_md5s):
    if not allthethings.utils.validate_canonical_md5s(canonical_md5s):
        raise Exception("Non-canonical md5")

    # Filter out bad data
    canonical_md5s = [val for val in canonical_md5s if val not in search_filtered_bad_md5s]

    # Uncomment the following line to use MySQL directly; useful for local development.
    # return get_md5_dicts_mysql(session, canonical_md5s)

    search_results_raw = es.mget(index="md5_dicts", ids=canonical_md5s)
    return [{'md5': result['_id'], **result['_source']} for result in search_results_raw['docs'] if result['found']]

def md5_dict_score_base(md5_dict):
    if len(md5_dict['file_unified_data'].get('problems') or []) > 0:
        return 0.0

    score = 10000.0
    if (md5_dict['file_unified_data'].get('filesize_best') or 0) > 500000:
        score += 1000.0
    # If we're not confident about the language, demote.
    if len(md5_dict['file_unified_data'].get('language_codes') or []) == 0:
        score -= 2.0
    if (md5_dict['file_unified_data'].get('extension_best') or '') in ['epub', 'pdf']:
        score += 10.0
    if len(md5_dict['file_unified_data'].get('cover_url_best') or '') > 0:
        # Since we only use the zlib cover as a last resort, and zlib is down / only on Tor,
        # stronlgy demote zlib-only books for now.
        if 'covers.zlibcdn2.com' in (md5_dict['file_unified_data'].get('cover_url_best') or ''):
            score -= 10.0
        else:
            score += 3.0
    if len(md5_dict['file_unified_data'].get('title_best') or '') > 0:
        score += 10.0
    if len(md5_dict['file_unified_data'].get('author_best') or '') > 0:
        score += 1.0
    if len(md5_dict['file_unified_data'].get('publisher_best') or '') > 0:
        score += 1.0
    if len(md5_dict['file_unified_data'].get('edition_varia_best') or '') > 0:
        score += 1.0
    if len(md5_dict['file_unified_data'].get('original_filename_best_name_only') or '') > 0:
        score += 1.0
    if len(md5_dict['file_unified_data'].get('sanitized_isbns') or []) > 0:
        score += 1.0
    if len(md5_dict['file_unified_data'].get('asin_multiple') or []) > 0:
        score += 1.0
    if len(md5_dict['file_unified_data'].get('googlebookid_multiple') or []) > 0:
        score += 1.0
    if len(md5_dict['file_unified_data'].get('openlibraryid_multiple') or []) > 0:
        score += 1.0
    if len(md5_dict['file_unified_data'].get('doi_multiple') or []) > 0:
        # For now demote DOI quite a bit, since tons of papers can drown out books.
        score -= 70.0
    if len(md5_dict['file_unified_data'].get('stripped_description_best') or '') > 0:
        score += 1.0
    return score

def get_md5_dicts_mysql(session, canonical_md5s):
    if not allthethings.utils.validate_canonical_md5s(canonical_md5s):
        raise Exception("Non-canonical md5")

    # Filter out bad data
    canonical_md5s = [val for val in canonical_md5s if val not in search_filtered_bad_md5s]

    # canonical_and_upper_md5s = canonical_md5s + [md5.upper() for md5 in canonical_md5s]
    lgrsnf_book_dicts = dict((item['md5'].lower(), item) for item in get_lgrsnf_book_dicts(session, "MD5", canonical_md5s))
    lgrsfic_book_dicts = dict((item['md5'].lower(), item) for item in get_lgrsfic_book_dicts(session, "MD5", canonical_md5s))
    lgli_file_dicts = dict((item['md5'].lower(), item) for item in get_lgli_file_dicts(session, "md5", canonical_md5s))
    zlib_book_dicts1 = dict((item['md5_reported'].lower(), item) for item in get_zlib_book_dicts(session, "md5_reported", canonical_md5s))
    zlib_book_dicts2 = dict((item['md5'].lower(), item) for item in get_zlib_book_dicts(session, "md5", canonical_md5s))

    md5_dicts = []
    for canonical_md5 in canonical_md5s:
        md5_dict = {}
        md5_dict['md5'] = canonical_md5
        md5_dict['lgrsnf_book'] = lgrsnf_book_dicts.get(canonical_md5)
        md5_dict['lgrsfic_book'] = lgrsfic_book_dicts.get(canonical_md5)
        md5_dict['lgli_file'] = lgli_file_dicts.get(canonical_md5)
        if md5_dict.get('lgli_file'):
            md5_dict['lgli_file']['editions'] = md5_dict['lgli_file']['editions'][0:5]
        md5_dict['zlib_book'] = zlib_book_dicts1.get(canonical_md5) or zlib_book_dicts2.get(canonical_md5)

        md5_dict['ipfs_infos'] = []
        if md5_dict['lgrsnf_book'] and len(md5_dict['lgrsnf_book'].get('ipfs_cid') or '') > 0:
            md5_dict['ipfs_infos'].append({ 'ipfs_cid': md5_dict['lgrsnf_book']['ipfs_cid'].lower(), 'filename': md5_dict['lgrsnf_book']['normalized_filename'], 'from': 'lgrsnf' })
        if md5_dict['lgrsfic_book'] and len(md5_dict['lgrsfic_book'].get('ipfs_cid') or '') > 0:
            md5_dict['ipfs_infos'].append({ 'ipfs_cid': md5_dict['lgrsfic_book']['ipfs_cid'].lower(), 'filename': md5_dict['lgrsfic_book']['normalized_filename'], 'from': 'lgrsfic' })
        if md5_dict['zlib_book'] and len(md5_dict['zlib_book'].get('ipfs_cid') or '') > 0:
            md5_dict['ipfs_infos'].append({ 'ipfs_cid': md5_dict['zlib_book']['ipfs_cid'].lower(), 'filename': md5_dict['zlib_book']['normalized_filename'], 'from': 'zlib' })

        md5_dict['file_unified_data'] = {}

        original_filename_multiple = [
            ((md5_dict['lgrsnf_book'] or {}).get('locator') or '').strip(),
            ((md5_dict['lgrsfic_book'] or {}).get('locator') or '').strip(),
            ((md5_dict['lgli_file'] or {}).get('locator') or '').strip(),
            (((md5_dict['lgli_file'] or {}).get('descriptions_mapped') or {}).get('library_filename_first') or '').strip(),
            ((md5_dict['lgli_file'] or {}).get('scimag_archive_path') or '').strip(),
        ]
        original_filename_multiple_processed = sort_by_length_and_filter_subsequences_with_longest_string(original_filename_multiple)
        md5_dict['file_unified_data']['original_filename_best'] = min(original_filename_multiple_processed, key=len) if len(original_filename_multiple_processed) > 0 else ''
        md5_dict['file_unified_data']['original_filename_additional'] = [s for s in original_filename_multiple_processed if s != md5_dict['file_unified_data']['original_filename_best']]
        md5_dict['file_unified_data']['original_filename_best_name_only'] =  re.split(r'[\\/]', md5_dict['file_unified_data']['original_filename_best'])[-1]

        # Select the cover_url_normalized in order of what is likely to be the best one: zlib, lgrsnf, lgrsfic, lgli.
        zlib_cover = ((md5_dict['zlib_book'] or {}).get('cover_url') or '').strip()
        cover_url_multiple = [
            # Put the zlib_cover at the beginning if it starts with the right prefix.
            # zlib_cover.strip() if zlib_cover.startswith('https://covers.zlibcdn2.com') else '',
            ((md5_dict['lgrsnf_book'] or {}).get('cover_url_normalized') or '').strip(),
            ((md5_dict['lgrsfic_book'] or {}).get('cover_url_normalized') or '').strip(),
            ((md5_dict['lgli_file'] or {}).get('cover_url_guess_normalized') or '').strip(),
            # Otherwie put it at the end.
            # '' if zlib_cover.startswith('https://covers.zlibcdn2.com') else zlib_cover.strip(),
            # Temporarily always put it at the end because their servers are down.
            zlib_cover.strip()
        ]
        cover_url_multiple_processed = list(dict.fromkeys(filter(len, cover_url_multiple)))
        md5_dict['file_unified_data']['cover_url_best'] = (cover_url_multiple_processed + [''])[0]
        md5_dict['file_unified_data']['cover_url_additional'] = [s for s in cover_url_multiple_processed if s != md5_dict['file_unified_data']['cover_url_best']]

        extension_multiple = [
            ((md5_dict['zlib_book'] or {}).get('extension') or '').strip().lower(),
            ((md5_dict['lgrsnf_book'] or {}).get('extension') or '').strip().lower(),
            ((md5_dict['lgrsfic_book'] or {}).get('extension') or '').strip().lower(),
            ((md5_dict['lgli_file'] or {}).get('extension') or '').strip().lower(),
        ]
        if "epub" in extension_multiple:
            md5_dict['file_unified_data']['extension_best'] = "epub"
        elif "pdf" in extension_multiple:
            md5_dict['file_unified_data']['extension_best'] = "pdf"
        else:
            md5_dict['file_unified_data']['extension_best'] = max(extension_multiple, key=len)
        md5_dict['file_unified_data']['extension_additional'] = [s for s in dict.fromkeys(filter(len, extension_multiple)) if s != md5_dict['file_unified_data']['extension_best']]

        filesize_multiple = [
            (md5_dict['zlib_book'] or {}).get('filesize_reported') or 0,
            (md5_dict['zlib_book'] or {}).get('filesize') or 0,
            (md5_dict['lgrsnf_book'] or {}).get('filesize') or 0,
            (md5_dict['lgrsfic_book'] or {}).get('filesize') or 0,
            (md5_dict['lgli_file'] or {}).get('filesize') or 0,
        ]
        md5_dict['file_unified_data']['filesize_best'] = max(filesize_multiple)
        zlib_book_filesize = (md5_dict['zlib_book'] or {}).get('filesize') or 0
        if zlib_book_filesize > 0:
            # If we have a zlib_book with a `filesize`, then that is leading, since we measured it ourselves.
            md5_dict['file_unified_data']['filesize_best'] = zlib_book_filesize
        md5_dict['file_unified_data']['filesize_additional'] = [s for s in dict.fromkeys(filter(lambda fz: fz > 0, filesize_multiple)) if s != md5_dict['file_unified_data']['filesize_best']]

        lgli_single_edition = md5_dict['lgli_file']['editions'][0] if len((md5_dict.get('lgli_file') or {}).get('editions') or []) == 1 else None
        lgli_all_editions = md5_dict['lgli_file']['editions'] if md5_dict.get('lgli_file') else []

        title_multiple = [
            ((md5_dict['zlib_book'] or {}).get('title') or '').strip(),
            ((md5_dict['lgrsnf_book'] or {}).get('title') or '').strip(),
            ((md5_dict['lgrsfic_book'] or {}).get('title') or '').strip(),
            ((lgli_single_edition or {}).get('title') or '').strip(),
        ]
        md5_dict['file_unified_data']['title_best'] = max(title_multiple, key=len)
        title_multiple += [(edition.get('title') or '').strip() for edition in lgli_all_editions]
        title_multiple += [(edition['descriptions_mapped'].get('maintitleonoriginallanguage_first') or '').strip() for edition in lgli_all_editions]
        title_multiple += [(edition['descriptions_mapped'].get('maintitleonenglishtranslate_first') or '').strip() for edition in lgli_all_editions]
        if md5_dict['file_unified_data']['title_best'] == '':
            md5_dict['file_unified_data']['title_best'] = max(title_multiple, key=len)
        md5_dict['file_unified_data']['title_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(title_multiple) if s != md5_dict['file_unified_data']['title_best']]

        author_multiple = [
            (md5_dict['zlib_book'] or {}).get('author', '').strip(),
            (md5_dict['lgrsnf_book'] or {}).get('author', '').strip(),
            (md5_dict['lgrsfic_book'] or {}).get('author', '').strip(),
            (lgli_single_edition or {}).get('authors_normalized', '').strip(),
        ]
        md5_dict['file_unified_data']['author_best'] = max(author_multiple, key=len)
        author_multiple += [edition.get('authors_normalized', '').strip() for edition in lgli_all_editions]
        if md5_dict['file_unified_data']['author_best'] == '':
            md5_dict['file_unified_data']['author_best'] = max(author_multiple, key=len)
        md5_dict['file_unified_data']['author_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(author_multiple) if s != md5_dict['file_unified_data']['author_best']]

        publisher_multiple = [
            ((md5_dict['zlib_book'] or {}).get('publisher') or '').strip(),
            ((md5_dict['lgrsnf_book'] or {}).get('publisher') or '').strip(),
            ((md5_dict['lgrsfic_book'] or {}).get('publisher') or '').strip(),
            ((lgli_single_edition or {}).get('publisher_normalized') or '').strip(),
        ]
        md5_dict['file_unified_data']['publisher_best'] = max(publisher_multiple, key=len)
        publisher_multiple += [(edition.get('publisher_normalized') or '').strip() for edition in lgli_all_editions]
        if md5_dict['file_unified_data']['publisher_best'] == '':
            md5_dict['file_unified_data']['publisher_best'] = max(publisher_multiple, key=len)
        md5_dict['file_unified_data']['publisher_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(publisher_multiple) if s != md5_dict['file_unified_data']['publisher_best']]

        edition_varia_multiple = [
            ((md5_dict['zlib_book'] or {}).get('edition_varia_normalized') or '').strip(),
            ((md5_dict['lgrsnf_book'] or {}).get('edition_varia_normalized') or '').strip(),
            ((md5_dict['lgrsfic_book'] or {}).get('edition_varia_normalized') or '').strip(),
            ((lgli_single_edition or {}).get('edition_varia_normalized') or '').strip(),
        ]
        md5_dict['file_unified_data']['edition_varia_best'] = max(edition_varia_multiple, key=len)
        edition_varia_multiple += [(edition.get('edition_varia_normalized') or '').strip() for edition in lgli_all_editions]
        if md5_dict['file_unified_data']['edition_varia_best'] == '':
            md5_dict['file_unified_data']['edition_varia_best'] = max(edition_varia_multiple, key=len)
        md5_dict['file_unified_data']['edition_varia_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(edition_varia_multiple) if s != md5_dict['file_unified_data']['edition_varia_best']]

        year_multiple_raw = [
            ((md5_dict['zlib_book'] or {}).get('year') or '').strip(),
            ((md5_dict['lgrsnf_book'] or {}).get('year') or '').strip(),
            ((md5_dict['lgrsfic_book'] or {}).get('year') or '').strip(),
            ((lgli_single_edition or {}).get('year') or '').strip(),
            ((lgli_single_edition or {}).get('issue_year_number') or '').strip(),
        ]
        # Filter out years in for which we surely don't have books (famous last words..)
        year_multiple = [(year if year.isdigit() and int(year) >= 1600 and int(year) < 2100 else '') for year in year_multiple_raw]
        md5_dict['file_unified_data']['year_best'] = max(year_multiple, key=len)
        year_multiple += [(edition.get('year_normalized') or '').strip() for edition in lgli_all_editions]
        for year in year_multiple:
            # If a year appears in edition_varia_best, then use that, for consistency.
            if year != '' and year in md5_dict['file_unified_data']['edition_varia_best']:
                md5_dict['file_unified_data']['year_best'] = year
        if md5_dict['file_unified_data']['year_best'] == '':
            md5_dict['file_unified_data']['year_best'] = max(year_multiple, key=len)
        md5_dict['file_unified_data']['year_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(year_multiple) if s != md5_dict['file_unified_data']['year_best']]

        comments_multiple = [
            ((md5_dict['lgrsnf_book'] or {}).get('commentary') or '').strip(),
            ((md5_dict['lgrsfic_book'] or {}).get('commentary') or '').strip(),
            ' -- '.join(filter(len, [((md5_dict['lgrsnf_book'] or {}).get('library') or '').strip(), (md5_dict['lgrsnf_book'] or {}).get('issue', '').strip()])),
            ' -- '.join(filter(len, [((md5_dict['lgrsfic_book'] or {}).get('library') or '').strip(), (md5_dict['lgrsfic_book'] or {}).get('issue', '').strip()])),
            ' -- '.join(filter(len, [((md5_dict['lgli_file'] or {}).get('descriptions_mapped') or {}).get('descriptions_mapped.library_first', '').strip(), (md5_dict['lgli_file'] or {}).get('descriptions_mapped', {}).get('descriptions_mapped.library_issue_first', '').strip()])),
            ((lgli_single_edition or {}).get('commentary') or '').strip(),
            ((lgli_single_edition or {}).get('editions_add_info') or '').strip(),
            ((lgli_single_edition or {}).get('commentary') or '').strip(),
            *[note.strip() for note in (((lgli_single_edition or {}).get('descriptions_mapped') or {}).get('descriptions_mapped.notes_multiple') or [])],
        ]
        md5_dict['file_unified_data']['comments_best'] = max(comments_multiple, key=len)
        comments_multiple += [(edition.get('comments_normalized') or '').strip() for edition in lgli_all_editions]
        for edition in lgli_all_editions:
            comments_multiple.append((edition.get('editions_add_info') or '').strip())
            comments_multiple.append((edition.get('commentary') or '').strip())
            for note in (edition.get('descriptions_mapped') or {}).get('descriptions_mapped.notes_multiple', []):
                comments_multiple.append(note.strip())
        if md5_dict['file_unified_data']['comments_best'] == '':
            md5_dict['file_unified_data']['comments_best'] = max(comments_multiple, key=len)
        md5_dict['file_unified_data']['comments_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(comments_multiple) if s != md5_dict['file_unified_data']['comments_best']]

        stripped_description_multiple = [
            ((md5_dict['zlib_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((md5_dict['lgrsnf_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((md5_dict['lgrsfic_book'] or {}).get('stripped_description') or '').strip()[0:5000],
            ((lgli_single_edition or {}).get('stripped_description') or '').strip()[0:5000],
        ]
        md5_dict['file_unified_data']['stripped_description_best'] = max(stripped_description_multiple, key=len)
        stripped_description_multiple += [(edition.get('stripped_description') or '').strip()[0:5000] for edition in lgli_all_editions]
        if md5_dict['file_unified_data']['stripped_description_best'] == '':
            md5_dict['file_unified_data']['stripped_description_best'] = max(stripped_description_multiple, key=len)
        md5_dict['file_unified_data']['stripped_description_additional'] = [s for s in sort_by_length_and_filter_subsequences_with_longest_string(stripped_description_multiple) if s != md5_dict['file_unified_data']['stripped_description_best']]

        md5_dict['file_unified_data']['language_codes'] = combine_bcp47_lang_codes([
            ((md5_dict['zlib_book'] or {}).get('language_codes') or []),
            ((md5_dict['lgrsnf_book'] or {}).get('language_codes') or []),
            ((md5_dict['lgrsfic_book'] or {}).get('language_codes') or []),
            ((lgli_single_edition or {}).get('language_codes') or []),
        ])
        if len(md5_dict['file_unified_data']['language_codes']) == 0:
            md5_dict['file_unified_data']['language_codes'] = combine_bcp47_lang_codes([(edition.get('language_codes') or []) for edition in lgli_all_editions])

        language_detection = ''
        if len(md5_dict['file_unified_data']['stripped_description_best']) > 20:
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
        # md5_dict['file_unified_data']['detected_language_codes_probs'] = ", ".join(detected_language_codes_probs)

        md5_dict['file_unified_data']['most_likely_language_code'] = ''
        if len(md5_dict['file_unified_data']['language_codes']) > 0:
            md5_dict['file_unified_data']['most_likely_language_code'] = md5_dict['file_unified_data']['language_codes'][0]
        elif len(language_detection) > 0:
            md5_dict['file_unified_data']['most_likely_language_code'] = get_bcp47_lang_codes(language_detection)[0]

 

        md5_dict['file_unified_data']['sanitized_isbns'] = list(set([
            *((md5_dict['zlib_book'] or {}).get('sanitized_isbns') or []),
            *((md5_dict['lgrsnf_book'] or {}).get('sanitized_isbns') or []),
            *((md5_dict['lgrsfic_book'] or {}).get('sanitized_isbns') or []),
            *([isbn for edition in lgli_all_editions for isbn in (edition.get('sanitized_isbns') or [])]),
        ]))
        md5_dict['file_unified_data']['asin_multiple'] = list(set(item for item in [
            (md5_dict['lgrsnf_book'] or {}).get('asin', '').strip(),
            (md5_dict['lgrsfic_book'] or {}).get('asin', '').strip(),
            *[item[1] for edition in lgli_all_editions for item in edition['identifiers_normalized'] if item[0] == 'asin_multiple'],
        ] if item != ''))
        md5_dict['file_unified_data']['googlebookid_multiple'] = list(set(item for item in [
            (md5_dict['lgrsnf_book'] or {}).get('googlebookid', '').strip(),
            (md5_dict['lgrsfic_book'] or {}).get('googlebookid', '').strip(),
            *[item[1] for edition in lgli_all_editions for item in edition['identifiers_normalized'] if item[0] == 'googlebookid_multiple'],
        ] if item != ''))
        md5_dict['file_unified_data']['openlibraryid_multiple'] = list(set(item for item in [
            (md5_dict['lgrsnf_book'] or {}).get('openlibraryid', '').strip(),
            *[item[1] for edition in lgli_all_editions for item in edition['identifiers_normalized'] if item[0] == 'openlibrary_multiple'],
        ] if item != ''))
        md5_dict['file_unified_data']['doi_multiple'] = list(set(item for item in [
            (md5_dict['lgrsnf_book'] or {}).get('doi', '').strip(),
            *[item[1] for edition in lgli_all_editions for item in edition['identifiers_normalized'] if item[0] == 'doi'],
        ] if item != ''))

        md5_dict['file_unified_data']['problems'] = []
        if ((md5_dict['lgrsnf_book'] or {}).get('visible') or '') != '':
            md5_dict['file_unified_data']['problems'].append({ 'type': 'lgrsnf_visible', 'descr': ((md5_dict['lgrsnf_book'] or {}).get('visible') or '') })
        if ((md5_dict['lgrsfic_book'] or {}).get('visible') or '') != '':
            md5_dict['file_unified_data']['problems'].append({ 'type': 'lgrsfic_visible', 'descr': ((md5_dict['lgrsfic_book'] or {}).get('visible') or '') })
        if ((md5_dict['lgli_file'] or {}).get('visible') or '') != '':
            md5_dict['file_unified_data']['problems'].append({ 'type': 'lgli_visible', 'descr': ((md5_dict['lgli_file'] or {}).get('visible') or '') })
        if ((md5_dict['lgli_file'] or {}).get('broken') or '') in [1, "1", "y", "Y"]:
            md5_dict['file_unified_data']['problems'].append({ 'type': 'lgli_broken', 'descr': ((md5_dict['lgli_file'] or {}).get('broken') or '') })
        if (md5_dict['zlib_book'] and (md5_dict['zlib_book']['in_libgen'] or False) == False and (md5_dict['zlib_book']['pilimi_torrent'] or '') == ''):
            md5_dict['file_unified_data']['problems'].append({ 'type': 'zlib_missing', 'descr': '' })

        md5_dict['file_unified_data']['content_type'] = 'book_unknown'
        if md5_dict['lgli_file'] != None:
            if md5_dict['lgli_file']['libgen_topic'] == 'l':
                md5_dict['file_unified_data']['content_type'] = 'book_nonfiction'
            if md5_dict['lgli_file']['libgen_topic'] == 'f':
                md5_dict['file_unified_data']['content_type'] = 'book_fiction'
            if md5_dict['lgli_file']['libgen_topic'] == 'r':
                md5_dict['file_unified_data']['content_type'] = 'book_fiction'
            if md5_dict['lgli_file']['libgen_topic'] == 'a':
                md5_dict['file_unified_data']['content_type'] = 'journal_article'
            if md5_dict['lgli_file']['libgen_topic'] == 's':
                md5_dict['file_unified_data']['content_type'] = 'standards_document'
            if md5_dict['lgli_file']['libgen_topic'] == 'm':
                md5_dict['file_unified_data']['content_type'] = 'magazine'
            if md5_dict['lgli_file']['libgen_topic'] == 'c':
                md5_dict['file_unified_data']['content_type'] = 'book_comic'
        if md5_dict['lgrsnf_book'] and (not md5_dict['lgrsfic_book']):
            md5_dict['file_unified_data']['content_type'] = 'book_nonfiction'
        if (not md5_dict['lgrsnf_book']) and md5_dict['lgrsfic_book']:
            md5_dict['file_unified_data']['content_type'] = 'book_fiction'


        if md5_dict['lgrsnf_book'] != None:
            md5_dict['lgrsnf_book'] = {
                'id': md5_dict['lgrsnf_book']['id'],
                'md5': md5_dict['lgrsnf_book']['md5'],
            }
        if md5_dict['lgrsfic_book'] != None:
            md5_dict['lgrsfic_book'] = {
                'id': md5_dict['lgrsfic_book']['id'],
                'md5': md5_dict['lgrsfic_book']['md5'],
            }
        if md5_dict['lgli_file'] != None:
            md5_dict['lgli_file'] = {
                'f_id': md5_dict['lgli_file']['f_id'],
                'md5': md5_dict['lgli_file']['md5'],
                'libgen_topic': md5_dict['lgli_file']['libgen_topic'],
            }
        if md5_dict['zlib_book'] != None:
            md5_dict['zlib_book'] = {
                'zlibrary_id': md5_dict['zlib_book']['zlibrary_id'],
                'md5': md5_dict['zlib_book']['md5'],
                'md5_reported': md5_dict['zlib_book']['md5_reported'],
                'filesize': md5_dict['zlib_book']['filesize'],
                'filesize_reported': md5_dict['zlib_book']['filesize_reported'],
                'in_libgen': md5_dict['zlib_book']['in_libgen'],
                'pilimi_torrent': md5_dict['zlib_book']['pilimi_torrent'],
            }


        md5_dict['search_only_fields'] = {}
        md5_dict['search_only_fields']['search_text'] = "\n".join([
            md5_dict['file_unified_data']['title_best'][:1000],
            md5_dict['file_unified_data']['author_best'][:1000],
            md5_dict['file_unified_data']['edition_varia_best'][:1000],
            md5_dict['file_unified_data']['publisher_best'][:1000],
            md5_dict['file_unified_data']['original_filename_best_name_only'][:1000],
            md5_dict['file_unified_data']['extension_best'],
        ]).replace('.', '. ').replace('_', ' ').replace('/', ' ').replace('\\', ' ')

        # At the very end
        md5_dict['search_only_fields']['score_base'] = float(md5_dict_score_base(md5_dict))

        md5_dicts.append(md5_dict)

    return md5_dicts

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
    if num < 1000000:
        return '<1MB'
    else:
        for unit in ["", "KB", "MB", "GB", "TB", "PB", "EB", "ZB"]:
            if abs(num) < 1000.0:
                return f"{num:3.1f}{unit}"
            num /= 1000.0
        return f"{num:.1f}YB"

def add_additional_to_md5_dict(md5_dict):
    additional = {}
    additional['most_likely_language_name'] = (get_display_name_for_lang(md5_dict['file_unified_data'].get('most_likely_language_code', None) or '', get_locale().language) if md5_dict['file_unified_data'].get('most_likely_language_code', None) else '')
    additional['top_box'] = {
        'meta_information': [item for item in [
                md5_dict['file_unified_data'].get('title_best', None) or '',
                md5_dict['file_unified_data'].get('author_best', None) or '',
                (md5_dict['file_unified_data'].get('stripped_description_best', None) or '')[0:100],
                md5_dict['file_unified_data'].get('publisher_best', None) or '',
                md5_dict['file_unified_data'].get('edition_varia_best', None) or '',
                md5_dict['file_unified_data'].get('original_filename_best_name_only', None) or '',
            ] if item != ''],
        'cover_url': md5_dict['file_unified_data'].get('cover_url_best', None) or '',
        'top_row': ", ".join([item for item in [
                additional['most_likely_language_name'],
                md5_dict['file_unified_data'].get('extension_best', None) or '',
                format_filesize(md5_dict['file_unified_data'].get('filesize_best', None) or 0),
                md5_dict['file_unified_data'].get('original_filename_best_name_only', None) or '',
            ] if item != '']),
        'title': md5_dict['file_unified_data'].get('title_best', None) or '',
        'publisher_and_edition': ", ".join([item for item in [
                md5_dict['file_unified_data'].get('publisher_best', None) or '',
                md5_dict['file_unified_data'].get('edition_varia_best', None) or '',
            ] if item != '']),
        'author': md5_dict['file_unified_data'].get('author_best', None) or '',
        'description': md5_dict['file_unified_data'].get('stripped_description_best', None) or '',
    }
    additional['isbns_rich'] = make_isbns_rich(md5_dict['file_unified_data']['sanitized_isbns'])
    additional['download_urls'] = []
    shown_click_get = False
    if md5_dict['lgrsnf_book'] != None:
        additional['download_urls'].append((gettext('page.md5.box.download.lgrsnf'), f"http://library.lol/main/{md5_dict['lgrsnf_book']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if md5_dict['lgrsfic_book'] != None:
        additional['download_urls'].append((gettext('page.md5.box.download.lgrsfic'), f"http://library.lol/fiction/{md5_dict['lgrsfic_book']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    if md5_dict['lgli_file'] != None:
        additional['download_urls'].append((gettext('page.md5.box.download.lgli'), f"http://libgen.li/ads.php?md5={md5_dict['lgli_file']['md5'].lower()}", gettext('page.md5.box.download.extra_also_click_get') if shown_click_get else gettext('page.md5.box.download.extra_click_get')))
        shown_click_get = True
    for doi in md5_dict['file_unified_data']['doi_multiple']:
        additional['download_urls'].append((gettext('page.md5.box.download.scihub', doi=doi), f"https://sci-hub.ru/{doi}", ""))
    if len(md5_dict['ipfs_infos']) > 0:
        additional['download_urls'].append((gettext('page.md5.box.download.ipfs_gateway', num=1), f"https://cloudflare-ipfs.com/ipfs/{md5_dict['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={md5_dict['ipfs_infos'][0]['filename']}", gettext('page.md5.box.download.ipfs_gateway_extra')))
        additional['download_urls'].append((gettext('page.md5.box.download.ipfs_gateway', num=2), f"https://ipfs.io/ipfs/{md5_dict['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={md5_dict['ipfs_infos'][0]['filename']}", ""))
        additional['download_urls'].append((gettext('page.md5.box.download.ipfs_gateway', num=3), f"https://gateway.pinata.cloud/ipfs/{md5_dict['ipfs_infos'][0]['ipfs_cid'].lower()}?filename={md5_dict['ipfs_infos'][0]['filename']}", ""))
    if md5_dict['zlib_book'] != None and len(md5_dict['zlib_book']['pilimi_torrent'] or '') > 0:
        additional['download_urls'].append((gettext('page.md5.box.download.zlib_anon', num=1), make_temp_anon_zlib_link("https://ktxr.rs", md5_dict['zlib_book']['zlibrary_id'], md5_dict['zlib_book']['pilimi_torrent'], md5_dict['file_unified_data']['extension_best']), ""))
        additional['download_urls'].append((gettext('page.md5.box.download.zlib_anon', num=2), make_temp_anon_zlib_link("https://nrzr.li", md5_dict['zlib_book']['zlibrary_id'], md5_dict['zlib_book']['pilimi_torrent'], md5_dict['file_unified_data']['extension_best']), ""))
        additional['download_urls'].append((gettext('page.md5.box.download.zlib_anon', num=3), make_temp_anon_zlib_link("http://193.218.118.109", md5_dict['zlib_book']['zlibrary_id'], md5_dict['zlib_book']['pilimi_torrent'], md5_dict['file_unified_data']['extension_best']), ""))
        additional['download_urls'].append((gettext('page.md5.box.download.zlib_anon', num=4), make_temp_anon_zlib_link("http://193.218.118.54", md5_dict['zlib_book']['zlibrary_id'], md5_dict['zlib_book']['pilimi_torrent'], md5_dict['file_unified_data']['extension_best']), ""))
    if md5_dict['zlib_book'] != None:
        additional['download_urls'].append((gettext('page.md5.box.download.zlib_tor'), f"http://zlibrary24tuxziyiyfr7zd46ytefdqbqd2axkmxm4o5374ptpc52fad.onion/md5/{md5_dict['zlib_book']['md5_reported'].lower()}", gettext('page.md5.box.download.zlib_tor_extra')))
    return { **md5_dict, 'additional': additional }


@page.get("/md5/<string:md5_input>")
def md5_page(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        return render_template("page/md5.html", header_active="search", md5_input=md5_input)

    if canonical_md5 != md5_input:
        return redirect(f"/md5/{canonical_md5}", code=301)

    with Session(engine) as session:
        md5_dicts = get_md5_dicts_elasticsearch(session, [canonical_md5])

        if len(md5_dicts) == 0:
            return render_template("page/md5.html", header_active="search", md5_input=md5_input)

        md5_dict = add_additional_to_md5_dict(md5_dicts[0])
        
        return render_template(
            "page/md5.html",
            header_active="search",
            md5_input=md5_input,
            md5_dict=md5_dict,
            md5_dict_json=nice_json(md5_dict),
            md5_content_type_mapping=get_md5_content_type_mapping(get_locale().language),
            md5_problem_type_mapping=get_md5_problem_type_mapping(),
        )


sort_search_md5_dicts_script = """
float score = params.boost + $('search_only_fields.score_base', 0);

score += _score / 100.0;

if (params.lang_code == $('file_unified_data.most_likely_language_code', '')) {
    score += 15.0;
}
if (params.lang_code == 'ca' && $('file_unified_data.most_likely_language_code', '') == 'es') {
    score += 10.0;
}
if (params.lang_code == 'bg' && $('file_unified_data.most_likely_language_code', '') == 'ru') {
    score += 10.0;
}
if ($('file_unified_data.most_likely_language_code', '') == 'en') {
    score += 5.0;
}

return score;
"""


search_query_aggs = {
    "most_likely_language_code": {
      "terms": { "field": "file_unified_data.most_likely_language_code", "size": 100 } 
    },
    "content_type": {
      "terms": { "field": "file_unified_data.content_type", "size": 200 } 
    },
    "extension_best": {
      "terms": { "field": "file_unified_data.extension_best", "size": 20 } 
    },
}

@functools.cache
def all_search_aggs(display_lang):
    search_results_raw = es.search(index="md5_dicts", size=0, aggs=search_query_aggs, timeout=ES_TIMEOUT)

    all_aggregations = {}
    # Unfortunately we have to special case the "unknown language", which is currently represented with an empty string `bucket['key'] != ''`, otherwise this gives too much trouble in the UI.
    all_aggregations['most_likely_language_code'] = []
    for bucket in search_results_raw['aggregations']['most_likely_language_code']['buckets']:
        if bucket['key'] == '':
            all_aggregations['most_likely_language_code'].append({ 'key': '_empty', 'label': get_display_name_for_lang('', display_lang), 'doc_count': bucket['doc_count'] })
        else:
            all_aggregations['most_likely_language_code'].append({ 'key': bucket['key'], 'label': get_display_name_for_lang(bucket['key'], display_lang), 'doc_count': bucket['doc_count'] })
    # We don't have browser_lang_codes for now..
    # total_doc_count = sum([record['doc_count'] for record in all_aggregations['most_likely_language_code']])
    # all_aggregations['most_likely_language_code'] = sorted(all_aggregations['most_likely_language_code'], key=lambda bucket: bucket['doc_count'] + (1000000000 if bucket['key'] in browser_lang_codes and bucket['doc_count'] >= total_doc_count//100 else 0), reverse=True)

    content_type_buckets = list(search_results_raw['aggregations']['content_type']['buckets'])
    md5_content_type_mapping = get_md5_content_type_mapping(display_lang)
    book_any_total = sum([bucket['doc_count'] for bucket in content_type_buckets if bucket['key'] in md5_content_type_book_any_subtypes])
    content_type_buckets.append({'key': 'book_any', 'doc_count': book_any_total})
    all_aggregations['content_type'] = [{ 'key': bucket['key'], 'label': md5_content_type_mapping[bucket['key']], 'doc_count': bucket['doc_count'] } for bucket in content_type_buckets]
    all_aggregations['content_type'] = sorted(all_aggregations['content_type'], key=lambda bucket: bucket['doc_count'], reverse=True)

    # Similarly to the "unknown language" issue above, we have to filter for empty-string extensions, since it gives too much trouble.
    all_aggregations['extension_best'] = []
    for bucket in search_results_raw['aggregations']['extension_best']['buckets']:
        if bucket['key'] == '':
            all_aggregations['extension_best'].append({ 'key': '_empty', 'label': 'unknown', 'doc_count': bucket['doc_count'] })
        else:
            all_aggregations['extension_best'].append({ 'key': bucket['key'], 'label': bucket['key'], 'doc_count': bucket['doc_count'] })

    return all_aggregations



@page.get("/search")
def search_page():
    search_input = request.args.get("q", "").strip()
    filter_values = {
        'most_likely_language_code': request.args.get("lang", "").strip()[0:15],
        'content_type': request.args.get("content", "").strip()[0:25],
        'extension_best': request.args.get("ext", "").strip()[0:10],
    }
    sort_value = request.args.get("sort", "").strip()

    if bool(re.match(r"^[a-fA-F\d]{32}$", search_input)):
        return redirect(f"/md5/{search_input}", code=301)

    if bool(re.match(r"^OL\d+M$", search_input)):
        return redirect(f"/ol/{search_input}", code=301)

    if looks_like_doi(search_input):
        return redirect(f"/doi/{search_input}", code=301)

    canonical_isbn13 = isbnlib.get_canonical_isbn(search_input, output='isbn13')
    if len(canonical_isbn13) == 13 and len(isbnlib.info(canonical_isbn13)) > 0:
        return redirect(f"/isbn/{canonical_isbn13}", code=301)

    post_filter = []
    for filter_key, filter_value in filter_values.items():
        if filter_value != '':
            if filter_key == 'content_type' and filter_value == 'book_any':
                post_filter.append({ "terms": { f"file_unified_data.content_type": md5_content_type_book_any_subtypes } })
            elif filter_value == '_empty':
                post_filter.append({ "term": { f"file_unified_data.{filter_key}": '' } })
            else:
                post_filter.append({ "term": { f"file_unified_data.{filter_key}": filter_value } })

    custom_search_sorting = []
    if sort_value == "newest":
        custom_search_sorting = [{ "file_unified_data.year_best": "desc" }]
    if sort_value == "oldest":
        custom_search_sorting = [{ "file_unified_data.year_best": "asc" }]
    if sort_value == "largest":
        custom_search_sorting = [{ "file_unified_data.filesize_best": "desc" }]
    if sort_value == "smallest":
        custom_search_sorting = [{ "file_unified_data.filesize_best": "asc" }]

    search_query = {
        "bool": {
            "should": [{
                "script_score": {
                    "query": { "match_phrase": { "search_only_fields.search_text": { "query": search_input } } },
                    "script": {
                        "source": sort_search_md5_dicts_script,
                        "params": { "lang_code": get_locale().language, "boost": 100000 }
                    }
                }
            }],
            "must": [{
                "script_score": {
                    "query": { "simple_query_string": {"query": search_input, "fields": ["search_only_fields.search_text"], "default_operator": "and"} },
                    "script": {
                        "source": sort_search_md5_dicts_script,
                        "params": { "lang_code": get_locale().language, "boost": 0 }
                    }
                }
            }]
        }
    }

    try:
        max_display_results = 200
        max_additional_display_results = 50

        search_results_raw = es.search(
            index="md5_dicts", 
            size=max_display_results, 
            query=search_query,
            aggs=search_query_aggs,
            post_filter={ "bool": { "filter": post_filter } },
            sort=custom_search_sorting+['_score'],
            track_total_hits=False,
            timeout=ES_TIMEOUT,
        )

        all_aggregations = all_search_aggs(get_locale().language)

        doc_counts = {}
        doc_counts['most_likely_language_code'] = {}
        doc_counts['content_type'] = {}
        doc_counts['extension_best'] = {}
        if search_input == '':
            for bucket in all_aggregations['most_likely_language_code']:
                doc_counts['most_likely_language_code'][bucket['key']] = bucket['doc_count']
            for bucket in all_aggregations['content_type']:
                doc_counts['content_type'][bucket['key']] = bucket['doc_count']
            for bucket in all_aggregations['extension_best']:
                doc_counts['extension_best'][bucket['key']] = bucket['doc_count']
        else:
            for bucket in search_results_raw['aggregations']['most_likely_language_code']['buckets']:
                doc_counts['most_likely_language_code'][bucket['key'] if bucket['key'] != '' else '_empty'] = bucket['doc_count']
            # Special casing for "book_any":
            doc_counts['content_type']['book_any'] = 0
            for bucket in search_results_raw['aggregations']['content_type']['buckets']:
                doc_counts['content_type'][bucket['key']] = bucket['doc_count']
                if bucket['key'] in md5_content_type_book_any_subtypes:
                    doc_counts['content_type']['book_any'] += bucket['doc_count']
            for bucket in search_results_raw['aggregations']['extension_best']['buckets']:
                doc_counts['extension_best'][bucket['key'] if bucket['key'] != '' else '_empty'] = bucket['doc_count']

        aggregations = {}
        aggregations['most_likely_language_code'] = [{
                **bucket,
                'doc_count': doc_counts['most_likely_language_code'].get(bucket['key'], 0),
                'selected':  (bucket['key'] == filter_values['most_likely_language_code']),
            } for bucket in all_aggregations['most_likely_language_code']]
        aggregations['content_type'] = [{
                **bucket,
                'doc_count': doc_counts['content_type'].get(bucket['key'], 0),
                'selected':  (bucket['key'] == filter_values['content_type']),
            } for bucket in all_aggregations['content_type']]
        aggregations['extension_best'] = [{
                **bucket,
                'doc_count': doc_counts['extension_best'].get(bucket['key'], 0),
                'selected':  (bucket['key'] == filter_values['extension_best']),
            } for bucket in all_aggregations['extension_best']]

        aggregations['most_likely_language_code'] = sorted(aggregations['most_likely_language_code'], key=lambda bucket: bucket['doc_count'], reverse=True)
        aggregations['content_type']              = sorted(aggregations['content_type'],              key=lambda bucket: bucket['doc_count'], reverse=True)
        aggregations['extension_best']            = sorted(aggregations['extension_best'],            key=lambda bucket: bucket['doc_count'], reverse=True)


        search_md5_dicts = [add_additional_to_md5_dict({'md5': md5_dict['_id'], **md5_dict['_source']}) for md5_dict in search_results_raw['hits']['hits'] if md5_dict['_id'] not in search_filtered_bad_md5s]

        max_search_md5_dicts_reached = False
        max_additional_search_md5_dicts_reached = False
        additional_search_md5_dicts = []

        if len(search_md5_dicts) < max_display_results:
            # For partial matches, first try our original query again but this time without filters.
            seen_md5s = set([md5_dict['md5'] for md5_dict in search_md5_dicts])
            search_results_raw = es.search(
                index="md5_dicts", 
                size=len(seen_md5s)+max_additional_display_results, # This way, we'll never filter out more than "max_display_results" results because we have seen them already., 
                query=search_query,
                sort=custom_search_sorting+['_score'],
                track_total_hits=False,
                timeout=ES_TIMEOUT,
            )
            if len(seen_md5s)+len(search_results_raw['hits']['hits']) >= max_additional_display_results:
                max_additional_search_md5_dicts_reached = True
            additional_search_md5_dicts = [add_additional_to_md5_dict({'md5': md5_dict['_id'], **md5_dict['_source']}) for md5_dict in search_results_raw['hits']['hits'] if md5_dict['_id'] not in seen_md5s and md5_dict['_id'] not in search_filtered_bad_md5s]

            # Then do an "OR" query, but this time with the filters again.
            if len(search_md5_dicts) + len(additional_search_md5_dicts) < max_display_results:
                seen_md5s = seen_md5s.union(set([md5_dict['md5'] for md5_dict in additional_search_md5_dicts]))
                search_results_raw = es.search(
                    index="md5_dicts",
                    size=len(seen_md5s)+max_additional_display_results, # This way, we'll never filter out more than "max_display_results" results because we have seen them already.
                    # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                    query={"bool": { "must": { "match": { "search_only_fields.search_text": { "query": search_input } } }, "filter": post_filter } },
                    sort=custom_search_sorting+['_score'],
                    track_total_hits=False,
                    timeout=ES_TIMEOUT,
                )
                if len(seen_md5s)+len(search_results_raw['hits']['hits']) >= max_additional_display_results:
                    max_additional_search_md5_dicts_reached = True
                additional_search_md5_dicts += [add_additional_to_md5_dict({'md5': md5_dict['_id'], **md5_dict['_source']}) for md5_dict in search_results_raw['hits']['hits'] if md5_dict['_id'] not in seen_md5s and md5_dict['_id'] not in search_filtered_bad_md5s]

                # If we still don't have enough, do another OR query but this time without filters.
                if len(search_md5_dicts) + len(additional_search_md5_dicts) < max_display_results:
                    seen_md5s = seen_md5s.union(set([md5_dict['md5'] for md5_dict in additional_search_md5_dicts]))
                    search_results_raw = es.search(
                        index="md5_dicts",
                        size=len(seen_md5s)+max_additional_display_results, # This way, we'll never filter out more than "max_display_results" results because we have seen them already.
                        # Don't use our own sorting here; otherwise we'll get a bunch of garbage at the top typically.
                        query={"bool": { "must": { "match": { "search_only_fields.search_text": { "query": search_input } } } } },
                        sort=custom_search_sorting+['_score'],
                        track_total_hits=False,
                        timeout=ES_TIMEOUT,
                    )
                    if len(seen_md5s)+len(search_results_raw['hits']['hits']) >= max_additional_display_results:
                        max_additional_search_md5_dicts_reached = True
                    additional_search_md5_dicts += [add_additional_to_md5_dict({'md5': md5_dict['_id'], **md5_dict['_source']}) for md5_dict in search_results_raw['hits']['hits'] if md5_dict['_id'] not in seen_md5s and md5_dict['_id'] not in search_filtered_bad_md5s]
        else:
            max_search_md5_dicts_reached = True

        
        search_dict = {}
        search_dict['search_md5_dicts'] = search_md5_dicts[0:max_display_results]
        search_dict['additional_search_md5_dicts'] = additional_search_md5_dicts[0:max_additional_display_results]
        search_dict['max_search_md5_dicts_reached'] = max_search_md5_dicts_reached
        search_dict['max_additional_search_md5_dicts_reached'] = max_additional_search_md5_dicts_reached
        search_dict['aggregations'] = aggregations
        search_dict['sort_value'] = sort_value

        return render_template(
            "page/search.html",
            header_active="search",
            search_input=search_input,
            search_dict=search_dict,
        )
    except Exception as err:
        raise
        print("Search error: ", err)

        return render_template(
            "page/search.html",
            header_active="search",
            search_input=search_input,
            search_dict=None,
        ), 500
