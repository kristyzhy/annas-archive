import jwt
import re
import ipaddress
import flask
import functools
import datetime
import forex_python.converter
import cachetools
import babel.numbers
import babel
import os
import base64
import base58
import hashlib
import urllib.parse
import orjson
import isbnlib
import math
import bip_utils
from flask_babel import gettext, get_babel, force_locale

from flask import Blueprint, request, g, make_response, render_template
from flask_cors import cross_origin
from sqlalchemy import select, func, text, inspect
from sqlalchemy.orm import Session
from flask_babel import format_timedelta

from allthethings.extensions import es, engine, mariapersist_engine, MariapersistDownloadsTotalByMd5, mail, MariapersistDownloadsHourlyByMd5, MariapersistDownloadsHourly, MariapersistMd5Report, MariapersistAccounts, MariapersistComments, MariapersistReactions, MariapersistLists, MariapersistListEntries, MariapersistDonations, MariapersistDownloads, MariapersistFastDownloadAccess
from config.settings import SECRET_KEY, DOWNLOADS_SECRET_KEY, MEMBERS_TELEGRAM_URL, FLASK_DEBUG, BIP39_MNEMONIC

FEATURE_FLAGS = { "isbn": FLASK_DEBUG }

def validate_canonical_md5s(canonical_md5s):
    return all([bool(re.match(r"^[a-f\d]{32}$", canonical_md5)) for canonical_md5 in canonical_md5s])

def validate_aarecord_ids(aarecord_ids):
    try:
        split_ids = split_aarecord_ids(aarecord_ids)
    except:
        return False
    return validate_canonical_md5s(split_ids['md5'])

def split_aarecord_ids(aarecord_ids):
    ret = {'md5': [], 'ia': [], 'isbn': []}
    for aarecord_id in aarecord_ids:
        split_aarecord_id = aarecord_id.split(':')
        ret[split_aarecord_id[0]].append(split_aarecord_id[1])
    return ret

JWT_PREFIX = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.'

ACCOUNT_COOKIE_NAME = "aa_account_id2"

def strip_jwt_prefix(jwt_payload):
    if not jwt_payload.startswith(JWT_PREFIX):
        raise Exception("Invalid jwt_payload; wrong prefix")
    return jwt_payload[len(JWT_PREFIX):]

def get_account_id(cookies):
    if len(cookies.get(ACCOUNT_COOKIE_NAME, "")) > 0:
        account_data = jwt.decode(
            jwt=JWT_PREFIX + cookies[ACCOUNT_COOKIE_NAME],
            key=SECRET_KEY,
            algorithms=["HS256"],
            options={ "verify_signature": True, "require": ["iat"], "verify_iat": True }
        )
        return account_data["a"]
    return None

def secret_key_from_account_id(account_id):
    hashkey = base58.b58encode(hashlib.md5(f"{SECRET_KEY}{account_id}".encode('utf-8')).digest()).decode('utf-8')
    return f"{account_id}{hashkey}"

def account_id_from_secret_key(secret_key):
    account_id = secret_key[0:7]
    correct_secret_key = secret_key_from_account_id(account_id)
    if secret_key != correct_secret_key:
        return None
    return account_id

def get_domain_lang_code(locale):
    if locale.script == 'Hant':
        return 'tw'
    elif str(locale) == 'nb_NO':
        return 'no'
    else:
        return str(locale)

def domain_lang_code_to_full_lang_code(domain_lang_code):
    if domain_lang_code == "tw":
        return 'zh_Hant'
    elif domain_lang_code == "no":
        return 'nb_NO'
    else:
        return domain_lang_code

def get_full_lang_code(locale):
    return str(locale)

def get_base_lang_code(locale):
    return locale.language

# Adapted from https://github.com/python-babel/flask-babel/blob/69d3340cd0ff52f3e23a47518285a7e6d8f8c640/flask_babel/__init__.py#L175
def list_translations():
    # return [locale for locale in babel.list_translations() if is_locale(locale)]
    result = []
    for dirname in get_babel().translation_directories:
        if not os.path.isdir(dirname):
            continue
        for folder in os.listdir(dirname):
            locale_dir = os.path.join(dirname, folder, 'LC_MESSAGES')
            if not os.path.isdir(locale_dir):
                continue
            if any(x.endswith('.mo') for x in os.listdir(locale_dir)):
                try:
                    result.append(babel.Locale.parse(folder))
                except babel.UnknownLocaleError:
                    pass
    return result

# Example to convert back from MySQL to IPv4:
# import ipaddress
# ipaddress.ip_address(0x2002AC16000100000000000000000000).sixtofour
# ipaddress.ip_address().sixtofour
def canonical_ip_bytes(ip):
    # Canonicalize to IPv6
    ipv6 = ipaddress.ip_address(ip)
    if ipv6.version == 4:
        # https://stackoverflow.com/a/19853184
        prefix = int(ipaddress.IPv6Address('2002::'))
        ipv6 = ipaddress.ip_address(prefix | (int(ipv6) << 80))
    return ipv6.packed


def public_cache(cloudflare_minutes=0, minutes=0):
    def fwrap(f):
        @functools.wraps(f)
        def wrapped_f(*args, **kwargs):
            r = flask.make_response(f(*args, **kwargs))
            if r.status_code <= 299:
                r.headers.add('Cache-Control', f"public,max-age={int(60 * minutes)},s-maxage={int(60 * minutes)}")
                r.headers.add('Cloudflare-CDN-Cache-Control', f"max-age={int(60 * cloudflare_minutes)}")
            else:
                r.headers.add('Cache-Control', 'no-cache')
                r.headers.add('Cloudflare-CDN-Cache-Control', 'no-cache')
            return r
        return wrapped_f
    return fwrap

def no_cache():
    def fwrap(f):
        @functools.wraps(f)
        def wrapped_f(*args, **kwargs):
            r = flask.make_response(f(*args, **kwargs))
            r.headers.add('Cache-Control', 'no-cache')
            r.headers.add('Cloudflare-CDN-Cache-Control', 'no-cache')
            return r
        return wrapped_f
    return fwrap

def get_md5_report_type_mapping():
    return {
        'metadata': 'Incorrect metadata (e.g. title, description, cover image)',
        'download': 'Downloading problems (e.g. can’t connect, error message, very slow)',
        'broken': 'File can’t be opened (e.g. corrupted file, DRM)',
        'pages': 'Poor quality (e.g. formatting issues, poor scan quality, missing pages)',
        'spam': 'Spam / file should be removed (e.g. advertising, abusive content)',
        'copyright': 'Copyright claim',
        'other': 'Other',
    }

@cachetools.cached(cache=cachetools.TTLCache(maxsize=1024, ttl=6*60*60))
def usd_currency_rates_cached():
    # try:
    #     return forex_python.converter.CurrencyRates().get_rates('USD')
    # except forex_python.converter.RatesNotAvailableError:
    #     print("RatesNotAvailableError -- using fallback!")
    #     # 2023-05-04 fallback
    return {'EUR': 0.9161704076958315, 'JPY': 131.46129180027486, 'BGN': 1.7918460833715073, 'CZK': 21.44663307375172, 'DKK': 6.8263857077416406, 'GBP': 0.8016032982134678, 'HUF': 344.57169033440226, 'PLN': 4.293449381584975, 'RON': 4.52304168575355, 'SEK': 10.432890517636281, 'CHF': 0.9049931287219424, 'ISK': 137.15071003206597, 'NOK': 10.43105817682089, 'TRY': 19.25744388456253, 'AUD': 1.4944571690334403, 'BRL': 5.047732478240953, 'CAD': 1.3471369674759506, 'CNY': 6.8725606962895105, 'HKD': 7.849931287219422, 'IDR': 14924.993128721942, 'INR': 81.87402656894183, 'KRW': 1318.1951442968393, 'MXN': 18.288960146587264, 'MYR': 4.398992212551534, 'NZD': 1.592945487860742, 'PHP': 54.56894182317912, 'SGD': 1.3290884104443428, 'THB': 34.054970224461755, 'ZAR': 18.225286303252407}

def account_is_member(account):
    return (account is not None) and (account.membership_expiration is not None) and (account.membership_expiration > datetime.datetime.now()) and (int(account.membership_tier or "0") >= 2)

@functools.cache
def membership_tier_names(locale):
    with force_locale(locale):
        return { 
            "2": gettext('common.membership.tier_name.2'),
            "3": gettext('common.membership.tier_name.3'),
            "4": gettext('common.membership.tier_name.4'),
            "5": gettext('common.membership.tier_name.5'),
        }

MEMBERSHIP_TIER_COSTS = { 
    "2": 5, "3": 10, "4": 30, "5": 100,
}
MEMBERSHIP_METHOD_DISCOUNTS = {
    # Note: keep manually in sync with HTML.
    "crypto": 20,
    # "cc":     20,
    "binance": 20,
    "paypal": 20,
    "paypalreg": 0,
    "amazon": 0,
    # "bmc":    0,
    # "alipay": 0,
    # "pix":    0,
    "payment1": 0,
    "givebutter": 0,
}
MEMBERSHIP_DURATION_DISCOUNTS = {
    # Note: keep manually in sync with HTML.
    "1": 0, "3": 5, "6": 10, "12": 15, "24": 25,
}
MEMBERSHIP_DOWNLOADS_PER_DAY = {
    "2": 20, "3": 50, "4": 100, "5": 1000,
}
MEMBERSHIP_TELEGRAM_URL = {
    "2": "", "3": "", "4": MEMBERS_TELEGRAM_URL, "5": MEMBERS_TELEGRAM_URL,
}
MEMBERSHIP_METHOD_MINIMUM_CENTS_USD = {
    "crypto": 0,
    # "cc":     20,
    "binance": 0,
    "paypal": 3500,
    "paypalreg": 0,
    "amazon": 1000,
    # "bmc":    0,
    # "alipay": 0,
    # "pix":    0,
    "payment1": 0,
    "givebutter": 500,
}

MEMBERSHIP_METHOD_MAXIMUM_CENTS_NATIVE = {
    "payment1": 40000,
}

def get_account_fast_download_info(mariapersist_session, account_id):
    account = mariapersist_session.connection().execute(select(MariapersistAccounts).where(MariapersistAccounts.account_id == account_id).limit(1)).first()
    if not account_is_member(account):
        return None
    downloads_left = MEMBERSHIP_DOWNLOADS_PER_DAY[account.membership_tier]
    recently_downloaded_md5s = [md5.hex() for md5 in mariapersist_session.connection().execute(select(MariapersistFastDownloadAccess.md5).where((MariapersistFastDownloadAccess.timestamp >= datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)) & (MariapersistFastDownloadAccess.account_id == account_id)).limit(10000)).scalars()]
    downloads_left -= len(recently_downloaded_md5s)
    return { 'downloads_left': max(0, downloads_left), 'recently_downloaded_md5s': recently_downloaded_md5s, 'downloads_per_day': MEMBERSHIP_DOWNLOADS_PER_DAY[account.membership_tier], 'telegram_url': MEMBERSHIP_TELEGRAM_URL[account.membership_tier] }

def cents_to_usd_str(cents):
    return str(cents)[:-2] + "." + str(cents)[-2:]

def format_currency(cost_cents_native_currency, native_currency_code, locale):
    output = babel.numbers.format_currency(cost_cents_native_currency / 100, native_currency_code, locale=locale)
    if output.endswith('.00') or output.endswith(',00'):
        output = output[0:-3]
    return output

def membership_format_native_currency(locale, native_currency_code, cost_cents_native_currency, cost_cents_usd):
    if native_currency_code != 'USD':
        return {
            'cost_cents_native_currency_str_calculator': f"{format_currency(cost_cents_native_currency, native_currency_code, locale)} ({format_currency(cost_cents_usd, 'USD', locale)}) total",
            'cost_cents_native_currency_str_button': f"{format_currency(cost_cents_native_currency, native_currency_code, locale)}",
            'cost_cents_native_currency_str_donation_page_formal': f"{format_currency(cost_cents_native_currency, native_currency_code, locale)} ({format_currency(cost_cents_usd, 'USD', locale)})",
            'cost_cents_native_currency_str_donation_page_instructions': f"{format_currency(cost_cents_native_currency, native_currency_code, locale)} ({format_currency(cost_cents_usd, 'USD', locale)})",
        }
    # elif native_currency_code == 'COFFEE':
    #     return {
    #         'cost_cents_native_currency_str_calculator': f"{format_currency(cost_cents_native_currency * 5, 'USD', locale)} ({cost_cents_native_currency} ☕️) total",
    #         'cost_cents_native_currency_str_button': f"{format_currency(cost_cents_native_currency * 5, 'USD', locale)}",
    #         'cost_cents_native_currency_str_donation_page_formal': f"{format_currency(cost_cents_native_currency * 5, 'USD', locale)} ({cost_cents_native_currency} ☕️)",
    #         'cost_cents_native_currency_str_donation_page_instructions': f"{cost_cents_native_currency} “coffee” ({format_currency(cost_cents_native_currency * 5, 'USD', locale)})",
    #     }
    else:
        return {
            'cost_cents_native_currency_str_calculator': f"{format_currency(cost_cents_native_currency, 'USD', locale)} total",
            'cost_cents_native_currency_str_button': f"{format_currency(cost_cents_native_currency, 'USD', locale)}",
            'cost_cents_native_currency_str_donation_page_formal': f"{format_currency(cost_cents_native_currency, 'USD', locale)}",
            'cost_cents_native_currency_str_donation_page_instructions': f"{format_currency(cost_cents_native_currency, 'USD', locale)}",
        }

@cachetools.cached(cache=cachetools.TTLCache(maxsize=1024, ttl=60*60))
def membership_costs_data(locale):
    usd_currency_rates = usd_currency_rates_cached()

    def calculate_membership_costs(inputs):
        tier = inputs['tier']
        method = inputs['method']
        duration = inputs['duration']
        if (tier not in MEMBERSHIP_TIER_COSTS.keys()) or (method not in MEMBERSHIP_METHOD_DISCOUNTS.keys()) or (duration not in MEMBERSHIP_DURATION_DISCOUNTS.keys()):
            raise Exception("Invalid fields")

        discounts = MEMBERSHIP_METHOD_DISCOUNTS[method] + MEMBERSHIP_DURATION_DISCOUNTS[duration]
        monthly_cents = round(MEMBERSHIP_TIER_COSTS[tier]*(100-discounts));
        cost_cents_usd = monthly_cents * int(duration);

        native_currency_code = 'USD'
        cost_cents_native_currency = cost_cents_usd
        if method in ['alipay', 'payment1']:
            native_currency_code = 'CNY'
            cost_cents_native_currency = math.floor(cost_cents_usd * 7 / 100) * 100
        # elif method == 'bmc':
        #     native_currency_code = 'COFFEE'
        #     cost_cents_native_currency = round(cost_cents_usd / 500)
        elif method == 'amazon':
            if cost_cents_usd <= 1000:
                cost_cents_usd = 1000
            elif cost_cents_usd <= 1500:
                cost_cents_usd = 1500
            elif cost_cents_usd <= 2000:
                cost_cents_usd = 2000
            elif cost_cents_usd <= 2700:
                cost_cents_usd = 2500
            elif cost_cents_usd == 5100:
                cost_cents_usd = 4500
            elif cost_cents_usd == 5400:
                cost_cents_usd = 5500
            elif cost_cents_usd == 8550:
                cost_cents_usd = 8500
            elif cost_cents_usd == 9000:
                cost_cents_usd = 8500
            elif cost_cents_usd == 30600:
                cost_cents_usd = 30000
            elif cost_cents_usd <= 100000:
                cost_cents_usd = round(cost_cents_usd / 1000) * 1000
            elif cost_cents_usd <= 200000:
                cost_cents_usd = math.ceil(cost_cents_usd / 5000) * 5000
            else:
                cost_cents_usd = math.ceil(cost_cents_usd / 10000) * 10000
            cost_cents_native_currency = cost_cents_usd
        elif method == 'pix':
            native_currency_code = 'BRL'
            cost_cents_native_currency = round(cost_cents_usd * usd_currency_rates['BRL'] / 100) * 100

        formatted_native_currency = membership_format_native_currency(locale, native_currency_code, cost_cents_native_currency, cost_cents_usd)

        return { 
            'cost_cents_usd': cost_cents_usd, 
            'cost_cents_usd_str': babel.numbers.format_currency(cost_cents_usd / 100.0, 'USD', locale=locale), 
            'cost_cents_native_currency': cost_cents_native_currency, 
            'cost_cents_native_currency_str_calculator': formatted_native_currency['cost_cents_native_currency_str_calculator'], 
            'cost_cents_native_currency_str_button': formatted_native_currency['cost_cents_native_currency_str_button'],
            'native_currency_code': native_currency_code,
            'monthly_cents': monthly_cents,
            'monthly_cents_str': babel.numbers.format_currency(monthly_cents / 100.0, 'USD', locale=locale),
            'discounts': discounts,
            'duration': duration,
            'tier_name': membership_tier_names(locale)[tier],
        }

    data = {}
    for tier in MEMBERSHIP_TIER_COSTS.keys():
        for method in MEMBERSHIP_METHOD_DISCOUNTS.keys():
            for duration in MEMBERSHIP_DURATION_DISCOUNTS.keys():
                inputs = { 'tier': tier, 'method': method, 'duration': duration }
                data[f"{tier},{method},{duration}"] = calculate_membership_costs(inputs)
    return data

@cachetools.cached(cache=cachetools.LRUCache(maxsize=1024))
def crypto_addresses(year, month, day):
    days_elapsed = (datetime.date(year, month, day) - datetime.date(2023, 9, 1)).days

    # BTC 
    base_account_number = (days_elapsed // 3) * 2
    btc_address_one_time_donation = bip_utils.Bip44.FromSeed(bip_utils.Bip39SeedGenerator(BIP39_MNEMONIC).Generate(), bip_utils.Bip44Coins.BITCOIN).Purpose().Coin().Account(base_account_number+0).Change(bip_utils.Bip44Changes.CHAIN_EXT).AddressIndex(0).PublicKey().ToAddress()
    btc_address_membership_donation = bip_utils.Bip44.FromSeed(bip_utils.Bip39SeedGenerator(BIP39_MNEMONIC).Generate(), bip_utils.Bip44Coins.BITCOIN).Purpose().Coin().Account(base_account_number+1).Change(bip_utils.Bip44Changes.CHAIN_EXT).AddressIndex(0).PublicKey().ToAddress()
    eth_address_one_time_donation = bip_utils.Bip44.FromSeed(bip_utils.Bip39SeedGenerator(BIP39_MNEMONIC).Generate(), bip_utils.Bip44Coins.ETHEREUM).Purpose().Coin().Account(base_account_number+0).Change(bip_utils.Bip44Changes.CHAIN_EXT).AddressIndex(0).PublicKey().ToAddress()
    eth_address_membership_donation = bip_utils.Bip44.FromSeed(bip_utils.Bip39SeedGenerator(BIP39_MNEMONIC).Generate(), bip_utils.Bip44Coins.ETHEREUM).Purpose().Coin().Account(base_account_number+1).Change(bip_utils.Bip44Changes.CHAIN_EXT).AddressIndex(0).PublicKey().ToAddress()

    return {
        "btc_address_one_time_donation": btc_address_one_time_donation,
        "btc_address_membership_donation": btc_address_membership_donation,
        "eth_address_one_time_donation": eth_address_one_time_donation,
        "eth_address_membership_donation": eth_address_membership_donation,
    }

def crypto_addresses_today():
    utc_now = datetime.datetime.utcnow() 
    return crypto_addresses(utc_now.year, utc_now.month, utc_now.day)

def make_anon_download_uri(limit_multiple, speed_kbps, path, filename, domain):
    limit_multiple_field = 'y' if limit_multiple else 'x'
    expiry = int((datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=6)).timestamp())
    md5 = base64.urlsafe_b64encode(hashlib.md5(f"{domain}/{limit_multiple_field}/{expiry}/{speed_kbps}/{path},{DOWNLOADS_SECRET_KEY}".encode('utf-8')).digest()).decode('utf-8').rstrip('=')
    return f"d2/{limit_multiple_field}/{expiry}/{speed_kbps}/{urllib.parse.quote(path)}~/{md5}/{filename}"

DICT_COMMENTS_NO_API_DISCLAIMER = "This page is *not* intended as an API. If you need programmatic access to this JSON, please set up your own instance. For more information, see: https://annas-archive.org/datasets and https://annas-software.org/AnnaArchivist/annas-archive/-/tree/main/data-imports"

COMMON_DICT_COMMENTS = {
    "identifier": ("after", ["Typically ISBN-10 or ISBN-13."]),
    "identifierwodash": ("after", ["Same as 'identifier' but without dashes."]),
    "locator": ("after", ["Original filename or path on the Library Genesis servers."]),
    "stripped_description": ("before", ["Anna's Archive version of the 'descr' or 'description' field, with HTML tags removed or replaced with regular whitespace."]),
    "language_codes": ("before", ["Anna's Archive version of the 'language' field, where we attempted to parse it into BCP 47 tags."]),
    "cover_url_normalized": ("after", ["Anna's Archive version of the 'coverurl' field, where we attempted to turn it into a full URL."]),
    "edition_varia_normalized": ("after", ["Anna's Archive version of the 'series', 'volume', 'edition', 'periodical', and 'year' fields; combining them into a single field for display and search."]),
    "topic_descr": ("after", ["A description of the 'topic' field using a separate database table, which seems to have its roots in the Kolxo3 library that Libgen was originally based on.",
                    "https://wiki.mhut.org/content:bibliographic_data says that this field will be deprecated in favor of Dewey Decimal."]),
    "topic": ("after", ["See 'topic_descr' below."]),
    "searchable": ("after", ["This seems to indicate that the book has been OCR'ed."]),
    "generic": ("after", ["If this is set to a different md5, then that version is preferred over this one, and should be shown in search results instead."]),
    "visible": ("after", ["If this is set, the book is in fact *not* visible in Libgen, and this string describes the reason."]),
    "commentary": ("after", ["Comments left by the uploader, an admin, or an automated process."]),
    "toc": ("before", ["Table of contents. May contain HTML."]),
    "ddc": ("after", ["See also https://libgen.li/biblioservice.php?type=ddc"]),
    "udc": ("after", ["See also https://libgen.li/biblioservice.php?type=udc"]),
    "lbc": ("after", ["See also https://libgen.li/biblioservice.php?type=bbc and https://www.isko.org/cyclo/lbc"]),
    "descriptions_mapped": ("before", ["Normalized fields by Anna's Archive, taken from the various `*_add_descr` Libgen.li tables, with comments taken from the `elem_descr` table which contain metadata about these fields, as well as sometimes our own metadata.",
                                       "The names themselves are taken from `name_en` in the corresponding `elem_descr` entry (lowercased, whitespace removed), with `name_add{1,2,3}_en` to create the compound keys, such as `isbn_isbnnotes`."]),
    "identifiers_unified": ("before", ["Anna's Archive version of various identity-related fields."]),
    "classifications_unified": ("before", ["Anna's Archive version of various classification-related fields."]),
}

# Hardcoded from the `descr_elems` table.
LGLI_EDITION_TYPE_MAPPING = {
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
LGLI_ISSUE_OTHER_FIELDS = [
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
LGLI_STANDARD_INFO_FIELDS = [
    "standardtype",
    "standardtype_standartnumber",
    "standardtype_standartdate",
    "standartnumber",
    "standartstatus",
    "standartstatus_additionalstandartstatus",
]
LGLI_DATE_INFO_FIELDS = [
    "datepublication",
    "dateintroduction",
    "dateactualizationtext",
    "dateregistration",
    "dateactualizationdescr",
    "dateexpiration",
    "datelastedition",
]
# Hardcoded from the `libgenli_elem_descr` table.
LGLI_IDENTIFIERS = {
    "asin": { "label": "ASIN", "url": "https://www.amazon.com/dp/%s", "description": "Amazon Standard Identification Number"},
    "audibleasin": { "label": "Audible-ASIN", "url": "https://www.audible.com/pd/%s", "description": "Audible ASIN"},
    "bl": { "label": "BL", "url": "http://explore.bl.uk/primo_library/libweb/action/dlDisplay.do?vid=BLVU1&amp;docId=BLL01%s", "description": "The British Library"},
    "bleilerearlyyears": { "label": "Bleiler Early Years", "url": "", "description": "Richard Bleiler, Everett F. Bleiler. Science-Fiction: The Early Years. Kent State University Press, 1991, xxiii+998 p."},
    "bleilergernsback": { "label": "Bleiler Gernsback", "url": "", "description": "Everett F. Bleiler, Richard Bleiler. Science-Fiction: The Gernsback Years. Kent State University Press, 1998, xxxii+730pp"},
    "bleilersupernatural": { "label": "Bleiler Supernatural", "url": "", "description": "Everett F. Bleiler. The Guide to Supernatural Fiction. Kent State University Press, 1983, xii+723 p."},
    "bn": { "label": "BN", "url": "http://www.barnesandnoble.com/s/%s", "description": "Barnes and Noble"},
    "bnb": { "label": "BNB", "url": "http://search.bl.uk/primo_library/libweb/action/search.do?fn=search&vl(freeText0)=%s", "description": "The British National Bibliography"},
    "bnf": { "label": "BNF", "url": "http://catalogue.bnf.fr/ark:/12148/%s", "description": "Bibliotheque nationale de France"},
    "coollibbookid": { "label": "Coollib", "url": "https://coollib.ru/b/%s", "description":""},
    "copac": { "label": "COPAC", "url": "http://copac.jisc.ac.uk/id/%s?style=html", "description": "UK/Irish union catalog"},
    "crossrefbookid": { "label": "Crossref", "url": "https://data.crossref.org/depositorreport?pubid=%s", "description":""},
    "dnb": { "label": "DNB", "url": "http://d-nb.info/%s", "description": "Deutsche Nationalbibliothek"},
    "fantlabeditionid": { "label": "FantLab Edition ID", "url": "https://fantlab.ru/edition%s", "description": "Лаболатория фантастики"},
    "flibustabookid": { "label": "Flibusta", "url": "https://flibusta.is/b/%s", "description":""},
    "goodreads": { "label": "Goodreads", "url": "http://www.goodreads.com/book/show/%s", "description": "Goodreads social cataloging site"},
    "googlebookid": { "label": "Google Books", "url": "https://books.google.com/books?id=%s", "description": ""},
    "isfdbpubideditions": { "label": "ISFDB (editions)", "url": "http://www.isfdb.org/cgi-bin/pl.cgi?%s", "description": ""},
    "issn": { "label": "ISSN", "url": "https://urn.issn.org/urn:issn:%s", "description": "International Standard Serial Number"},
    "jnbjpno": { "label": "JNB/JPNO", "url": "https://iss.ndl.go.jp/api/openurl?ndl_jpno=%s&amp;locale=en", "description": "The Japanese National Bibliography"},
    "jstorstableid": { "label": "JSTOR Stable", "url": "https://www.jstor.org/stable/%s", "description": ""},
    "kbr": { "label": "KBR", "url": "https://opac.kbr.be/Library/doc/SYRACUSE/%s/", "description": "De Belgische Bibliografie/La Bibliographie de Belgique"},
    "lccn": { "label": "LCCN", "url": "http://lccn.loc.gov/%s", "description": "Library of Congress Control Number"},
    "librusecbookid": { "label": "Librusec", "url": "https://lib.rus.ec/b/%s", "description":""},
    "litmirbookid": { "label": "Litmir", "url": "https://www.litmir.me/bd/?b=%s", "description":""},
    "ltf": { "label": "LTF", "url": "http://www.tercerafundacion.net/biblioteca/ver/libro/%s", "description": "La Tercera Fundaci&#243;n"},
    "maximabookid": { "label": "Maxima", "url": "http://maxima-library.org/mob/b/%s", "description":""},
    "ndl": { "label": "NDL", "url": "http://id.ndl.go.jp/bib/%s/eng", "description": "National Diet Library"},
    "nilf": { "label": "NILF", "url": "http://nilf.it/%s/", "description": "Numero Identificativo della Letteratura Fantastica / Fantascienza"},
    "nla": { "label": "NLA", "url": "https://nla.gov.au/nla.cat-vn%s", "description": "National Library of Australia"},
    "noosfere": { "label": "NooSFere", "url": "https://www.noosfere.org/livres/niourf.asp?numlivre=%s", "description": "NooSFere"},
    "oclcworldcat": { "label": "OCLC/WorldCat", "url": "https://www.worldcat.org/oclc/%s", "description": "Online Computer Library Center"},
    "openlibrary": { "label": "Open Library", "url": "https://openlibrary.org/books/%s", "description": ""},
    "pii": { "label": "PII", "url": "", "description": "Publisher Item Identifier", "website": "https://en.wikipedia.org/wiki/Publisher_Item_Identifier"},
    "pmcid": { "label": "PMC ID", "url": "https://www.ncbi.nlm.nih.gov/pmc/articles/%s/", "description": "PubMed Central ID"},
    "pmid": { "label": "PMID", "url": "https://pubmed.ncbi.nlm.nih.gov/%s/", "description": "PubMed ID"},
    "porbase": { "label": "PORBASE", "url": "http://id.bnportugal.gov.pt/bib/porbase/%s", "description": "Biblioteca Nacional de Portugal"},
    "ppn": { "label": "PPN", "url": "http://picarta.pica.nl/xslt/DB=3.9/XMLPRS=Y/PPN?PPN=%s", "description": "De Nederlandse Bibliografie Pica Productie Nummer"},
    "reginald1": { "label": "Reginald-1", "url": "", "description": "R. Reginald. Science Fiction and Fantasy Literature: A Checklist, 1700-1974, with Contemporary Science Fiction Authors II. Gale Research Co., 1979, 1141p."},
    "reginald3": { "label": "Reginald-3", "url": "", "description": "Robert Reginald. Science Fiction and Fantasy Literature, 1975-1991: A Bibliography of Science Fiction, Fantasy, and Horror Fiction Books and Nonfiction Monographs. Gale Research Inc., 1992, 1512 p."},
    "sfbg": { "label": "SFBG", "url": "http://www.sfbg.us/book/%s", "description": "Catalog of books published in Bulgaria"},
    "sfleihbuch": { "label": "SF-Leihbuch", "url": "http://www.sf-leihbuch.de/index.cfm?bid=%s", "description": "Science Fiction-Leihbuch-Datenbank"},
}
# Hardcoded from the `libgenli_elem_descr` table.
LGLI_CLASSIFICATIONS = {
    "classification": { "label": "Classification", "url": "", "description": "" },
    "classificationokp": { "label": "OKP", "url": "https://classifikators.ru/okp/%s", "description": "" },
    "classificationgostgroup": { "label": "GOST group", "url": "", "description": "", "website": "https://en.wikipedia.org/wiki/GOST" },
    "classificationoks": { "label": "OKS", "url": "", "description": "" },
    "libraryofcongressclassification": { "label": "LCC", "url": "", "description": "Library of Congress Classification", "website": "https://en.wikipedia.org/wiki/Library_of_Congress_Classification" },
    "udc": { "label": "UDC", "url": "https://libgen.li/biblioservice.php?value=%s&type=udc", "description": "Universal Decimal Classification", "website": "https://en.wikipedia.org/wiki/Universal_Decimal_Classification" },
    "ddc": { "label": "DDC", "url": "https://libgen.li/biblioservice.php?value=%s&type=ddc", "description": "Dewey Decimal", "website": "https://en.wikipedia.org/wiki/List_of_Dewey_Decimal_classes" },
    "lbc": { "label": "LBC", "url": "https://libgen.li/biblioservice.php?value=%s&type=bbc", "description": "Library-Bibliographical Classification", "website": "https://www.isko.org/cyclo/lbc" },
}

LGRS_TO_UNIFIED_IDENTIFIERS_MAPPING = { 
    'asin': 'asin', 
    'googlebookid': 'googlebookid', 
    'openlibraryid': 'openlibrary', 
    'doi': 'doi',
    'issn': 'issn',
}
LGRS_TO_UNIFIED_CLASSIFICATIONS_MAPPING = { 
    'udc': 'udc',
    'ddc': 'ddc',
    'lbc': 'lbc',
    'lcc': 'libraryofcongressclassification', 
}

UNIFIED_IDENTIFIERS = {
    "isbn10": { "label": "ISBN-10", "url": "https://en.wikipedia.org/wiki/Special:BookSources?isbn=%s", "description": ""},
    "isbn13": { "label": "ISBN-13", "url": "https://en.wikipedia.org/wiki/Special:BookSources?isbn=%s", "description": ""},
    "doi": { "label": "DOI", "url": "https://doi.org/%s", "description": "Digital Object Identifier"},
    **LGLI_IDENTIFIERS,
    # Plus more added below!
}
UNIFIED_CLASSIFICATIONS = {
    **LGLI_CLASSIFICATIONS,
    # Plus more added below!
}

OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING = {
    'amazon': 'asin',
    'british_library': 'bl',
    'british_national_bibliography': 'bnb',
    'google': 'googlebookid',
    'isbn_10': 'isbn10',
    'isbn_13': 'isbn13',
    'national_diet_library,_japan': 'ndl',
    'oclc_numbers': 'oclcworldcat',
    'isfdb': 'isfdbpubideditions',
    # Plus more added below!
}
OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING = {
    'dewey_decimal_class': 'ddc',
    'dewey_number': 'ddc',
    'lc_classifications': 'libraryofcongressclassification',
    'library_bibliographical_classification': 'lbc',
    'udc': 'udc',
    # Plus more added below!
}
# Hardcoded labels for OL. The "label" fields in ol_edition.json become "description" instead.
OPENLIB_LABELS = {
    "abaa": "ABAA",
    "abebooks.de": "Abebooks",
    "abwa_bibliographic_number": "ABWA",
    "alibris_id": "Alibris",
    "bayerische_staatsbibliothek": "BSB-ID",
    "bcid": "BCID",
    "better_world_books": "BWB",
    "bhl": "BHL",
    "bibliothèque_nationale_de_france": "BnF",
    "bibsys": "Bibsys",
    "bodleian,_oxford_university": "Bodleian",
    "booklocker.com": "BookLocker",
    "bookmooch": "Book Mooch",
    "booksforyou": "Books For You",
    "bookwire": "BookWire",
    "boston_public_library": "BPL",
    "canadian_national_library_archive": "CNLA",
    "choosebooks": "Choosebooks",
    "cornell_university_library": "Cornell",
    "cornell_university_online_library": "Cornell",
    "dc_books": "DC",
    "depósito_legal": "Depósito Legal",
    "digital_library_pomerania": "Pomerania",
    "discovereads": "Discovereads",
    "dnb": "DNB",
    "dominican_institute_for_oriental_studies_library": "Al Kindi",
    "etsc": "ETSC",
    "fennica": "Fennica",
    "finnish_public_libraries_classification_system": "FPL",
    "folio": "Folio",
    "freebase": "Freebase",
    "goethe_university_library,_frankfurt": "Goethe",
    "goodreads": "Goodreads",
    "grand_comics_database": "Grand Comics DB",
    "harvard": "Harvard",
    "hathi_trust": "Hathi",
    "identificativo_sbn": "SBN",
    "ilmiolibro": "Ilmiolibro",
    "inducks": "INDUCKS",
    "issn": "ISSN",
    "istc": "ISTC",
    "lccn": "LCCN",
    "learnawesome": "LearnAwesome",
    "library_and_archives_canada_cataloguing_in_publication": "CIP",
    "librarything": "Library Thing",
    "libris": "Libris",
    "librivox": "LibriVox",
    "lulu": "Lulu",
    "magcloud": "Magcloud",
    "nbuv": "NBUV",
    "nla": "NLA",
    "nur": "NUR",
    "ocaid": "IA",
    "openstax": "OpenStax",
    "overdrive": "OverDrive",
    "paperback_swap": "Paperback Swap",
    "project_gutenberg": "Gutenberg",
    "publishamerica": "PublishAmerica",
    "rvk": "RVK",
    "scribd": "Scribd",
    "shelfari": "Shelfari",
    "siso": "SISO",
    "smashwords_book_download": "Smashwords",
    "standard_ebooks": "Standard Ebooks",
    "storygraph": "Storygraph",
    "ulrls": "ULRLS",
    "ulrls_classmark": "ULRLS Classmark",
    "w._w._norton": "W.W.Norton",
    "wikidata": "Wikidata",
    "wikisource": "Wikisource",
    "yakaboo": "Yakaboo",
    "zdb-id": "ZDB-ID",
}
# Retrieved from https://openlibrary.org/config/edition.json on 2023-07-02
ol_edition_json = orjson.loads(open(os.path.dirname(os.path.realpath(__file__)) + '/page/ol_edition.json').read())
for identifier in ol_edition_json['identifiers']:
    if 'url' in identifier:
        identifier['url'] = identifier['url'].replace('@@@', '%s')
    unified_name = identifier['name']
    if unified_name in OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING:
        unified_name = OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING[unified_name]
        if unified_name not in UNIFIED_IDENTIFIERS:
            raise Exception(f"unified_name '{unified_name}' should be in UNIFIED_IDENTIFIERS")
    else:
        OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING[unified_name] = unified_name
        if unified_name not in UNIFIED_IDENTIFIERS:
            # If unified name is not in OPENLIB_TO_UNIFIED_*_MAPPING, then it *has* to be in OPENLIB_LABELS.
            label = OPENLIB_LABELS[unified_name]
            description = ''
            if identifier.get('description', '') != label:
                description = identifier.get('description', '')
            UNIFIED_IDENTIFIERS[unified_name] = { **identifier, 'label': label, 'description': description }
for classification in ol_edition_json['classifications']:
    if 'website' in classification:
        classification['website'] = classification['website'].split(' ')[0] # Sometimes there's a suffix in text..
    unified_name = classification['name']
    if unified_name in OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING:
        unified_name = OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[unified_name]
        if unified_name not in UNIFIED_CLASSIFICATIONS:
            raise Exception(f"unified_name '{unified_name}' should be in UNIFIED_CLASSIFICATIONS")
    else:
        OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING[unified_name] = unified_name
        if unified_name not in UNIFIED_CLASSIFICATIONS:
            # If unified name is not in OPENLIB_TO_UNIFIED_*_MAPPING, then it *has* to be in OPENLIB_LABELS.
            label = OPENLIB_LABELS[unified_name]
            description = ''
            if classification.get('description', '') != label:
                description = classification.get('description', '')
            UNIFIED_CLASSIFICATIONS[unified_name] = { **classification, 'label': label, 'description': description }

def init_identifiers_and_classification_unified(output_dict):
    if 'identifiers_unified' not in output_dict:
        output_dict['identifiers_unified'] = {}
    if 'classifications_unified' not in output_dict:
        output_dict['classifications_unified'] = {}

def add_identifier_unified(output_dict, name, value):
    name = name.strip()
    value = value.strip()
    if len(value) == 0:
        return
    unified_name = OPENLIB_TO_UNIFIED_IDENTIFIERS_MAPPING.get(name, name)
    if unified_name in UNIFIED_IDENTIFIERS:
        if unified_name not in output_dict['identifiers_unified']:
            output_dict['identifiers_unified'][unified_name] = []
        output_dict['identifiers_unified'][unified_name].append(value.strip())
    else:
        raise Exception(f"Unknown identifier in add_identifier_unified: {name}")

def add_classification_unified(output_dict, name, value):
    name = name.strip()
    value = value.strip()
    if len(value) == 0:
        return
    unified_name = OPENLIB_TO_UNIFIED_CLASSIFICATIONS_MAPPING.get(name, name)
    if unified_name in UNIFIED_CLASSIFICATIONS:
        if unified_name not in output_dict['classifications_unified']:
            output_dict['classifications_unified'][unified_name] = []
        output_dict['classifications_unified'][unified_name].append(value.strip())
    else:
        raise Exception(f"Unknown classification in add_classification_unified: {name}")

def normalize_isbn(string):
    canonical_isbn13 = isbnlib.get_canonical_isbn(string, output='isbn13')
    try: 
        if (not isbnlib.is_isbn10(isbnlib.to_isbn10(canonical_isbn13))) or len(canonical_isbn13) != 13 or len(isbnlib.info(canonical_isbn13)) == 0:
            return ''
    except:
        return ''
    return canonical_isbn13

def add_isbns_unified(output_dict, potential_isbns):
    isbn10s = set()
    isbn13s = set()
    for potential_isbn in potential_isbns:
        isbn13 = normalize_isbn(potential_isbn)
        if isbn13 != '':
            isbn13s.add(isbn13)
            isbn10 = isbnlib.to_isbn10(isbn13)
            if isbnlib.is_isbn10(isbn10 or ''):
                isbn10s.add(isbn10)
    for isbn10 in isbn10s:
        add_identifier_unified(output_dict, 'isbn10', isbn10)
    for isbn13 in isbn13s:
        add_identifier_unified(output_dict, 'isbn13', isbn13)

def merge_unified_fields(list_of_fields_unified):
    merged_sets = {}
    for fields_unified in list_of_fields_unified:
        for unified_name, values in fields_unified.items():
            if unified_name not in merged_sets:
                merged_sets[unified_name] = set()
            for value in values:
                merged_sets[unified_name].add(value)
    return { unified_name: list(merged_set) for unified_name, merged_set in merged_sets.items() }

SEARCH_INDEX_SHORT_LONG_MAPPING = {
    '': 'aarecords',
    'digital_lending': 'aarecords_digital_lending',
    'meta': 'aarecords_metadata',
}
AARECORD_PREFIX_SEARCH_INDEX_MAPPING = {
    'md5': 'aarecords',
    'ia': 'aarecords_digital_lending',
    'isbn': 'aarecords_metadata',
}
