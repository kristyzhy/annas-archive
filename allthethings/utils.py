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
from flask_babel import gettext, get_babel, force_locale

from config.settings import SECRET_KEY, DOWNLOADS_SECRET_KEY

FEATURE_FLAGS = {}

def validate_canonical_md5s(canonical_md5s):
    return all([bool(re.match(r"^[a-f\d]{32}$", canonical_md5)) for canonical_md5 in canonical_md5s])

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
    else:
        return str(locale)

def domain_lang_code_to_full_lang_code(domain_lang_code):
    if domain_lang_code == "tw":
        return 'zh_Hant'
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
    "paypal": 20,
    # "bmc":    0,
    "alipay": 0,
    "pix":    0,
}
MEMBERSHIP_DURATION_DISCOUNTS = {
    # Note: keep manually in sync with HTML.
    "1": 0, "3": 5, "6": 10, "12": 15,
}

def cents_to_usd_str(cents):
    return str(cents)[:-2] + "." + str(cents)[-2:]

def membership_format_native_currency(locale, native_currency_code, cost_cents_native_currency, cost_cents_usd):
    if native_currency_code != 'USD':
        return {
            'cost_cents_native_currency_str_calculator': f"{babel.numbers.format_currency(cost_cents_native_currency / 100, native_currency_code, locale=locale)} ({babel.numbers.format_currency(cost_cents_usd / 100, 'USD', locale=locale)}) total",
            'cost_cents_native_currency_str_button': f"{babel.numbers.format_currency(cost_cents_native_currency / 100, native_currency_code, locale=locale)}",
            'cost_cents_native_currency_str_donation_page_formal': f"{babel.numbers.format_currency(cost_cents_native_currency / 100, native_currency_code, locale=locale)} ({babel.numbers.format_currency(cost_cents_usd / 100, 'USD', locale=locale)})",
            'cost_cents_native_currency_str_donation_page_instructions': f"{babel.numbers.format_currency(cost_cents_native_currency / 100, native_currency_code, locale=locale)} ({babel.numbers.format_currency(cost_cents_usd / 100, 'USD', locale=locale)})",
        }
    # elif native_currency_code == 'COFFEE':
    #     return {
    #         'cost_cents_native_currency_str_calculator': f"{babel.numbers.format_currency(cost_cents_native_currency * 5, 'USD', locale=locale)} ({cost_cents_native_currency} ☕️) total",
    #         'cost_cents_native_currency_str_button': f"{babel.numbers.format_currency(cost_cents_native_currency * 5, 'USD', locale=locale)}",
    #         'cost_cents_native_currency_str_donation_page_formal': f"{babel.numbers.format_currency(cost_cents_native_currency * 5, 'USD', locale=locale)} ({cost_cents_native_currency} ☕️)",
    #         'cost_cents_native_currency_str_donation_page_instructions': f"{cost_cents_native_currency} “coffee” ({babel.numbers.format_currency(cost_cents_native_currency * 5, 'USD', locale=locale)})",
    #     }
    else:
        return {
            'cost_cents_native_currency_str_calculator': f"{babel.numbers.format_currency(cost_cents_native_currency / 100, 'USD', locale=locale)} total",
            'cost_cents_native_currency_str_button': f"{babel.numbers.format_currency(cost_cents_native_currency / 100, 'USD', locale=locale)}",
            'cost_cents_native_currency_str_donation_page_formal': f"{babel.numbers.format_currency(cost_cents_native_currency / 100, 'USD', locale=locale)}",
            'cost_cents_native_currency_str_donation_page_instructions': f"{babel.numbers.format_currency(cost_cents_native_currency / 100, 'USD', locale=locale)}",
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
        if method == 'alipay':
            native_currency_code = 'CNY'
            cost_cents_native_currency = round(cost_cents_usd * usd_currency_rates['CNY'] / 100) * 100
        # elif method == 'bmc':
        #     native_currency_code = 'COFFEE'
        #     cost_cents_native_currency = round(cost_cents_usd / 500)
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

def make_anon_download_uri(limit_multiple, speed_kbps, path, filename):
    limit_multiple_field = 'y' if limit_multiple else 'x'
    expiry = int((datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(days=1)).timestamp())
    md5 = base64.urlsafe_b64encode(hashlib.md5(f"{limit_multiple_field}/{expiry}/{speed_kbps}/{urllib.parse.unquote(path)},{DOWNLOADS_SECRET_KEY}".encode('utf-8')).digest()).decode('utf-8').rstrip('=')
    return f"d1/{limit_multiple_field}/{expiry}/{speed_kbps}/{path}~/{md5}/{filename}"








