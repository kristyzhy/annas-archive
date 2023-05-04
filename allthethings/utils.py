import jwt
import re
import ipaddress
import flask
import functools
import datetime

from config.settings import SECRET_KEY

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

MEMBERSHIP_TIER_NAMES = { 
    "2": "Brilliant Bookworm", 
    "3": "Lucky Librarian", 
    "4": "Dazzling Datahoarder", 
    "5": "Amazing Archivist",
}
MEMBERSHIP_TIER_COSTS = { 
    "2": 5, "3": 10, "4": 30, "5": 100,
}
MEMBERSHIP_METHOD_DISCOUNTS = {
    # Note: keep manually in sync with HTML.
    "crypto": 20,
    # "cc":     20,
    # "paypal": 20,
    "bmc":    0,
    "alipay": 0,
    "pix":    0,
}
MEMBERSHIP_DURATION_DISCOUNTS = {
    # Note: keep manually in sync with HTML.
    "1": 0, "3": 5, "6": 10, "12": 15,
}

def cents_to_usd_str(cents):
    return str(cents)[:-2] + "." + str(cents)[-2:]


@functools.cache
def membership_costs_data():
    def calculate_membership_costs(inputs):
        tier = inputs['tier']
        method = inputs['method']
        duration = inputs['duration']
        if (tier not in MEMBERSHIP_TIER_COSTS.keys()) or (method not in MEMBERSHIP_METHOD_DISCOUNTS.keys()) or (duration not in MEMBERSHIP_DURATION_DISCOUNTS.keys()):
            raise Exception("Invalid fields")

        discounts = MEMBERSHIP_METHOD_DISCOUNTS[method] + MEMBERSHIP_DURATION_DISCOUNTS[duration]
        monthly_cents = round(MEMBERSHIP_TIER_COSTS[tier]*(100-discounts));
        cost_cents_usd = monthly_cents * int(duration);

        return { 
            'cost_cents_usd': cost_cents_usd, 
            'cost_cents_usd_str': cents_to_usd_str(cost_cents_usd), 
            'cost_cents_native_currency': cost_cents_usd, 
            'cost_cents_native_currency_str': cents_to_usd_str(cost_cents_usd), 
            'native_currency_code': 'USD',
            'monthly_cents': monthly_cents,
            'monthly_cents_str': cents_to_usd_str(monthly_cents),
            'discounts': discounts,
            'duration': duration,
            'tier_name': MEMBERSHIP_TIER_NAMES[tier],
        }
        
    membership_costs_data = {}
    for tier in MEMBERSHIP_TIER_COSTS.keys():
        for method in MEMBERSHIP_METHOD_DISCOUNTS.keys():
            for duration in MEMBERSHIP_DURATION_DISCOUNTS.keys():
                inputs = { 'tier': tier, 'method': method, 'duration': duration }
                membership_costs_data[f"{tier},{method},{duration}"] = calculate_membership_costs(inputs)
    return membership_costs_data









