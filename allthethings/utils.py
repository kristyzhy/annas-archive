import jwt
import re

from config.settings import SECRET_KEY

def validate_canonical_md5s(canonical_md5s):
    return all([bool(re.match(r"^[a-f\d]{32}$", canonical_md5)) for canonical_md5 in canonical_md5s])

JWT_PREFIX = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.'

ACCOUNT_COOKIE_NAME = "aa_account_id"

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
