import os
import datetime


SECRET_KEY = os.getenv("SECRET_KEY", None)
DOWNLOADS_SECRET_KEY = os.getenv("DOWNLOADS_SECRET_KEY", None)
MEMBERS_TELEGRAM_URL = os.getenv("MEMBERS_TELEGRAM_URL", None)
PAYMENT1_ID = os.getenv("PAYMENT1_ID", None)
PAYMENT1_KEY = os.getenv("PAYMENT1_KEY", None)
BIP39_MNEMONIC = os.getenv("BIP39_MNEMONIC", None)

# Redis.
# REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Celery.
# CELERY_CONFIG = {
#     "broker_url": REDIS_URL,
#     "result_backend": REDIS_URL,
#     "include": [],
# }

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")

MAIL_USERNAME = 'anna@annas-mail.org'
MAIL_DEFAULT_SENDER = ('Anna’s Archive', 'anna@annas-mail.org')
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD", "")
if len(MAIL_PASSWORD) == 0:
    MAIL_SERVER = 'mailpit'
    MAIL_PORT = 1025
    MAIL_DEBUG = True
else:
    MAIL_SERVER = 'mail.annas-mail.org'
    MAIL_PORT = 587
    MAIL_USE_TLS = True

SLOW_DATA_IMPORTS = os.getenv("SLOW_DATA_IMPORTS", "")

FLASK_DEBUG = str(os.getenv("FLASK_DEBUG", "")).lower() in ["1","true"]
