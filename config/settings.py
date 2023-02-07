import os


SECRET_KEY = os.getenv("SECRET_KEY", None)

# Redis.
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Celery.
CELERY_CONFIG = {
    "broker_url": REDIS_URL,
    "result_backend": REDIS_URL,
    "include": [],
}

ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
