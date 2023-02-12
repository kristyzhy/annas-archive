import time
import ipaddress

from flask import Blueprint, request
from flask_cors import cross_origin
from sqlalchemy import select, func, text, inspect
from sqlalchemy.orm import Session

from allthethings.extensions import es, engine, mariapersist_engine, MariapersistDownloadsTotalByMd5
from allthethings.initializers import redis

import allthethings.utils


dyn = Blueprint("dyn", __name__, template_folder="templates", url_prefix="/dyn")


@dyn.get("/up/")
@cross_origin()
def index():
    # For testing, uncomment:
    # if "testing_redirects" not in request.headers['Host']:
    #     return "Simulate server down", 513
    return ""


@dyn.get("/up/databases/")
def databases():
    # redis.ping()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1 FROM zlib_book LIMIT 1"))
    with mariapersist_engine.connect() as mariapersist_conn:
        mariapersist_conn.execute(text("SELECT 1 FROM mariapersist_downloads_total_by_md5 LIMIT 1"))
    return ""

@dyn.post("/downloads/increment/<string:md5_input>")
def downloads_increment(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        raise Exception("Non-canonical md5")

    # Prevent hackers from filling up our database with non-existing MD5s.
    if not es.exists(index="md5_dicts", id=canonical_md5):
        raise Exception("Md5 not found")

    # Canonicalize to IPv6
    ipv6 = ipaddress.ip_address(request.remote_addr)
    if ipv6.version == 4:
        ipv6 = ipaddress.ip_address('2002::' + request.remote_addr)

    with Session(mariapersist_engine) as session:
        data = { 
            'hour_since_epoch': int(time.time() / 3600),
            'md5': bytes.fromhex(canonical_md5),
            'ip': ipv6.packed,
        }
        session.execute('INSERT INTO mariapersist_downloads_hourly_by_ip (ip, hour_since_epoch, count) VALUES (:ip, :hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1', data)
        session.execute('INSERT INTO mariapersist_downloads_hourly_by_md5 (md5, hour_since_epoch, count) VALUES (:md5, :hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1', data)
        session.execute('INSERT INTO mariapersist_downloads_total_by_md5 (md5, count) VALUES (:md5, 1) ON DUPLICATE KEY UPDATE count = count + 1', data)
        session.execute('INSERT INTO mariapersist_downloads_hourly (hour_since_epoch, count) VALUES (:hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1', data)
        session.execute('INSERT IGNORE INTO mariapersist_downloads (md5, ip) VALUES (:md5, :ip)', data)
        session.commit()
        return ""

