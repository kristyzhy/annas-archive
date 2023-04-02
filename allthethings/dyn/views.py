import time
import ipaddress
import json
import orjson
import flask_mail
import datetime
import jwt

from flask import Blueprint, request, g, make_response
from flask_cors import cross_origin
from sqlalchemy import select, func, text, inspect
from sqlalchemy.orm import Session

from allthethings.extensions import es, engine, mariapersist_engine, MariapersistDownloadsTotalByMd5, mail
from config.settings import SECRET_KEY

import allthethings.utils


dyn = Blueprint("dyn", __name__, template_folder="templates", url_prefix="/dyn")


@dyn.get("/up/")
@cross_origin()
def index():
    # For testing, uncomment:
    # if "testing_redirects" not in request.headers['Host']:
    #     return "Simulate server down", 513

    account_id = allthethings.utils.get_account_id(request.cookies)
    aa_logged_in = 0 if account_id is None else 1
    return orjson.dumps({ "aa_logged_in": aa_logged_in })


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
        data_hour_since_epoch = int(time.time() / 3600)
        data_md5 = bytes.fromhex(canonical_md5)
        data_ip = ipv6.packed
        session.connection().execute(text('INSERT INTO mariapersist_downloads_hourly_by_ip (ip, hour_since_epoch, count) VALUES (:ip, :hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(hour_since_epoch=data_hour_since_epoch, ip=data_ip))
        session.connection().execute(text('INSERT INTO mariapersist_downloads_hourly_by_md5 (md5, hour_since_epoch, count) VALUES (:md5, :hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(hour_since_epoch=data_hour_since_epoch, md5=data_md5))
        session.connection().execute(text('INSERT INTO mariapersist_downloads_total_by_md5 (md5, count) VALUES (:md5, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(md5=data_md5))
        session.connection().execute(text('INSERT INTO mariapersist_downloads_hourly (hour_since_epoch, count) VALUES (:hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(hour_since_epoch=data_hour_since_epoch))
        session.connection().execute(text('INSERT IGNORE INTO mariapersist_downloads (md5, ip) VALUES (:md5, :ip)').bindparams(md5=data_md5, ip=data_ip))
        session.commit()
        return ""


@dyn.get("/downloads/total/<string:md5_input>")
def downloads_total(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        raise Exception("Non-canonical md5")

    with mariapersist_engine.connect() as conn:
        record = conn.execute(select(MariapersistDownloadsTotalByMd5).where(MariapersistDownloadsTotalByMd5.md5 == bytes.fromhex(canonical_md5)).limit(1)).first()
        return orjson.dumps({ "count": record.count })


@dyn.put("/account/access/")
def account_access():
    email = request.form['email']
    jwt_payload = jwt.encode(
        payload={ "m": email, "exp": datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(hours=1) },
        key=SECRET_KEY,
        algorithm="HS256"
    )

    url = g.full_domain + '/account/access/' + allthethings.utils.strip_jwt_prefix(jwt_payload)
    subject = "Log in to Anna’s Archive"
    body = "Hi! Please use the following link to log in to Anna’s Archive:\n\n" + url + "\n\nIf you run into any issues, feel free to reply to this email.\n-Anna"

    email_msg = flask_mail.Message(subject=subject, body=body, recipients=[email])
    mail.send(email_msg)
    return "{}"

@dyn.put("/account/logout/")
def account_logout():
    request.cookies[allthethings.utils.ACCOUNT_COOKIE_NAME] # Error if cookie is not set.
    resp = make_response(orjson.dumps({ "aa_logged_in": 0 }))
    resp.set_cookie(
        key=allthethings.utils.ACCOUNT_COOKIE_NAME,
        httponly=True,
        secure=g.secure_domain,
        domain=g.base_domain,
    )
    return resp
