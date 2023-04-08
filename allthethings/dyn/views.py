import time
import json
import orjson
import flask_mail
import datetime
import jwt

from flask import Blueprint, request, g, make_response
from flask_cors import cross_origin
from sqlalchemy import select, func, text, inspect
from sqlalchemy.orm import Session

from allthethings.extensions import es, engine, mariapersist_engine, MariapersistDownloadsTotalByMd5, mail, MariapersistDownloadsHourlyByMd5, MariapersistDownloadsHourly
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

    with Session(mariapersist_engine) as mariapersist_session:
        data_hour_since_epoch = int(time.time() / 3600)
        data_md5 = bytes.fromhex(canonical_md5)
        data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
        account_id = allthethings.utils.get_account_id(request.cookies)
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_downloads_hourly_by_ip (ip, hour_since_epoch, count) VALUES (:ip, :hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(hour_since_epoch=data_hour_since_epoch, ip=data_ip))
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_downloads_hourly_by_md5 (md5, hour_since_epoch, count) VALUES (:md5, :hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(hour_since_epoch=data_hour_since_epoch, md5=data_md5))
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_downloads_total_by_md5 (md5, count) VALUES (:md5, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(md5=data_md5))
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_downloads_hourly (hour_since_epoch, count) VALUES (:hour_since_epoch, 1) ON DUPLICATE KEY UPDATE count = count + 1').bindparams(hour_since_epoch=data_hour_since_epoch))
        mariapersist_session.connection().execute(text('INSERT IGNORE INTO mariapersist_downloads (md5, ip, account_id) VALUES (:md5, :ip, :account_id)').bindparams(md5=data_md5, ip=data_ip, account_id=account_id))
        mariapersist_session.commit()
        return ""

# TODO: hourly caching
@dyn.get("/downloads/stats/")
def downloads_stats_total():
    with mariapersist_engine.connect() as mariapersist_conn:
        hour_now = int(time.time() / 3600)
        hour_week_ago = hour_now - 24*31
        timeseries = mariapersist_conn.execute(select(MariapersistDownloadsHourly.hour_since_epoch, MariapersistDownloadsHourly.count).where(MariapersistDownloadsHourly.hour_since_epoch >= hour_week_ago).limit(hour_week_ago+1)).all()
        timeseries_by_hour = {}
        for t in timeseries:
            timeseries_by_hour[t.hour_since_epoch] = t.count
        timeseries_x = list(range(hour_week_ago, hour_now+1))
        timeseries_y = [timeseries_by_hour.get(x, 0) for x in timeseries_x]
        return orjson.dumps({ "timeseries_x": timeseries_x, "timeseries_y": timeseries_y })

# TODO: hourly caching
@dyn.get("/downloads/stats/<string:md5_input>")
def downloads_stats_md5(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        raise Exception("Non-canonical md5")

    with mariapersist_engine.connect() as mariapersist_conn:
        total = mariapersist_conn.execute(select(MariapersistDownloadsTotalByMd5.count).where(MariapersistDownloadsTotalByMd5.md5 == bytes.fromhex(canonical_md5)).limit(1)).scalars().first() or 0
        hour_now = int(time.time() / 3600)
        hour_week_ago = hour_now - 24*31
        timeseries = mariapersist_conn.execute(select(MariapersistDownloadsHourlyByMd5.hour_since_epoch, MariapersistDownloadsHourlyByMd5.count).where((MariapersistDownloadsHourlyByMd5.md5 == bytes.fromhex(canonical_md5)) & (MariapersistDownloadsHourlyByMd5.hour_since_epoch >= hour_week_ago)).limit(hour_week_ago+1)).all()
        timeseries_by_hour = {}
        for t in timeseries:
            timeseries_by_hour[t.hour_since_epoch] = t.count
        timeseries_x = list(range(hour_week_ago, hour_now+1))
        timeseries_y = [timeseries_by_hour.get(x, 0) for x in timeseries_x]
        return orjson.dumps({ "total": int(total), "timeseries_x": timeseries_x, "timeseries_y": timeseries_y })


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

@dyn.put("/copyright/")
def copyright():
    with Session(mariapersist_engine) as mariapersist_session:
        data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
        data_json = orjson.dumps(request.form)
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_copyright_claims (ip, json) VALUES (:ip, :json)').bindparams(ip=data_ip, json=data_json))
        mariapersist_session.commit()
        return "{}"

@dyn.put("/md5_report/<string:md5_input>")
def md5_report(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]
    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        raise Exception("Non-canonical md5")

    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    report_type = request.form['type']
    if report_type not in ["download", "broken", "pages", "spam", "other"]:
        raise Exception("Incorrect report_type")

    description = request.form['description']
    if len(description) == 0:
        raise Exception("Empty description")

    better_md5 = request.form['better_md5'][0:50]
    canonical_better_md5 = better_md5.strip().lower()
    if (len(canonical_better_md5) == 0) or (canonical_better_md5 == canonical_md5):
        canonical_better_md5 = None
    elif not allthethings.utils.validate_canonical_md5s([canonical_better_md5]):
        raise Exception("Non-canonical better_md5")

    with Session(mariapersist_engine) as mariapersist_session:
        data_md5 = bytes.fromhex(canonical_md5)
        data_better_md5 = None
        if canonical_better_md5 is not None:
            data_better_md5 = bytes.fromhex(canonical_better_md5)
        data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_md5_report (md5, account_id, ip, type, description, better_md5) VALUES (:md5, :account_id, :ip, :type, :description, :better_md5)').bindparams(md5=data_md5, account_id=account_id, ip=data_ip, type=report_type, description=description, better_md5=data_better_md5))
        mariapersist_session.commit()
        return "{}"
