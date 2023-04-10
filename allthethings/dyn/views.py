import time
import json
import orjson
import flask_mail
import datetime
import jwt
import re
import collections

from flask import Blueprint, request, g, make_response, render_template
from flask_cors import cross_origin
from sqlalchemy import select, func, text, inspect
from sqlalchemy.orm import Session
from flask_babel import format_timedelta

from allthethings.extensions import es, engine, mariapersist_engine, MariapersistDownloadsTotalByMd5, mail, MariapersistDownloadsHourlyByMd5, MariapersistDownloadsHourly, MariapersistMd5Report, MariapersistAccounts, MariapersistComments, MariapersistCommentReactions
from config.settings import SECRET_KEY

import allthethings.utils


dyn = Blueprint("dyn", __name__, template_folder="templates", url_prefix="/dyn")


@dyn.get("/up/")
@allthethings.utils.no_cache()
@cross_origin()
def index():
    # For testing, uncomment:
    # if "testing_redirects" not in request.headers['Host']:
    #     return "Simulate server down", 513

    account_id = allthethings.utils.get_account_id(request.cookies)
    aa_logged_in = 0 if account_id is None else 1
    return orjson.dumps({ "aa_logged_in": aa_logged_in })


@dyn.get("/up/databases/")
@allthethings.utils.no_cache()
def databases():
    # redis.ping()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1 FROM zlib_book LIMIT 1"))
    with mariapersist_engine.connect() as mariapersist_conn:
        mariapersist_conn.execute(text("SELECT 1 FROM mariapersist_downloads_total_by_md5 LIMIT 1"))
    return ""

@dyn.post("/downloads/increment/<string:md5_input>")
@allthethings.utils.no_cache()
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

@dyn.get("/downloads/stats/")
@allthethings.utils.public_cache(minutes=5, shared_minutes=60)
def downloads_stats_total():
    with mariapersist_engine.connect() as mariapersist_conn:
        hour_now = int(time.time() / 3600)
        hour_week_ago = hour_now - 24*31
        timeseries = mariapersist_conn.execute(select(MariapersistDownloadsHourly.hour_since_epoch, MariapersistDownloadsHourly.count).where(MariapersistDownloadsHourly.hour_since_epoch >= hour_week_ago).limit(hour_week_ago+1)).all()
        timeseries_by_hour = {}
        for t in timeseries:
            timeseries_by_hour[t.hour_since_epoch] = t.count
        timeseries_x = list(range(hour_week_ago, hour_now))
        timeseries_y = [timeseries_by_hour.get(x, 0) for x in timeseries_x]
        return orjson.dumps({ "timeseries_x": timeseries_x, "timeseries_y": timeseries_y })

@dyn.get("/downloads/stats/<string:md5_input>")
@allthethings.utils.public_cache(minutes=5, shared_minutes=60)
def downloads_stats_md5(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]

    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        raise Exception("Non-canonical md5")

    with mariapersist_engine.connect() as mariapersist_conn:
        total = mariapersist_conn.execute(select(MariapersistDownloadsTotalByMd5.count).where(MariapersistDownloadsTotalByMd5.md5 == bytes.fromhex(canonical_md5)).limit(1)).scalar() or 0
        hour_now = int(time.time() / 3600)
        hour_week_ago = hour_now - 24*31
        timeseries = mariapersist_conn.execute(select(MariapersistDownloadsHourlyByMd5.hour_since_epoch, MariapersistDownloadsHourlyByMd5.count).where((MariapersistDownloadsHourlyByMd5.md5 == bytes.fromhex(canonical_md5)) & (MariapersistDownloadsHourlyByMd5.hour_since_epoch >= hour_week_ago)).limit(hour_week_ago+1)).all()
        timeseries_by_hour = {}
        for t in timeseries:
            timeseries_by_hour[t.hour_since_epoch] = t.count
        timeseries_x = list(range(hour_week_ago, hour_now))
        timeseries_y = [timeseries_by_hour.get(x, 0) for x in timeseries_x]
        return orjson.dumps({ "total": int(total), "timeseries_x": timeseries_x, "timeseries_y": timeseries_y })


@dyn.put("/account/access/")
@allthethings.utils.no_cache()
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
@allthethings.utils.no_cache()
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
@allthethings.utils.no_cache()
def copyright():
    with Session(mariapersist_engine) as mariapersist_session:
        data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
        data_json = orjson.dumps(request.form)
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_copyright_claims (ip, json) VALUES (:ip, :json)').bindparams(ip=data_ip, json=data_json))
        mariapersist_session.commit()
        return "{}"

@dyn.get("/md5/summary/<string:md5_input>")
@allthethings.utils.public_cache(minutes=0, shared_minutes=1)
def md5_summary(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]
    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        raise Exception("Non-canonical md5")

    with Session(mariapersist_engine) as mariapersist_session:
        data_md5 = bytes.fromhex(canonical_md5)
        reports_count = mariapersist_session.connection().execute(select(func.count(MariapersistMd5Report.md5_report_id)).where(MariapersistMd5Report.md5 == data_md5).limit(1)).scalar()
        comments_count = mariapersist_session.connection().execute(select(func.count(MariapersistComments.comment_id)).where(MariapersistComments.resource == f"md5:{data_md5}").limit(1)).scalar()
        downloads_total = mariapersist_session.connection().execute(select(MariapersistDownloadsTotalByMd5.count).where(MariapersistDownloadsTotalByMd5.md5 == bytes.fromhex(canonical_md5)).limit(1)).scalar() or 0
        return orjson.dumps({ "reports_count": reports_count, "comments_count": comments_count, "downloads_total": downloads_total })


@dyn.put("/md5_report/<string:md5_input>")
@allthethings.utils.no_cache()
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

    content = request.form['content']
    if len(content) == 0:
        raise Exception("Empty content")

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
        md5_report_id = mariapersist_session.connection().execute(text('INSERT INTO mariapersist_md5_report (md5, account_id, type, better_md5) VALUES (:md5, :account_id, :type, :better_md5) RETURNING md5_report_id').bindparams(md5=data_md5, account_id=account_id, type=report_type, better_md5=data_better_md5)).scalar()
        mariapersist_session.connection().execute(
            text('INSERT INTO mariapersist_comments (account_id, resource, content) VALUES (:account_id, :resource, :content)')
                .bindparams(account_id=account_id, resource=f"md5_report:{md5_report_id}", content=content))
        mariapersist_session.commit()
        return "{}"

@dyn.put("/account/display_name/")
@allthethings.utils.no_cache()
def display_name():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    display_name = request.form['display_name'].strip()

    if len(display_name) < 4:
        return "", 500
    if len(display_name) > 20:
        return "", 500

    with Session(mariapersist_engine) as mariapersist_session:
        mariapersist_session.connection().execute(text('UPDATE mariapersist_accounts SET display_name = :display_name WHERE account_id = :account_id').bindparams(display_name=display_name, account_id=account_id))
        mariapersist_session.commit()
        return "{}"

@dyn.put("/comments/<string:resource>")
@allthethings.utils.no_cache()
def put_comment(resource):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    if not bool(re.match(r"^md5:[a-f\d]{32}$", resource)):
        raise Exception("resource")

    content = request.form['content'].strip()
    if len(content) == 0:
        raise Exception("Empty content")

    with Session(mariapersist_engine) as mariapersist_session:
        mariapersist_session.connection().execute(
            text('INSERT INTO mariapersist_comments (account_id, resource, content) VALUES (:account_id, :resource, :content)')
                .bindparams(account_id=account_id, resource=resource, content=content))
        mariapersist_session.commit()
        return "{}"

def get_comment_dicts(mariapersist_session, resources):
    account_id = allthethings.utils.get_account_id(request.cookies)

    comments = mariapersist_session.connection().execute(
            select(MariapersistComments, MariapersistAccounts.display_name, MariapersistCommentReactions.type.label('user_reaction'))
            .join(MariapersistAccounts, MariapersistAccounts.account_id == MariapersistComments.account_id)
            .join(MariapersistCommentReactions, (MariapersistCommentReactions.comment_id == MariapersistComments.comment_id) & (MariapersistCommentReactions.account_id == account_id), isouter=True)
            .where(MariapersistComments.resource.in_(resources))
            .order_by(MariapersistComments.created.desc())
            .limit(10000)
        ).all()
    comment_reactions = mariapersist_session.connection().execute(
            select(MariapersistCommentReactions.comment_id, MariapersistCommentReactions.type, func.count(MariapersistCommentReactions.account_id).label('count'))
            .where(MariapersistCommentReactions.comment_id.in_([comment.comment_id for comment in comments]))
            .group_by(MariapersistCommentReactions.comment_id, MariapersistCommentReactions.type)
            .limit(10000)
        ).all()
    comment_reactions_by_id = collections.defaultdict(dict)
    for reaction in comment_reactions:
        comment_reactions_by_id[reaction['comment_id']][reaction['type']] = reaction['count']

    comment_dicts = [{ 
        **comment,
        'created_delta': comment.created - datetime.datetime.now(),
        'abuse_total': comment_reactions_by_id[comment.comment_id].get(1, 0),
        'thumbs_up': comment_reactions_by_id[comment.comment_id].get(2, 0),
        'thumbs_down': comment_reactions_by_id[comment.comment_id].get(3, 0),
    } for comment in comments]

    comment_dicts.sort(reverse=True, key=lambda c: 100000*(c['thumbs_up']-c['thumbs_down']-c['abuse_total']*5) + c['comment_id'] )
    return comment_dicts


@dyn.get("/comments/<string:resource>")
@allthethings.utils.no_cache()
def get_comments(resource):
    if not bool(re.match(r"^md5:[a-f\d]{32}$", resource)):
        raise Exception("resource")

    with Session(mariapersist_engine) as mariapersist_session:
        comment_dicts = get_comment_dicts(mariapersist_session, [resource])        

        return render_template(
            "dyn/comments.html",
            comment_dicts=comment_dicts,
            current_account_id=allthethings.utils.get_account_id(request.cookies),
            reload_url=f"/dyn/comments/{resource}",
        )

@dyn.get("/md5_reports/<string:md5_input>")
@allthethings.utils.no_cache()
def md5_reports(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]
    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        raise Exception("Non-canonical md5")

    with Session(mariapersist_engine) as mariapersist_session:
        data_md5 = bytes.fromhex(canonical_md5)
        reports = mariapersist_session.connection().execute(
                select(MariapersistMd5Report.md5_report_id, MariapersistMd5Report.type, MariapersistMd5Report.better_md5)
                .where(MariapersistMd5Report.md5 == data_md5)
                .order_by(MariapersistMd5Report.created.desc())
                .limit(10000)
            ).all()
        report_dicts_by_resource = {}
        for r in reports:
            report_dicts_by_resource[f"md5_report:{r.md5_report_id}"] = dict(r)

        comment_dicts = [{ 
            **comment_dict,
            'report_dict': report_dicts_by_resource[comment_dict['resource']],
        } for comment_dict in get_comment_dicts(mariapersist_session, report_dicts_by_resource.keys())]

        return render_template(
            "dyn/comments.html",
            comment_dicts=comment_dicts,
            current_account_id=allthethings.utils.get_account_id(request.cookies),
            reload_url=f"/dyn/md5_reports/{canonical_md5}",
            md5_report_type_mapping=allthethings.utils.get_md5_report_type_mapping(),
        )

@dyn.put("/comments/reactions/<int:reaction_type>/<int:comment_id>")
@allthethings.utils.no_cache()
def put_comment_reaction(reaction_type, comment_id):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    if reaction_type not in [0,1,2,3]:
        raise Exception("Invalid type")

    with Session(mariapersist_engine) as mariapersist_session:
        comment_account_id = mariapersist_session.connection().execute(
                select(MariapersistComments.account_id)
                .where(MariapersistComments.comment_id == comment_id)
                .limit(1)
            ).scalar()
        if comment_account_id == account_id:
            return "", 403

        if reaction_type == 0:
            mariapersist_session.connection().execute(text('DELETE FROM mariapersist_comment_reactions WHERE account_id = :account_id AND comment_id = :comment_id').bindparams(account_id=account_id, comment_id=comment_id))
        else:
            mariapersist_session.connection().execute(text('INSERT INTO mariapersist_comment_reactions (account_id, comment_id, type) VALUES (:account_id, :comment_id, :type) ON DUPLICATE KEY UPDATE type = :type').bindparams(account_id=account_id, comment_id=comment_id, type=reaction_type))
        mariapersist_session.commit()
        return "{}"
