import time
import json
import orjson
import flask_mail
import datetime
import jwt
import re
import collections
import shortuuid
import urllib.parse
import base64
import pymysql
import hashlib
import hmac
import httpx
import email
import email.policy

from flask import Blueprint, request, g, make_response, render_template, redirect
from flask_cors import cross_origin
from sqlalchemy import select, func, text, inspect
from sqlalchemy.orm import Session
from flask_babel import format_timedelta

from allthethings.extensions import es, engine, mariapersist_engine, MariapersistDownloadsTotalByMd5, mail, MariapersistDownloadsHourlyByMd5, MariapersistDownloadsHourly, MariapersistMd5Report, MariapersistAccounts, MariapersistComments, MariapersistReactions, MariapersistLists, MariapersistListEntries, MariapersistDonations, MariapersistDownloads, MariapersistFastDownloadAccess
from config.settings import SECRET_KEY, PAYMENT1_KEY, PAYMENT2_URL, PAYMENT2_API_KEY, PAYMENT2_PROXIES, PAYMENT2_HMAC, PAYMENT2_SIG_HEADER, GC_NOTIFY_SIG, HOODPAY_URL, HOODPAY_AUTH, HOODPAY_MEMBERKEY
from allthethings.page.views import get_aarecords_elasticsearch

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
    if not es.exists(index="aarecords", id=f"md5:{canonical_md5}"):
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
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
@allthethings.utils.public_cache(minutes=5, cloudflare_minutes=60)
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
    with Session(mariapersist_engine) as mariapersist_session:
        email = request.form['email']
        account = mariapersist_session.connection().execute(select(MariapersistAccounts).where(MariapersistAccounts.email_verified == email).limit(1)).first()
        if account is None:
            return "{}"

        url = g.full_domain + '/account/?key=' + allthethings.utils.secret_key_from_account_id(account.account_id)
        subject = "Secret key for Anna’s Archive"
        body = "Hi! Please use the following link to get your secret key for Anna’s Archive:\n\n" + url + "\n\nNote that we will discontinue email logins at some point, so make sure to save your secret key.\n-Anna"

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
@allthethings.utils.no_cache()
def md5_summary(md5_input):
    md5_input = md5_input[0:50]
    canonical_md5 = md5_input.strip().lower()[0:32]
    if not allthethings.utils.validate_canonical_md5s([canonical_md5]):
        raise Exception("Non-canonical md5")

    account_id = allthethings.utils.get_account_id(request.cookies)

    with Session(mariapersist_engine) as mariapersist_session:
        data_md5 = bytes.fromhex(canonical_md5)
        reports_count = mariapersist_session.connection().execute(select(func.count(MariapersistMd5Report.md5_report_id)).where(MariapersistMd5Report.md5 == data_md5).limit(1)).scalar()
        comments_count = mariapersist_session.connection().execute(select(func.count(MariapersistComments.comment_id)).where(MariapersistComments.resource == f"md5:{canonical_md5}").limit(1)).scalar()
        lists_count = mariapersist_session.connection().execute(select(func.count(MariapersistListEntries.list_entry_id)).where(MariapersistListEntries.resource == f"md5:{canonical_md5}").limit(1)).scalar()
        downloads_total = mariapersist_session.connection().execute(select(MariapersistDownloadsTotalByMd5.count).where(MariapersistDownloadsTotalByMd5.md5 == data_md5).limit(1)).scalar() or 0
        great_quality_count = mariapersist_session.connection().execute(select(func.count(MariapersistReactions.reaction_id)).where(MariapersistReactions.resource == f"md5:{canonical_md5}").limit(1)).scalar()
        user_reaction = None
        downloads_left = 0
        is_member = 0
        download_still_active = 0
        if account_id is not None:
            user_reaction = mariapersist_session.connection().execute(select(MariapersistReactions.type).where((MariapersistReactions.resource == f"md5:{canonical_md5}") & (MariapersistReactions.account_id == account_id)).limit(1)).scalar()

            account_fast_download_info = allthethings.utils.get_account_fast_download_info(mariapersist_session, account_id)
            if account_fast_download_info is not None:
                is_member = 1
                downloads_left = account_fast_download_info['downloads_left']
                if canonical_md5 in account_fast_download_info['recently_downloaded_md5s']:
                    download_still_active = 1
        return orjson.dumps({ "reports_count": reports_count, "comments_count": comments_count, "lists_count": lists_count, "downloads_total": downloads_total, "great_quality_count": great_quality_count, "user_reaction": user_reaction, "downloads_left": downloads_left, "is_member": is_member, "download_still_active": download_still_active })


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
def put_display_name():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    display_name = request.form['display_name'].strip()

    if len(display_name) < 4:
        return "", 500
    if len(display_name) > 20:
        return "", 500

    with Session(mariapersist_engine) as mariapersist_session:
        mariapersist_session.connection().execute(text('UPDATE mariapersist_accounts SET display_name = :display_name WHERE account_id = :account_id LIMIT 1').bindparams(display_name=display_name, account_id=account_id))
        mariapersist_session.commit()
        return "{}"


@dyn.put("/list/name/<string:list_id>")
@allthethings.utils.no_cache()
def put_list_name(list_id):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    name = request.form['name'].strip()
    if len(name) == 0:
        return "", 500

    with Session(mariapersist_engine) as mariapersist_session:
        # Note, this also does validation by checking for account_id.
        mariapersist_session.connection().execute(text('UPDATE mariapersist_lists SET name = :name WHERE account_id = :account_id AND list_id = :list_id LIMIT 1').bindparams(name=name, account_id=account_id, list_id=list_id))
        mariapersist_session.commit()
        return "{}"


def get_resource_type(resource):
    if bool(re.match(r"^md5:[a-f\d]{32}$", resource)):
        return 'md5'
    if bool(re.match(r"^comment:[\d]+$", resource)):
        return 'comment'
    return None


@dyn.put("/comments/<string:resource>")
@allthethings.utils.no_cache()
def put_comment(resource):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    content = request.form['content'].strip()
    if len(content) == 0:
        raise Exception("Empty content")

    with Session(mariapersist_engine) as mariapersist_session:
        resource_type = get_resource_type(resource)
        if resource_type not in ['md5', 'comment']:
            raise Exception("Invalid resource")

        if resource_type == 'comment':
            parent_resource = mariapersist_session.connection().execute(select(MariapersistComments.resource).where(MariapersistComments.comment_id == int(resource[len('comment:'):])).limit(1)).scalar()
            if parent_resource is None:
                raise Exception("No parent comment")
            parent_resource_type = get_resource_type(parent_resource)
            if parent_resource_type == 'comment':
                raise Exception("Parent comment is itself a reply")

        mariapersist_session.connection().execute(
            text('INSERT INTO mariapersist_comments (account_id, resource, content) VALUES (:account_id, :resource, :content)')
                .bindparams(account_id=account_id, resource=resource, content=content))
        mariapersist_session.commit()
        return "{}"


def get_comment_dicts(mariapersist_session, resources):
    account_id = allthethings.utils.get_account_id(request.cookies)

    comments = mariapersist_session.connection().execute(
            select(MariapersistComments, MariapersistAccounts.display_name, MariapersistReactions.type.label('user_reaction'))
            .join(MariapersistAccounts, MariapersistAccounts.account_id == MariapersistComments.account_id)
            .join(MariapersistReactions, (MariapersistReactions.resource == func.concat("comment:",MariapersistComments.comment_id)) & (MariapersistReactions.account_id == account_id), isouter=True)
            .where(MariapersistComments.resource.in_(resources))
            .limit(10000)
        ).all()
    replies = mariapersist_session.connection().execute(
            select(MariapersistComments, MariapersistAccounts.display_name, MariapersistReactions.type.label('user_reaction'))
            .join(MariapersistAccounts, MariapersistAccounts.account_id == MariapersistComments.account_id)
            .join(MariapersistReactions, (MariapersistReactions.resource == func.concat("comment:",MariapersistComments.comment_id)) & (MariapersistReactions.account_id == account_id), isouter=True)
            .where(MariapersistComments.resource.in_([f"comment:{comment.comment_id}" for comment in comments]))
            .order_by(MariapersistComments.comment_id.asc())
            .limit(10000)
        ).all()
    comment_reactions = mariapersist_session.connection().execute(
            select(MariapersistReactions.resource, MariapersistReactions.type, func.count(MariapersistReactions.account_id).label('count'))
            .where(MariapersistReactions.resource.in_([f"comment:{comment.comment_id}" for comment in (comments+replies)]))
            .group_by(MariapersistReactions.resource, MariapersistReactions.type)
            .limit(10000)
        ).all()
    comment_reactions_by_id = collections.defaultdict(dict)
    for reaction in comment_reactions:
        comment_reactions_by_id[int(reaction['resource'][len("comment:"):])][reaction['type']] = reaction['count']

    reply_dicts_by_parent_comment_id = collections.defaultdict(list)
    for reply in replies: # Note: these are already sorted chronologically.
        reply_dicts_by_parent_comment_id[int(reply.resource[len('comment:'):])].append({ 
            **reply,
            'created_delta': reply.created - datetime.datetime.now(),
            'abuse_total': comment_reactions_by_id[reply.comment_id].get(1, 0),
            'thumbs_up': comment_reactions_by_id[reply.comment_id].get(2, 0),
            'thumbs_down': comment_reactions_by_id[reply.comment_id].get(3, 0),
        })

    comment_dicts = [{ 
        **comment,
        'created_delta': comment.created - datetime.datetime.now(),
        'abuse_total': comment_reactions_by_id[comment.comment_id].get(1, 0),
        'thumbs_up': comment_reactions_by_id[comment.comment_id].get(2, 0),
        'thumbs_down': comment_reactions_by_id[comment.comment_id].get(3, 0),
        'reply_dicts': reply_dicts_by_parent_comment_id[comment.comment_id],
        'can_have_replies': True,
    } for comment in comments]



    comment_dicts.sort(reverse=True, key=lambda c: 100000*(c['thumbs_up']-c['thumbs_down']-c['abuse_total']*5) + c['comment_id'] )
    return comment_dicts


# @dyn.get("/comments/<string:resource>")
# @allthethings.utils.no_cache()
# def get_comments(resource):
#     if not bool(re.match(r"^md5:[a-f\d]{32}$", resource)):
#         raise Exception("Invalid resource")

#     with Session(mariapersist_engine) as mariapersist_session:
#         comment_dicts = get_comment_dicts(mariapersist_session, [resource])        

#         return render_template(
#             "dyn/comments.html",
#             comment_dicts=comment_dicts,
#             current_account_id=allthethings.utils.get_account_id(request.cookies),
#             reload_url=f"/dyn/comments/{resource}",
#         )


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
            'report_dict': report_dicts_by_resource.get(comment_dict['resource'], None),
        } for comment_dict in get_comment_dicts(mariapersist_session, ([f"md5:{canonical_md5}"] + list(report_dicts_by_resource.keys())))]

        return render_template(
            "dyn/comments.html",
            comment_dicts=comment_dicts,
            current_account_id=allthethings.utils.get_account_id(request.cookies),
            reload_url=f"/dyn/md5_reports/{canonical_md5}",
            md5_report_type_mapping=allthethings.utils.get_md5_report_type_mapping(),
        )


@dyn.put("/reactions/<int:reaction_type>/<string:resource>")
@allthethings.utils.no_cache()
def put_comment_reaction(reaction_type, resource):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    with Session(mariapersist_engine) as mariapersist_session:
        resource_type = get_resource_type(resource)
        if resource_type not in ['md5', 'comment']:
            raise Exception("Invalid resource")
        if resource_type == 'comment':
            if reaction_type not in [0,1,2,3]:
                raise Exception("Invalid reaction_type")
            comment_account_id = mariapersist_session.connection().execute(select(MariapersistComments.resource).where(MariapersistComments.comment_id == int(resource[len('comment:'):])).limit(1)).scalar()
            if comment_account_id is None:
                raise Exception("No parent comment")
            if comment_account_id == account_id:
                return "", 403
        elif resource_type == 'md5':
            if reaction_type not in [0,2]:
                raise Exception("Invalid reaction_type")

        if reaction_type == 0:
            mariapersist_session.connection().execute(text('DELETE FROM mariapersist_reactions WHERE account_id = :account_id AND resource = :resource').bindparams(account_id=account_id, resource=resource))
        else:
            mariapersist_session.connection().execute(text('INSERT INTO mariapersist_reactions (account_id, resource, type) VALUES (:account_id, :resource, :type) ON DUPLICATE KEY UPDATE type = :type').bindparams(account_id=account_id, resource=resource, type=reaction_type))
        mariapersist_session.commit()
        return "{}"


@dyn.put("/lists_update/<string:resource>")
@allthethings.utils.no_cache()
def lists_update(resource):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    with Session(mariapersist_engine) as mariapersist_session:
        resource_type = get_resource_type(resource)
        if resource_type not in ['md5']:
            raise Exception("Invalid resource")

        my_lists = mariapersist_session.connection().execute(
            select(MariapersistLists.list_id, MariapersistListEntries.list_entry_id)
            .join(MariapersistListEntries, (MariapersistListEntries.list_id == MariapersistLists.list_id) & (MariapersistListEntries.account_id == account_id) & (MariapersistListEntries.resource == resource), isouter=True)
            .where(MariapersistLists.account_id == account_id)
            .order_by(MariapersistLists.updated.desc())
            .limit(10000)
        ).all()

        selected_list_ids = set([list_id for list_id in request.form.keys() if list_id != 'list_new_name' and request.form[list_id] == 'on'])
        list_ids_to_add = []
        list_ids_to_remove = []
        for list_record in my_lists:
            if list_record.list_entry_id is None and list_record.list_id in selected_list_ids:
                list_ids_to_add.append(list_record.list_id)
            elif list_record.list_entry_id is not None and list_record.list_id not in selected_list_ids:
                list_ids_to_remove.append(list_record.list_id)
        list_new_name = request.form['list_new_name'].strip()

        if len(list_new_name) > 0:
            for _ in range(5):
                insert_data = { 'list_id': shortuuid.random(length=7), 'account_id': account_id, 'name': list_new_name }
                try:
                    mariapersist_session.connection().execute(text('INSERT INTO mariapersist_lists (list_id, account_id, name) VALUES (:list_id, :account_id, :name)').bindparams(**insert_data))
                    list_ids_to_add.append(insert_data['list_id'])
                    break
                except Exception as err:
                    print("List creation error", err)
                    pass

        if len(list_ids_to_add) > 0:
            mariapersist_session.execute('INSERT INTO mariapersist_list_entries (account_id, list_id, resource) VALUES (:account_id, :list_id, :resource)',
                [{ 'account_id': account_id, 'list_id': list_id, 'resource': resource } for list_id in list_ids_to_add])
        if len(list_ids_to_remove) > 0:
            mariapersist_session.execute('DELETE FROM mariapersist_list_entries WHERE account_id = :account_id AND resource = :resource AND list_id = :list_id',
                [{ 'account_id': account_id, 'list_id': list_id, 'resource': resource } for list_id in list_ids_to_remove])
        mariapersist_session.commit()

        return '{}'


@dyn.get("/lists/<string:resource>")
@allthethings.utils.no_cache()
def lists(resource):
    with Session(mariapersist_engine) as mariapersist_session:
        resource_lists = mariapersist_session.connection().execute(
            select(MariapersistLists.list_id, MariapersistLists.name, MariapersistAccounts.display_name, MariapersistAccounts.account_id)
            .join(MariapersistListEntries, MariapersistListEntries.list_id == MariapersistLists.list_id)
            .join(MariapersistAccounts, MariapersistLists.account_id == MariapersistAccounts.account_id)
            .where(MariapersistListEntries.resource == resource)
            .order_by(MariapersistLists.updated.desc())
            .limit(10000)
        ).all()

        my_lists = []
        account_id = allthethings.utils.get_account_id(request.cookies)
        if account_id is not None:
            my_lists = mariapersist_session.connection().execute(
                select(MariapersistLists.list_id, MariapersistLists.name, MariapersistListEntries.list_entry_id)
                .join(MariapersistListEntries, (MariapersistListEntries.list_id == MariapersistLists.list_id) & (MariapersistListEntries.account_id == account_id) & (MariapersistListEntries.resource == resource), isouter=True)
                .where(MariapersistLists.account_id == account_id)
                .order_by(MariapersistLists.updated.desc())
                .limit(10000)
            ).all()

        return render_template(
            "dyn/lists.html",
            resource_list_dicts=[dict(list_record) for list_record in resource_lists],
            my_list_dicts=[{ "list_id": list_record['list_id'], "name": list_record['name'], "selected": list_record['list_entry_id'] is not None } for list_record in my_lists],
            reload_url=f"/dyn/lists/{resource}",
            resource=resource,
        )

        
@dyn.put("/account/buy_membership/")
@allthethings.utils.no_cache()
def account_buy_membership():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    tier = request.form['tier']
    method = request.form['method']
    duration = request.form['duration']
    # This also makes sure that the values above are valid.
    membership_costs = allthethings.utils.membership_costs_data('en')[f"{tier},{method},{duration}"]

    cost_cents_usd_verification = request.form['costCentsUsdVerification']
    if str(membership_costs['cost_cents_usd']) != cost_cents_usd_verification:
        raise Exception(f"Invalid costCentsUsdVerification")

    donation_type = 0 # manual
    if method in ['payment1', 'payment2', 'payment2paypal', 'payment2cc', 'amazon']:
        donation_type = 1

    donation_id = shortuuid.uuid()
    donation_json = {
        'tier': tier,
        'method': method,
        'duration': duration,
        'monthly_cents': membership_costs['monthly_cents'],
        'discounts': membership_costs['discounts'],
    }

    if method == 'hoodpay':
        payload = {
            "metadata": { "memberkey": HOODPAY_MEMBERKEY },
            "name":"Anna",
            "currency":"USD",
            "amount": round(float(membership_costs['cost_cents_usd']) / 100.0, 2),
            "redirectUrl":"annas-archive.org/account",
        }
        response = httpx.post(HOODPAY_URL, json=payload, headers={"Authorization": f"Bearer {HOODPAY_AUTH}"}, proxies=PAYMENT2_PROXIES)
        response.raise_for_status()
        donation_json['hoodpay_request'] = response.json()

    if method in ['payment2', 'payment2paypal', 'payment2cc']:
        if method == 'payment2':
            pay_currency = request.form['pay_currency']
        elif method == 'payment2paypal':
            pay_currency = 'pyusd'
        elif method == 'payment2cc':
            pay_currency = 'btc'
        if pay_currency not in ['btc','eth','bch','ltc','xmr','ada','bnbbsc','busdbsc','dai','doge','dot','matic','near','pax','pyusd','sol','ton','trx','tusd','usdc','usdterc20','usdttrc20','xrp']:
            raise Exception(f"Invalid pay_currency: {pay_currency}")

        price_currency = 'usd'
        if pay_currency in ['busdbsc','dai','pyusd','tusd','usdc','usdterc20','usdttrc20']:
            price_currency = pay_currency

        donation_json['payment2_request'] = httpx.post(PAYMENT2_URL, headers={'x-api-key': PAYMENT2_API_KEY}, proxies=PAYMENT2_PROXIES, json={
            "price_amount": round(float(membership_costs['cost_cents_usd']) * (1.03 if price_currency == 'usd' else 1.0) / 100.0, 2),
            "price_currency": price_currency,
            "pay_currency": pay_currency,
            "order_id": donation_id,
        }).json()

        if 'code' in donation_json['payment2_request']:
            if donation_json['payment2_request']['code'] == 'AMOUNT_MINIMAL_ERROR':
                return orjson.dumps({ 'error': 'This coin has a higher than usual minimum. Please select a different duration or a different coin.' })
            else:
                print(f"Warning: unknown error in payment2: {donation_json['payment2_request']}")
                return orjson.dumps({ 'error': 'An unknown error occurred. Please contact us at AnnaArchivist@proton.me with a screenshot.' })

    with Session(mariapersist_engine) as mariapersist_session:
        # existing_unpaid_donations_counts = mariapersist_session.connection().execute(select(func.count(MariapersistDonations.donation_id)).where((MariapersistDonations.account_id == account_id) & ((MariapersistDonations.processing_status == 0) | (MariapersistDonations.processing_status == 4))).limit(1)).scalar()
        # if existing_unpaid_donations_counts > 0:
        #     raise Exception(f"Existing unpaid or manualconfirm donations open")

        data_ip = allthethings.utils.canonical_ip_bytes(request.remote_addr)
        data = {
            'donation_id': donation_id,
            'account_id': account_id,
            'cost_cents_usd': membership_costs['cost_cents_usd'],
            'cost_cents_native_currency': membership_costs['cost_cents_native_currency'],
            'native_currency_code': membership_costs['native_currency_code'],
            'processing_status': 0, # unpaid
            'donation_type': donation_type,
            'ip': allthethings.utils.canonical_ip_bytes(request.remote_addr),
            'json': orjson.dumps(donation_json),
        }
        mariapersist_session.execute('INSERT INTO mariapersist_donations (donation_id, account_id, cost_cents_usd, cost_cents_native_currency, native_currency_code, processing_status, donation_type, ip, json) VALUES (:donation_id, :account_id, :cost_cents_usd, :cost_cents_native_currency, :native_currency_code, :processing_status, :donation_type, :ip, :json)', [data])
        mariapersist_session.commit()

        return orjson.dumps({ 'redirect_url': '/account/donations/' + data['donation_id'] })


@dyn.put("/account/mark_manual_donation_sent/<string:donation_id>")
@allthethings.utils.no_cache()
def account_mark_manual_donation_sent(donation_id):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    with Session(mariapersist_engine) as mariapersist_session:
        donation = mariapersist_session.connection().execute(select(MariapersistDonations).where((MariapersistDonations.account_id == account_id) & (MariapersistDonations.processing_status == 0) & (MariapersistDonations.donation_id == donation_id)).limit(1)).first()
        if donation is None:
            return "", 403

        mariapersist_session.execute('UPDATE mariapersist_donations SET processing_status = 4 WHERE donation_id = :donation_id AND processing_status = 0 AND account_id = :account_id LIMIT 1', [{ 'donation_id': donation_id, 'account_id': account_id }])
        mariapersist_session.commit()
        return "{}"


@dyn.put("/account/cancel_donation/<string:donation_id>")
@allthethings.utils.no_cache()
def account_cancel_donation(donation_id):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    with Session(mariapersist_engine) as mariapersist_session:
        donation = mariapersist_session.connection().execute(select(MariapersistDonations).where((MariapersistDonations.account_id == account_id) & ((MariapersistDonations.processing_status == 0) | (MariapersistDonations.processing_status == 4)) & (MariapersistDonations.donation_id == donation_id)).limit(1)).first()
        if donation is None:
            return "", 403

        mariapersist_session.execute('UPDATE mariapersist_donations SET processing_status = 2 WHERE donation_id = :donation_id AND (processing_status = 0 OR processing_status = 4) AND account_id = :account_id LIMIT 1', [{ 'donation_id': donation_id, 'account_id': account_id }])
        mariapersist_session.commit()
        return "{}"


@dyn.get("/recent_downloads/")
@allthethings.utils.public_cache(minutes=1, cloudflare_minutes=1)
@cross_origin()
def recent_downloads():
    with Session(engine) as session:
        with Session(mariapersist_engine) as mariapersist_session:
            downloads = mariapersist_session.connection().execute(
                select(MariapersistDownloads)
                .order_by(MariapersistDownloads.timestamp.desc())
                .limit(50)
            ).all()

            aarecords = []
            if len(downloads) > 0:
                aarecords = get_aarecords_elasticsearch(session, ['md5:' + download['md5'].hex() for download in downloads])
            seen_ids = set()
            seen_titles = set()
            output = []
            for aarecord in aarecords:
                title = aarecord['file_unified_data']['title_best']
                if aarecord['id'] not in seen_ids and title not in seen_titles:
                    output.append({ 'path': aarecord['path'], 'title': title })
                seen_ids.add(aarecord['id'])
                seen_titles.add(title)
            return orjson.dumps(output)

@dyn.post("/log_search")
@allthethings.utils.no_cache()
def log_search():
    # search_input = request.args.get("q", "").strip()
    # if len(search_input) > 0:
    #     with Session(mariapersist_engine) as mariapersist_session:
    #         mariapersist_session.connection().execute(text('INSERT INTO mariapersist_searches (search_input) VALUES (:search_input)').bindparams(search_input=search_input.encode('utf-8')))
    #         mariapersist_session.commit()
    return ""

@dyn.get("/payment1_notify/")
@allthethings.utils.no_cache()
def payment1_notify():
    data = {
        # Note that these are sorted by key.
        "money": request.args.get('money'),
        "name": request.args.get('name'),
        "out_trade_no": request.args.get('out_trade_no'),
        "pid": request.args.get('pid'),
        "trade_no": request.args.get('trade_no'),
        "trade_status": request.args.get('trade_status'),
        "type": request.args.get('type'),
    }
    sign_str = '&'.join([f'{k}={v}' for k, v in data.items()]) + PAYMENT1_KEY
    sign = hashlib.md5((sign_str).encode()).hexdigest()
    if sign != request.args.get('sign'):
        print(f"Warning: failed payment1_notify request because of incorrect signature {sign_str} /// {dict(request.args)}.")
        return "fail"
    if data['trade_status'] == 'TRADE_SUCCESS':
        with mariapersist_engine.connect() as connection:
            donation_id = data['out_trade_no']
            cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
            if allthethings.utils.confirm_membership(cursor, donation_id, 'payment1_notify', data):
                return "success"
            else:
                return "fail"
    return "success"

@dyn.post("/payment2_notify/")
@allthethings.utils.no_cache()
def payment2_notify():
    sign_str = orjson.dumps(dict(sorted(request.json.items())))
    if request.headers.get(PAYMENT2_SIG_HEADER) != hmac.new(PAYMENT2_HMAC.encode(), sign_str, hashlib.sha512).hexdigest():
        print(f"Warning: failed payment2_notify request because of incorrect signature {sign_str} /// {dict(sorted(request.json.items()))}.")
        return "Bad request", 404
    with mariapersist_engine.connect() as connection:
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        payment2_status, payment2_request_success = allthethings.utils.payment2_check(cursor, request.json['payment_id'])
        if not payment2_request_success:
            return "Error happened", 404
    return ""

@dyn.post("/gc_notify/")
@allthethings.utils.no_cache()
def gc_notify():
    request_data = request.get_data()
    message = email.message_from_bytes(request_data, policy=email.policy.default)
    
    if message['Subject'].strip().endswith('is waiting'):
        return ""

    to_split = message['X-Original-To'].replace('+', '@').split('@')
    if len(to_split) != 3:
        print(f"Warning: gc_notify message '{message['X-Original-To']}' with wrong X-Original-To: {message['X-Original-To']}")
        return "", 404
    donation_id = allthethings.utils.receipt_id_to_donation_id(to_split[1])

    with mariapersist_engine.connect() as connection:
        cursor = connection.connection.cursor(pymysql.cursors.DictCursor)
        cursor.execute('SELECT * FROM mariapersist_donations WHERE donation_id=%(donation_id)s LIMIT 1', { 'donation_id': donation_id })
        donation = cursor.fetchone()
        if donation is None:
            print(f"Warning: gc_notify message '{message['X-Original-To']}' donation_id not found {donation_id}")
            return "", 404

        donation_json = orjson.loads(donation['json'])
        donation_json['gc_notify_debug'] = (donation_json.get('gc_notify_debug') or [])

        message_body = "\n\n".join([item.get_payload(decode=True).decode() for item in message.get_payload()])

        auth_results = "\n\n".join(message.get_all('Authentication-Results'))
        if "dkim=pass" not in auth_results:
            error = f"Warning: gc_notify message '{message['X-Original-To']}' with wrong auth_results: {auth_results}"
            donation_json['gc_notify_debug'].append({ "error": error, "message_body": message_body, "email_data": request_data.decode() })
            cursor.execute('UPDATE mariapersist_donations SET json=%(json)s WHERE donation_id = %(donation_id)s LIMIT 1', { 'donation_id': donation_id, 'json': orjson.dumps(donation_json) })
            cursor.execute('COMMIT')
            print(error)
            return "", 404

        if not message['From'].strip().endswith('<gc-orders@gc.email.amazon.com>'):
            error = f"Warning: gc_notify message '{message['X-Original-To']}' with wrong From: {message['From']}"
            donation_json['gc_notify_debug'].append({ "error": error, "message_body": message_body, "email_data": request_data.decode() })
            cursor.execute('UPDATE mariapersist_donations SET json=%(json)s WHERE donation_id = %(donation_id)s LIMIT 1', { 'donation_id': donation_id, 'json': orjson.dumps(donation_json) })
            cursor.execute('COMMIT')
            print(error)
            return "", 404

        if not message['Subject'].strip().endswith('sent you an Amazon Gift Card!'):
            error = f"Warning: gc_notify message '{message['X-Original-To']}' with wrong Subject: {message['Subject']}"
            donation_json['gc_notify_debug'].append({ "error": error, "message_body": message_body, "email_data": request_data.decode() })
            cursor.execute('UPDATE mariapersist_donations SET json=%(json)s WHERE donation_id = %(donation_id)s LIMIT 1', { 'donation_id': donation_id, 'json': orjson.dumps(donation_json) })
            cursor.execute('COMMIT')
            print(error)
            return "", 404

        potential_money = re.search(r"\$([0123456789.]+) Amazon gift card", message_body)
        if potential_money is None:
            error = f"Warning: gc_notify message '{message['X-Original-To']}' with no matches for potential_money"
            donation_json['gc_notify_debug'].append({ "error": error, "message_body": message_body, "email_data": request_data.decode() })
            cursor.execute('UPDATE mariapersist_donations SET json=%(json)s WHERE donation_id = %(donation_id)s LIMIT 1', { 'donation_id': donation_id, 'json': orjson.dumps(donation_json) })
            cursor.execute('COMMIT')
            print(error)
            return "", 404

        money = float(potential_money[1])
        # Allow for 10% margin
        if money * 110 < int(donation['cost_cents_usd']):
            error = f"Warning: gc_notify message '{message['X-Original-To']}' with too small amount gift card {money*110} < {donation['cost_cents_usd']}"
            donation_json['gc_notify_debug'].append({ "error": error, "message_body": message_body, "email_data": request_data.decode() })
            cursor.execute('UPDATE mariapersist_donations SET json=%(json)s WHERE donation_id = %(donation_id)s LIMIT 1', { 'donation_id': donation_id, 'json': orjson.dumps(donation_json) })
            cursor.execute('COMMIT')
            print(error)
            return "", 404

        potential_link = re.search(r'(https://www.amazon.com/gp/r.html?[^\n)>"]+)', message_body)
        if potential_link is None:
            error = f"Warning: gc_notify message '{message['X-Original-To']}' with no matches for potential_link"
            donation_json['gc_notify_debug'].append({ "error": error, "message_body": message_body, "email_data": request_data.decode() })
            cursor.execute('UPDATE mariapersist_donations SET json=%(json)s WHERE donation_id = %(donation_id)s LIMIT 1', { 'donation_id': donation_id, 'json': orjson.dumps(donation_json) })
            cursor.execute('COMMIT')
            print(error)
            return "", 404
        link = potential_link[1]

        potential_claim_code = re.search(r'Claim Code:[ ]+([^> \n]+)[<\n]', message_body)
        claim_code = None
        if potential_claim_code is not None:
            claim_code = potential_claim_code[1]

        sig = request.headers['X-GC-NOTIFY-SIG']
        if sig != GC_NOTIFY_SIG:
            error = f"Warning: gc_notify message '{message['X-Original-To']}' has incorrect signature: '{sig}'"
            donation_json['gc_notify_debug'].append({ "error": error, "message_body": message_body, "email_data": request_data.decode() })
            cursor.execute('UPDATE mariapersist_donations SET json=%(json)s WHERE donation_id = %(donation_id)s LIMIT 1', { 'donation_id': donation_id, 'json': orjson.dumps(donation_json) })
            cursor.execute('COMMIT')
            print(error)
            return "", 404

        data_value = { "link": link, "claim_code": claim_code }
        if not allthethings.utils.confirm_membership(cursor, donation_id, 'amazon_gc_done', data_value):
            error = f"Warning: gc_notify message '{message['X-Original-To']}' confirm_membership failed"
            donation_json['gc_notify_debug'].append({ "error": error, "message_body": message_body, "email_data": request_data.decode() })
            cursor.execute('UPDATE mariapersist_donations SET json=%(json)s WHERE donation_id = %(donation_id)s LIMIT 1', { 'donation_id': donation_id, 'json': orjson.dumps(donation_json) })
            cursor.execute('COMMIT')
            print(error)
            return "", 404
    return ""

















