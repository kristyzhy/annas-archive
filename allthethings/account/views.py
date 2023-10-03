import time
import ipaddress
import json
import flask_mail
import datetime
import jwt
import shortuuid
import orjson
import babel
import hashlib
import base64
import re
import functools
import urllib
import pymysql
import httpx

from flask import Blueprint, request, g, render_template, make_response, redirect
from flask_cors import cross_origin
from sqlalchemy import select, func, text, inspect
from sqlalchemy.orm import Session
from flask_babel import gettext, ngettext, force_locale, get_locale

from allthethings.extensions import es, es_aux, engine, mariapersist_engine, MariapersistAccounts, mail, MariapersistDownloads, MariapersistLists, MariapersistListEntries, MariapersistDonations
from allthethings.page.views import get_aarecords_elasticsearch
from config.settings import SECRET_KEY, PAYMENT1_ID, PAYMENT1_KEY

import allthethings.utils


account = Blueprint("account", __name__, template_folder="templates")


@account.get("/account/")
@allthethings.utils.no_cache()
def account_index_page():
    if (request.args.get('key', '') != '') and (not bool(re.match(r"^[a-zA-Z\d]+$", request.args.get('key')))):
        return redirect(f"/account/", code=302)

    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return render_template(
            "account/index.html",
            header_active="account",
            membership_tier_names=allthethings.utils.membership_tier_names(get_locale()),
        )

    with Session(mariapersist_engine) as mariapersist_session:
        account = mariapersist_session.connection().execute(select(MariapersistAccounts).where(MariapersistAccounts.account_id == account_id).limit(1)).first()
        if account is None:
            raise Exception("Valid account_id was not found in db!")

        return render_template(
            "account/index.html",
            header_active="account",
            account_dict=dict(account),
            account_fast_download_info=allthethings.utils.get_account_fast_download_info(mariapersist_session, account_id),
            membership_tier_names=allthethings.utils.membership_tier_names(get_locale()),
        )


@account.get("/account/downloaded")
@allthethings.utils.no_cache()
def account_downloaded_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return redirect(f"/account/", code=302)

    with Session(mariapersist_engine) as mariapersist_session:
        downloads = mariapersist_session.connection().execute(select(MariapersistDownloads).where(MariapersistDownloads.account_id == account_id).order_by(MariapersistDownloads.timestamp.desc()).limit(100)).all()
        aarecords_downloaded = []
        if len(downloads) > 0:
            aarecords_downloaded = get_aarecords_elasticsearch([f"md5:{download.md5.hex()}" for download in downloads])
        return render_template("account/downloaded.html", header_active="account/downloaded", aarecords_downloaded=aarecords_downloaded)


@account.post("/account/")
@allthethings.utils.no_cache()
def account_index_post_page():
    account_id = allthethings.utils.account_id_from_secret_key(request.form['key'])
    if account_id is None:
        return render_template(
            "account/index.html",
            invalid_key=True,
            header_active="account",
            membership_tier_names=allthethings.utils.membership_tier_names(get_locale()),
        )

    with Session(mariapersist_engine) as mariapersist_session:
        account = mariapersist_session.connection().execute(select(MariapersistAccounts).where(MariapersistAccounts.account_id == account_id).limit(1)).first()
        if account is None:
            return render_template(
                "account/index.html",
                invalid_key=True,
                header_active="account",
                membership_tier_names=allthethings.utils.membership_tier_names(get_locale()),
            )

        mariapersist_session.connection().execute(text('INSERT IGNORE INTO mariapersist_account_logins (account_id, ip) VALUES (:account_id, :ip)')
            .bindparams(account_id=account_id, ip=allthethings.utils.canonical_ip_bytes(request.remote_addr)))
        mariapersist_session.commit()

        account_token = jwt.encode(
            payload={ "a": account_id, "iat": datetime.datetime.now(tz=datetime.timezone.utc) },
            key=SECRET_KEY,
            algorithm="HS256"
        )
        resp = make_response(redirect(f"/account/", code=302))
        resp.set_cookie(
            key=allthethings.utils.ACCOUNT_COOKIE_NAME,
            value=allthethings.utils.strip_jwt_prefix(account_token),
            expires=datetime.datetime(9999,1,1),
            httponly=True,
            secure=g.secure_domain,
            domain=g.base_domain,
        )
        return resp


@account.post("/account/register")
@allthethings.utils.no_cache()
def account_register_page():
    with Session(mariapersist_engine) as mariapersist_session:
        account_id = None
        for _ in range(5):
            insert_data = { 'account_id': shortuuid.random(length=7) }
            try:
                mariapersist_session.connection().execute(text('INSERT INTO mariapersist_accounts (account_id, display_name) VALUES (:account_id, :account_id)').bindparams(**insert_data))
                mariapersist_session.commit()
                account_id = insert_data['account_id']
                break
            except Exception as err:
                print("Account creation error", err)
                pass
        if account_id is None:
            raise Exception("Failed to create account after multiple attempts")

        return redirect(f"/account/?key={allthethings.utils.secret_key_from_account_id(account_id)}", code=302)


@account.get("/account/request")
@allthethings.utils.no_cache()
def request_page():
    return render_template("account/request.html", header_active="account/request")


@account.get("/account/upload")
@allthethings.utils.no_cache()
def upload_page():
    return render_template("account/upload.html", header_active="account/upload")


@account.get("/list/<string:list_id>")
@allthethings.utils.no_cache()
def list_page(list_id):
    current_account_id = allthethings.utils.get_account_id(request.cookies)

    with Session(mariapersist_engine) as mariapersist_session:
        list_record = mariapersist_session.connection().execute(select(MariapersistLists).where(MariapersistLists.list_id == list_id).limit(1)).first()
        if list_record is None:
            return "List not found", 404
        account = mariapersist_session.connection().execute(select(MariapersistAccounts).where(MariapersistAccounts.account_id == list_record.account_id).limit(1)).first()
        list_entries = mariapersist_session.connection().execute(select(MariapersistListEntries).where(MariapersistListEntries.list_id == list_id).order_by(MariapersistListEntries.updated.desc()).limit(10000)).all()

        aarecords = []
        if len(list_entries) > 0:
            aarecords = get_aarecords_elasticsearch([entry.resource for entry in list_entries if entry.resource.startswith("md5:")])

        return render_template(
            "account/list.html", 
            header_active="account",
            list_record_dict={ 
                **list_record,
                'created_delta': list_record.created - datetime.datetime.now(),
            },
            aarecords=aarecords,
            account_dict=dict(account),
            current_account_id=current_account_id,
        )


@account.get("/profile/<string:account_id>")
@allthethings.utils.no_cache()
def profile_page(account_id):
    current_account_id = allthethings.utils.get_account_id(request.cookies)

    with Session(mariapersist_engine) as mariapersist_session:
        account = mariapersist_session.connection().execute(select(MariapersistAccounts).where(MariapersistAccounts.account_id == account_id).limit(1)).first()
        lists = mariapersist_session.connection().execute(select(MariapersistLists).where(MariapersistLists.account_id == account_id).order_by(MariapersistLists.updated.desc()).limit(10000)).all()

        if account is None:
            return render_template("account/profile.html", header_active="account"), 404

        return render_template(
            "account/profile.html", 
            header_active="account/profile" if account.account_id == current_account_id else "account",
            account_dict={ 
                **account,
                'created_delta': account.created - datetime.datetime.now(),
            },
            list_dicts=list(map(dict, lists)),
            current_account_id=current_account_id,
        )


@account.get("/account/profile")
@allthethings.utils.no_cache()
def account_profile_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403
    return redirect(f"/profile/{account_id}", code=302)


@account.get("/donate")
@allthethings.utils.no_cache()
def donate_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    has_made_donations = False
    existing_unpaid_donation_id = None
    if account_id is not None:
        with Session(mariapersist_engine) as mariapersist_session:
            existing_unpaid_donation_id = mariapersist_session.connection().execute(select(MariapersistDonations.donation_id).where((MariapersistDonations.account_id == account_id) & ((MariapersistDonations.processing_status == 0) | (MariapersistDonations.processing_status == 4))).limit(1)).scalar()
            previous_donation_id = mariapersist_session.connection().execute(select(MariapersistDonations.donation_id).where((MariapersistDonations.account_id == account_id)).limit(1)).scalar()
            if (existing_unpaid_donation_id is not None) or (previous_donation_id is not None):
                has_made_donations = True

    return render_template(
        "account/donate.html", 
        header_active="donate", 
        has_made_donations=has_made_donations,
        existing_unpaid_donation_id=existing_unpaid_donation_id,
        membership_costs_data=allthethings.utils.membership_costs_data(get_locale()),
        membership_tier_names=allthethings.utils.membership_tier_names(get_locale()),
        MEMBERSHIP_TIER_COSTS=allthethings.utils.MEMBERSHIP_TIER_COSTS,
        MEMBERSHIP_METHOD_DISCOUNTS=allthethings.utils.MEMBERSHIP_METHOD_DISCOUNTS,
        MEMBERSHIP_DURATION_DISCOUNTS=allthethings.utils.MEMBERSHIP_DURATION_DISCOUNTS,
        MEMBERSHIP_DOWNLOADS_PER_DAY=allthethings.utils.MEMBERSHIP_DOWNLOADS_PER_DAY,
        MEMBERSHIP_METHOD_MINIMUM_CENTS_USD=allthethings.utils.MEMBERSHIP_METHOD_MINIMUM_CENTS_USD,
        MEMBERSHIP_METHOD_MAXIMUM_CENTS_NATIVE=allthethings.utils.MEMBERSHIP_METHOD_MAXIMUM_CENTS_NATIVE,
    )


@account.get("/donation_faq")
@allthethings.utils.no_cache()
def donation_faq_page():
    return render_template("account/donation_faq.html", header_active="donate")

@functools.cache
def get_order_processing_status_labels(locale):
    with force_locale(locale):
        return {
            0: gettext('common.donation.order_processing_status_labels.0'),
            1: gettext('common.donation.order_processing_status_labels.1'),
            2: gettext('common.donation.order_processing_status_labels.2'),
            3: gettext('common.donation.order_processing_status_labels.3'),
            4: gettext('common.donation.order_processing_status_labels.4'),
        }


def make_donation_dict(donation):
    donation_json = orjson.loads(donation['json'])
    return {
        **donation,
        'json': donation_json,
        'total_amount_usd': babel.numbers.format_currency(donation.cost_cents_usd / 100.0, 'USD', locale=get_locale()),
        'monthly_amount_usd': babel.numbers.format_currency(donation_json['monthly_cents'] / 100.0, 'USD', locale=get_locale()),
        'receipt_id': allthethings.utils.donation_id_to_receipt_id(donation.donation_id),
        'formatted_native_currency': allthethings.utils.membership_format_native_currency(get_locale(), donation.native_currency_code, donation.cost_cents_native_currency, donation.cost_cents_usd),
    }


@account.get("/account/donations/<string:donation_id>")
@allthethings.utils.no_cache()
def donation_page(donation_id):
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    donation_confirming = False
    donation_time_left = datetime.timedelta()
    donation_time_left_not_much = False
    donation_time_expired = False
    donation_pay_amount = ""

    with Session(mariapersist_engine) as mariapersist_session:
        donation = mariapersist_session.connection().execute(select(MariapersistDonations).where((MariapersistDonations.account_id == account_id) & (MariapersistDonations.donation_id == donation_id)).limit(1)).first()
        if donation is None:
            return "", 403

        donation_json = orjson.loads(donation['json'])

        if donation_json['method'] == 'payment1' and donation.processing_status == 0:
            data = {
                # Note that these are sorted by key.
                "money": str(int(float(donation.cost_cents_usd) * 7.0 / 100.0)),
                "name": "Anna’s Archive Membership",
                "notify_url": "https://annas-archive.org/dyn/payment1_notify/",
                "out_trade_no": str(donation.donation_id),
                "pid": PAYMENT1_ID,
                "return_url": "https://annas-archive.org/account/",
                "sitename": "Anna’s Archive",
                # "type": method,
            }
            sign_str = '&'.join([f'{k}={v}' for k, v in data.items()]) + PAYMENT1_KEY
            sign = hashlib.md5((sign_str).encode()).hexdigest()
            return redirect(f'https://merchant.pacypay.net/submit.php?{urllib.parse.urlencode(data)}&sign={sign}&sign_type=MD5', code=302)

        if donation_json['method'] in ['payment2', 'payment2paypal', 'payment2cashapp', 'payment2cc'] and donation.processing_status == 0:
            donation_time_left = donation.created - datetime.datetime.now() + datetime.timedelta(days=5)
            if donation_time_left < datetime.timedelta(hours=2):
                donation_time_left_not_much = True
            if donation_time_left < datetime.timedelta():
                donation_time_expired = True

            if donation_json['payment2_request']['pay_amount']*100 == int(donation_json['payment2_request']['pay_amount']*100):
                donation_pay_amount = f"{donation_json['payment2_request']['pay_amount']:.2f}"
            else:
                donation_pay_amount = f"{donation_json['payment2_request']['pay_amount']}"

            mariapersist_session.connection().connection.ping(reconnect=True)
            cursor = mariapersist_session.connection().connection.cursor(pymysql.cursors.DictCursor)
            payment2_status, payment2_request_success = allthethings.utils.payment2_check(cursor, donation_json['payment2_request']['payment_id'])
            if not payment2_request_success:
                raise Exception("Not payment2_request_success in donation_page")
            if payment2_status['payment_status'] == 'confirming':
                donation_confirming = True

        donation_dict = make_donation_dict(donation)

        donation_email = f"AnnaReceipts+{donation_dict['receipt_id']}@proton.me"
        if donation_json['method'] == 'amazon':
            donation_email = f"giftcards+{donation_dict['receipt_id']}@annas-mail.org"

        return render_template(
            "account/donation.html", 
            header_active="account/donations",
            donation_dict=donation_dict,
            order_processing_status_labels=get_order_processing_status_labels(get_locale()),
            donation_confirming=donation_confirming,
            donation_time_left=donation_time_left,
            donation_time_left_not_much=donation_time_left_not_much,
            donation_time_expired=donation_time_expired,
            donation_pay_amount=donation_pay_amount,
            donation_email=donation_email,
        )


@account.get("/account/donations/")
@allthethings.utils.no_cache()
def donations_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return "", 403

    with Session(mariapersist_engine) as mariapersist_session:
        donations = mariapersist_session.connection().execute(select(MariapersistDonations).where(MariapersistDonations.account_id == account_id).order_by(MariapersistDonations.created.desc()).limit(10000)).all()

        return render_template(
            "account/donations.html",
            header_active="account/donations",
            donation_dicts=[make_donation_dict(donation) for donation in donations],
            order_processing_status_labels=get_order_processing_status_labels(get_locale()),
        )







