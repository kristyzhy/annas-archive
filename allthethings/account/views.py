import time
import ipaddress
import json
import flask_mail
import datetime
import jwt
import shortuuid

from flask import Blueprint, request, g, render_template, make_response, redirect
from flask_cors import cross_origin
from sqlalchemy import select, func, text, inspect
from sqlalchemy.orm import Session

from allthethings.extensions import es, engine, mariapersist_engine, MariapersistAccounts, mail, MariapersistDownloads
from allthethings.page.views import get_md5_dicts_elasticsearch
from config.settings import SECRET_KEY

import allthethings.utils


account = Blueprint("account", __name__, template_folder="templates", url_prefix="/account")


@account.get("/")
def account_index_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return render_template("account/index.html", header_active="account", email=None)

    with Session(mariapersist_engine) as mariapersist_session:
        account = mariapersist_session.connection().execute(select(MariapersistAccounts).where(MariapersistAccounts.account_id == account_id).limit(1)).first()
        return render_template("account/index.html", header_active="account", email=account.email_verified)

@account.get("/downloaded")
def account_downloaded_page():
    account_id = allthethings.utils.get_account_id(request.cookies)
    if account_id is None:
        return redirect(f"/account/", code=302)

    with Session(mariapersist_engine) as mariapersist_session:
        downloads = mariapersist_session.connection().execute(select(MariapersistDownloads).where(MariapersistDownloads.account_id == account_id).order_by(MariapersistDownloads.timestamp.desc()).limit(100)).all()
        md5_dicts_downloaded = []
        if len(downloads) > 0:
            md5_dicts_downloaded = get_md5_dicts_elasticsearch(mariapersist_session, [download.md5.hex() for download in downloads])
        return render_template("account/downloaded.html", header_active="account/downloaded", md5_dicts_downloaded=md5_dicts_downloaded)

@account.get("/access/<string:partial_jwt_token>")
def account_access_page(partial_jwt_token):
    try:
        token_data = jwt.decode(
            jwt=allthethings.utils.JWT_PREFIX + partial_jwt_token,
            key=SECRET_KEY,
            algorithms=["HS256"],
            options={ "verify_signature": True, "require": ["exp"], "verify_exp": True }
        )
    except jwt.exceptions.ExpiredSignatureError:
        return render_template("account/expired.html", header_active="account")

    normalized_email = token_data["m"].lower()

    with Session(mariapersist_engine) as mariapersist_session:
        account = mariapersist_session.connection().execute(select(MariapersistAccounts).where(MariapersistAccounts.email_verified == normalized_email).limit(1)).first()

        account_id = None
        if account is not None:
            account_id = account.account_id
        else:
            for _ in range(5):
                insert_data = { 'account_id': shortuuid.random(length=7), 'email_verified': normalized_email }
                try:
                    mariapersist_session.connection().execute(text('INSERT INTO mariapersist_accounts (account_id, email_verified, display_name) VALUES (:account_id, :email_verified, :account_id)').bindparams(**insert_data))
                    mariapersist_session.commit()
                    account_id = insert_data['account_id']
                    break
                except Exception as err:
                    print("Account creation error", err)
                    pass
            if account_id is None:
                raise Exception("Failed to create account after multiple attempts")
        mariapersist_session.connection().execute(text('INSERT INTO mariapersist_account_logins (account_id, ip) VALUES (:account_id, :ip)')
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

@account.get("/request")
def request_page():
    return render_template("account/request.html", header_active="account/request")

@account.get("/upload")
def upload_page():
    return render_template("account/upload.html", header_active="account/upload")

