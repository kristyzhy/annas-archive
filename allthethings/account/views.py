import time
import ipaddress
import json
import flask_mail
import datetime
import jwt

from flask import Blueprint, request, g, render_template, session, make_response, redirect
from flask_cors import cross_origin
from sqlalchemy import select, func, text, inspect
from sqlalchemy.orm import Session

from allthethings.extensions import es, engine, mariapersist_engine, MariapersistDownloadsTotalByMd5, mail
from config.settings import SECRET_KEY

import allthethings.utils


account = Blueprint("account", __name__, template_folder="templates", url_prefix="/account")


@account.get("/")
def account_index_page():
    email = None
    if len(request.cookies.get(allthethings.utils.ACCOUNT_COOKIE_NAME, "")) > 0:
        account_data = jwt.decode(
            jwt=allthethings.utils.JWT_PREFIX + request.cookies[allthethings.utils.ACCOUNT_COOKIE_NAME],
            key=SECRET_KEY,
            algorithms=["HS256"],
            options={ "verify_signature": True, "require": ["iat"], "verify_iat": True }
        )
        email = account_data["m"]

    return render_template("index.html", header_active="", email=email)


@account.get("/access/<string:partial_jwt_token>")
def account_access_page(partial_jwt_token):
    token_data = jwt.decode(
        jwt=allthethings.utils.JWT_PREFIX + partial_jwt_token,
        key=SECRET_KEY,
        algorithms=["HS256"],
        options={ "verify_signature": True, "require": ["exp"], "verify_exp": True }
    )

    email = token_data["m"]
    account_token = jwt.encode(
        payload={ "m": email, "iat": datetime.datetime.now(tz=datetime.timezone.utc) },
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
