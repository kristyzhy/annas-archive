from flask import Blueprint, request
from flask_cors import cross_origin
from sqlalchemy import select, func, text, inspect

from allthethings.extensions import db
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
    db.engine.execute("SELECT 1 FROM zlib_book LIMIT 1")
    db.engines['mariapersist'].execute("SELECT 1 FROM mariapersist_downloads_total_by_md5 LIMIT 1")
    return ""
