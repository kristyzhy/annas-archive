import datetime
import time
import httpx
import shortuuid

from config import settings
from flask import Blueprint, __version__, render_template, make_response, redirect, request
from allthethings.extensions import engine, mariadb_url, es, Reflected, mail, mariapersist_engine
from sqlalchemy import select, func, text, create_engine
from sqlalchemy.dialects.mysql import match
from sqlalchemy.orm import Session
from pymysql.constants import CLIENT

import allthethings.utils

cron = Blueprint("cron", __name__, template_folder="templates")

DOWNLOAD_TESTS = [ 
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'https://momot.rs', 'path': 'zlib1/pilimi-zlib-0-119999/2094', 'filesize': 11146011 },
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'https://momot.in', 'path': 'zlib1/pilimi-zlib-0-119999/2094', 'filesize': 11146011 },
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'https://ktxr.rs', 'path': 'zlib1/pilimi-zlib-0-119999/2094', 'filesize': 11146011 },
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'https://nrzr.li', 'path': 'zlib1/pilimi-zlib-0-119999/2094', 'filesize': 11146011 },
]

#################################################################################################
# ./run flask cron infinite_loop
@cron.cli.command('infinite_loop')
def infinite_loop():
    while True:
        print(f"Infinite loop running {datetime.datetime.now().minute}")
        if datetime.datetime.now().minute % 20 == 0:
            print("Running download tests")
            for download_test in DOWNLOAD_TESTS:
                # Size: 11146011 bytes
                start = time.time()
                try:
                    if 'url' in download_test:
                        url = download_test['url']
                    else:
                        uri = allthethings.utils.make_anon_download_uri(False, 999999999, download_test['path'], 'dummy')
                        url = f"{download_test['server']}/{uri}"
                    httpx.get(url, timeout=300)
                except httpx.ConnectError as err:
                    print(f"Download error: {err}")
                    continue

                elapsed_sec = time.time() - start
                insert_data = {
                    'md5': bytes.fromhex(download_test['md5']), 
                    'server': download_test['server'], 
                    'url': url, 
                    'filesize': download_test['filesize'], 
                    'elapsed_sec': elapsed_sec, 
                    'kbps': int(download_test['filesize'] / elapsed_sec / 1000),
                }
                print("Download test result: ", insert_data)
                with Session(mariapersist_engine) as mariapersist_session:
                    mariapersist_session.execute('INSERT INTO mariapersist_download_tests (md5, server, url, filesize, elapsed_sec, kbps) VALUES (:md5, :server, :url, :filesize, :elapsed_sec, :kbps)', insert_data)
                    mariapersist_session.commit()
        time.sleep(60)
