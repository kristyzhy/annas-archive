import datetime
import time
import httpx

from config import settings
from flask import Blueprint, __version__, render_template, make_response, redirect, request
from allthethings.extensions import engine, mariadb_url, es, Reflected, mail, mariapersist_engine
from sqlalchemy import select, func, text, create_engine
from sqlalchemy.dialects.mysql import match
from sqlalchemy.orm import Session
from pymysql.constants import CLIENT

cron = Blueprint("cron", __name__, template_folder="templates")

DOWNLOAD_TESTS = [ 
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'https://nrzr.li', 'url': 'https://nrzr.li/zlib1/pilimi-zlib-0-119999/2094.fb2.zip', 'filesize': 11146011 },
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'https://ktxr.rs', 'url': 'https://ktxr.rs/zlib1/pilimi-zlib-0-119999/2094.fb2.zip', 'filesize': 11146011 },
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'https://momot.rs', 'url': 'https://momot.rs/zlib1/pilimi-zlib-0-119999/2094.fb2.zip', 'filesize': 11146011 },
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'https://momot.li', 'url': 'https://momot.li/zlib1/pilimi-zlib-0-119999/2094.fb2.zip', 'filesize': 11146011 },
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'https://momot.in', 'url': 'https://momot.in/zlib1/pilimi-zlib-0-119999/2094.fb2.zip', 'filesize': 11146011 },
    # https://nrzr.li raw ip
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'http://193.218.118.54', 'url': 'http://193.218.118.54/zlib1/pilimi-zlib-0-119999/2094.fb2.zip', 'filesize': 11146011 },
    # https://ktxr.rs raw ip
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'http://193.218.118.109', 'url': 'http://193.218.118.109/zlib1/pilimi-zlib-0-119999/2094.fb2.zip', 'filesize': 11146011 },
    # https://momot.rs raw ip
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'http://95.214.235.224', 'url': 'http://95.214.235.224/zlib1/pilimi-zlib-0-119999/2094.fb2.zip', 'filesize': 11146011 },
    # https://momot.li raw ip
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'http://62.182.86.182', 'url': 'http://62.182.86.182/zlib1/pilimi-zlib-0-119999/2094.fb2.zip', 'filesize': 11146011 },
    # https://momot.in raw ip
    { 'md5': '07989749da490e5af48938e9aeab27b2', 'server': 'http://89.248.162.228', 'url': 'http://89.248.162.228/zlib1/pilimi-zlib-0-119999/2094.fb2.zip', 'filesize': 11146011 },
]

#################################################################################################
# ./run flask cron infinite_loop
@cron.cli.command('infinite_loop')
def infinite_loop():
    while True:
        time.sleep(10)
        print("Infinite loop running")

        if datetime.datetime.now().minute % 20 == 0:
            print("Running download tests")
            for download_test in DOWNLOAD_TESTS:
                # Size: 11146011 bytes
                start = time.time()
                try:
                    httpx.get(download_test['url'], timeout=300)
                except httpx.ConnectError:
                    continue

                elapsed_sec = time.time() - start
                insert_data = {
                    'md5': bytes.fromhex(download_test['md5']), 
                    'server': download_test['server'], 
                    'url': download_test['url'], 
                    'filesize': download_test['filesize'], 
                    'elapsed_sec': elapsed_sec, 
                    'kbps': int(download_test['filesize'] / elapsed_sec / 1000),
                }
                print("Download test result: ", insert_data)
                with Session(mariapersist_engine) as mariapersist_session:
                    mariapersist_session.execute('INSERT INTO mariapersist_download_tests (md5, server, url, filesize, elapsed_sec, kbps) VALUES (:md5, :server, :url, :filesize, :elapsed_sec, :kbps)', insert_data)
                    mariapersist_session.commit()
            time.sleep(60)
