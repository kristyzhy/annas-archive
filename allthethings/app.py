import hashlib
import os
import functools
import base64
import sys
import time
import babel.numbers as babel_numbers
import multiprocessing

from celery import Celery
from flask import Flask, request, g
from werkzeug.security import safe_join
from werkzeug.debug import DebuggedApplication
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_babel import get_locale, get_translations, force_locale, gettext
from sqlalchemy import select
from sqlalchemy.orm import Session

from allthethings.account.views import account
from allthethings.blog.views import blog
from allthethings.page.views import page, all_search_aggs
from allthethings.dyn.views import dyn
from allthethings.cli.views import cli
from allthethings.extensions import engine, mariapersist_engine, babel, debug_toolbar, flask_static_digest, Base, Reflected, ReflectedMariapersist, mail, LibgenrsUpdated, LibgenliFiles
from config.settings import SECRET_KEY, DOWNLOADS_SECRET_KEY, X_AA_SECRET

import allthethings.utils

multiprocessing.set_start_method('spawn', force=True)

# Rewrite `annas-blog.org` to `/blog` as a workaround for Flask not nicely supporting multiple domains.
# Also strip `/blog` if we encounter it directly, to avoid duplicating it.
class BlogMiddleware(object):
    def __init__(self, app):
        self.app = app
    def __call__(self, environ, start_response):
        # Not just .startswith('annas-blog.org') bc then you get potential domains like www.annas-blog.org/md5/021bf980b32f1ec86758e06bf40a2b4c
        if 'annas-blog.org' in environ['HTTP_HOST']: # so we can test using http://annas-blog.org.localtest.me:8000/
            environ['PATH_INFO'] = '/blog' + environ['PATH_INFO']
        elif environ['PATH_INFO'].startswith('/blog'): # Don't allow the /blog path directly to avoid duplication between annas-blog.org and /blog
            # Note that this HAS to be in an `elif`, because some blog paths actually start with `/blog`, e.g. `/blog-introducing.html`!
            environ['PATH_INFO'] = environ['PATH_INFO'][len('/blog'):]
        return self.app(environ, start_response)


def create_celery_app(app=None):
    """
    Create a new Celery app and tie together the Celery config to the app's
    config. Wrap all tasks in the context of the application.

    :param app: Flask app
    :return: Celery app
    """
    app = app or create_app()

    celery = Celery(app.import_name)
    celery.conf.update(app.config.get("CELERY_CONFIG", {}))
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery.Task = ContextTask

    return celery


def create_app(settings_override=None):
    """
    Create a Flask application using the app factory pattern.

    :param settings_override: Override settings
    :return: Flask app
    """
    app = Flask(__name__, static_folder="../public", static_url_path="")

    app.config.from_object("config.settings")

    if settings_override:
        app.config.update(settings_override)

    if not app.debug and len(SECRET_KEY) < 30:
        raise Exception("Use longer SECRET_KEY!")
    if not app.debug and len(DOWNLOADS_SECRET_KEY) < 30:
        raise Exception("Use longer DOWNLOADS_SECRET_KEY!")

    middleware(app)

    app.register_blueprint(account)
    app.register_blueprint(blog)
    app.register_blueprint(dyn)
    app.register_blueprint(page)
    app.register_blueprint(cli)

    extensions(app)

    return app

def extensions(app):
    """
    Register 0 or more extensions (mutates the app passed in).

    :param app: Flask application instance
    :return: None
    """
    debug_toolbar.init_app(app)
    flask_static_digest.init_app(app)
    with app.app_context():
        try:
            with Session(engine) as session:
                session.execute('SELECT 1')
        except:
            print("mariadb not yet online, restarting")
            time.sleep(3)
            sys.exit(1)

        try:
            with Session(mariapersist_engine) as mariapersist_session:
                mariapersist_session.execute('SELECT 1')
        except:
            print("mariapersist not yet online, continuing since it's optional")

        try:
            Reflected.prepare(engine)
        except:
            if os.getenv("DATA_IMPORTS_MODE", "") == "1":
                print("Ignoring db error because DATA_IMPORTS_MODE=1")
            else:
                print("Error in loading tables; reset using './run flask cli dbreset'")

        try:
            ReflectedMariapersist.prepare(mariapersist_engine)
        except:
            print("Error in loading 'mariapersist' db; continuing since it's optional")
    mail.init_app(app)

    def localeselector():
        potential_locale = request.headers['Host'].split('.')[0]
        if potential_locale in [allthethings.utils.get_domain_lang_code(locale) for locale in allthethings.utils.list_translations()]:
            return allthethings.utils.domain_lang_code_to_full_lang_code(potential_locale)
        return 'en'
    babel.init_app(app, locale_selector=localeselector)

    # https://stackoverflow.com/a/57950565
    app.jinja_env.trim_blocks = True
    app.jinja_env.lstrip_blocks = True
    app.jinja_env.globals['get_locale'] = get_locale
    app.jinja_env.globals['FEATURE_FLAGS'] = allthethings.utils.FEATURE_FLAGS
    def urlsafe_b64encode(string):
        return base64.urlsafe_b64encode(string.encode()).decode()
    app.jinja_env.globals['urlsafe_b64encode'] = urlsafe_b64encode

    # https://stackoverflow.com/a/18095320
    hash_cache = {}
    @app.url_defaults
    def add_hash_for_static_files(endpoint, values):
        '''Add content hash argument for url to make url unique.
        It's have sense for updates to avoid caches.
        '''
        if endpoint != 'static':
            return
        filename = values['filename']
        # Exclude some.
        if filename in ['content-search.xml']:
            return
        if filename in hash_cache:
            values['hash'] = hash_cache[filename]
            return
        filepath = safe_join(app.static_folder, filename)
        if os.path.isfile(filepath):
            with open(filepath, 'rb') as static_file:
                filehash = hashlib.md5(static_file.read()).hexdigest()[:20]
                values['hash'] = hash_cache[filename] = filehash

    @functools.cache
    def get_display_name_for_lang(lang_code, display_lang):
        result = langcodes.Language.make(lang_code).display_name(display_lang)
        if '[' not in result:
            result = result + ' [' + lang_code + ']'
        return result.replace(' []', '')

    @functools.cache
    def last_data_refresh_date():
        with engine.connect() as conn:
            libgenrs_statement = select(LibgenrsUpdated.TimeLastModified).order_by(LibgenrsUpdated.ID.desc()).limit(1)
            libgenli_statement = select(LibgenliFiles.time_last_modified).order_by(LibgenliFiles.f_id.desc()).limit(1)
            try:
                libgenrs_time = conn.execute(libgenrs_statement).scalars().first()
                libgenli_time = conn.execute(libgenli_statement).scalars().first()
            except:
                return ''
            latest_time = max([libgenrs_time, libgenli_time])
            return latest_time.date()

    translations_with_english_fallback = set()
    @app.before_request
    def before_req():
        if X_AA_SECRET is not None and request.headers.get('x-aa-secret') != X_AA_SECRET and (not request.full_path.startswith('/dyn/up')):
            return gettext('layout.index.invalid_request', websites='annas-archive.org, .gs, .se')

        # Add English as a fallback language to all translations.
        translations = get_translations()
        if translations not in translations_with_english_fallback:
            with force_locale('en'):
                translations.add_fallback(get_translations())
            translations_with_english_fallback.add(translations)

        g.app_debug = app.debug
        g.base_domain = 'annas-archive.org'
        valid_other_domains = ['annas-archive.gs', 'annas-archive.se']
        if app.debug:
            valid_other_domains.append('localtest.me:8000')
            valid_other_domains.append('localhost:8000')
        for valid_other_domain in valid_other_domains:
            if request.headers['Host'].endswith(valid_other_domain):
                g.base_domain = valid_other_domain
                break

        g.domain_lang_code = allthethings.utils.get_domain_lang_code(get_locale())
        g.show_wechat_in_layout = g.domain_lang_code in ['zh', 'tw']
        g.full_lang_code = allthethings.utils.get_full_lang_code(get_locale())

        g.secure_domain = g.base_domain not in ['localtest.me:8000', 'localhost:8000']
        g.full_domain = g.base_domain
        if g.domain_lang_code != 'en':
            g.full_domain = g.domain_lang_code + '.' + g.base_domain
        if g.secure_domain:
            g.full_domain = 'https://' + g.full_domain
        else:
            g.full_domain = 'http://' + g.full_domain

        g.languages = [(allthethings.utils.get_domain_lang_code(locale), locale.get_display_name()) for locale in allthethings.utils.list_translations()]
        g.languages.sort()

        g.last_data_refresh_date = last_data_refresh_date()
        doc_counts = {content_type['key']: content_type['doc_count'] for content_type in all_search_aggs('en', 'aarecords')[0]['search_content_type']}
        doc_counts['total'] = sum(doc_counts.values())
        doc_counts['journal_article'] = doc_counts.get('journal_article') or 0
        doc_counts['book_comic'] = doc_counts.get('book_comic') or 0
        doc_counts['magazine'] = doc_counts.get('magazine') or 0
        doc_counts['book_any'] = (doc_counts.get('book_unknown') or 0) + (doc_counts.get('book_fiction') or 0) + (doc_counts.get('book_nonfiction') or 0)
        g.header_stats = {key: babel_numbers.format_number(value, locale=get_locale()) for key, value in doc_counts.items() }

        new_header_tagline_scihub = gettext('layout.index.header.tagline_scihub')
        new_header_tagline_libgen = gettext('layout.index.header.tagline_libgen')
        new_header_tagline_zlib = gettext('layout.index.header.tagline_zlib')
        new_header_tagline_openlib = gettext('layout.index.header.tagline_openlib')
        new_header_tagline_duxiu = gettext('layout.index.header.tagline_duxiu')
        new_header_tagline_separator = gettext('layout.index.header.tagline_separator')
        new_header_tagline_and = gettext('layout.index.header.tagline_and')
        new_header_tagline_and_more = gettext('layout.index.header.tagline_and_more')
        new_stats = {
            'book_count': babel_numbers.format_number((doc_counts.get('book_unknown') or 0) + (doc_counts.get('book_fiction') or 0) + (doc_counts.get('book_nonfiction') or 0) + (doc_counts.get('book_comic') or 0) + (doc_counts.get('musical_score') or 0), locale=get_locale()),
            'paper_count': babel_numbers.format_number((doc_counts.get('journal_article') or 0) + (doc_counts.get('standards_document') or 0) + (doc_counts.get('magazine') or 0), locale=get_locale()),
            # 'libraries': new_header_tagline_separator.join([new_header_tagline_scihub, new_header_tagline_libgen]),
            'libraries': "".join([new_header_tagline_scihub, new_header_tagline_and, new_header_tagline_libgen]),
            'scraped': new_header_tagline_separator.join([new_header_tagline_zlib, new_header_tagline_openlib, new_header_tagline_and_more]),
        }
        tagline_newnew2a = gettext('layout.index.header.tagline_newnew2a', **new_stats)
        tagline_newnew2b = gettext('layout.index.header.tagline_newnew2b', **new_stats)
        new_header_tagline = " ".join([gettext('layout.index.header.tagline_new1'), tagline_newnew2a, tagline_newnew2b, gettext('layout.index.header.tagline_new3', **new_stats)])
        g.header_tagline = new_header_tagline
        g.header_tagline_mid = " ".join([gettext('layout.index.header.tagline_new1'), tagline_newnew2a, tagline_newnew2b, gettext('layout.index.header.tagline_new3', **new_stats)])
        g.header_tagline_short = " ".join([gettext('layout.index.header.tagline_new1'), tagline_newnew2a, tagline_newnew2b])
        if str(get_locale()) != 'en':
            with force_locale('en'):
                new_header_tagline_english = " ".join([gettext('layout.index.header.tagline_new1'), tagline_newnew2a, tagline_newnew2b, gettext('layout.index.header.tagline_new3', **new_stats)])
            if new_header_tagline == new_header_tagline_english:
                g.header_tagline = gettext('layout.index.header.tagline', **g.header_stats)
                g.header_tagline_mid = gettext('layout.index.header.tagline', **g.header_stats)
                g.header_tagline_short = gettext('layout.index.header.tagline_short')

    return None


def middleware(app):
    """
    Register 0 or more middleware (mutates the app passed in).

    :param app: Flask application instance
    :return: None
    """
    # Enable the Flask interactive debugger in the brower for development.
    if app.debug:
        app.wsgi_app = DebuggedApplication(app.wsgi_app, evalex=True)

    # Set the real IP address into request.remote_addr when behind a proxy.
    # x_for=2 because of Varnish, then Cloudflare.
    app.wsgi_app = BlogMiddleware(ProxyFix(app.wsgi_app, x_for=2, x_proto=1))

    return None


celery_app = create_celery_app()
