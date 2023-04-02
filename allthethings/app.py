import hashlib
import os
import functools

from celery import Celery
from flask import Flask, request, g
from werkzeug.security import safe_join
from werkzeug.debug import DebuggedApplication
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_babel import get_locale, get_translations, force_locale

from allthethings.account.views import account
from allthethings.blog.views import blog
from allthethings.page.views import page
from allthethings.dyn.views import dyn
from allthethings.cli.views import cli
from allthethings.extensions import engine, mariapersist_engine, es, babel, debug_toolbar, flask_static_digest, Base, Reflected, ReflectedMariapersist, mail
from config.settings import SECRET_KEY

import allthethings.utils

# Rewrite `annas-blog.org` to `/blog` as a workaround for Flask not nicely supporting multiple domains.
# Also strip `/blog` if we encounter it directly, to avoid duplicating it.
class BlogMiddleware(object):
    def __init__(self, app):
        self.app = app
    def __call__(self, environ, start_response):
        if environ['HTTP_HOST'].startswith('annas-blog.org'): # `startswith` so we can test using http://annas-blog.org.localtest.me:8000/
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
            Reflected.prepare(engine)
        except:
            if os.getenv("DATA_IMPORTS_MODE", "") == "1":
                print("Ignoring db error because DATA_IMPORTS_MODE=1")
            else:
                print("Error in loading tables; comment out the following 'raise' in app.py to prevent restarts; and then reset using './run flask cli dbreset'")
                raise
        try:
            ReflectedMariapersist.prepare(mariapersist_engine)
        except:
            print("Error in loading 'mariapersist' db; continuing since it's optional")
    es.init_app(app)
    babel.init_app(app)
    mail.init_app(app)

    # https://stackoverflow.com/a/57950565
    app.jinja_env.trim_blocks = True
    app.jinja_env.lstrip_blocks = True
    app.jinja_env.globals['get_locale'] = get_locale

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

    @babel.localeselector
    def localeselector():
        potential_locale = request.headers['Host'].split('.')[0]
        if potential_locale in [allthethings.utils.get_domain_lang_code(locale) for locale in babel.list_translations()]:
            return allthethings.utils.domain_lang_code_to_full_lang_code(potential_locale)
        return 'en'

    @functools.cache
    def last_data_refresh_date():
        with engine.connect() as conn:
            try:
                libgenrs_time = conn.execute(select(LibgenrsUpdated.TimeLastModified).order_by(LibgenrsUpdated.ID.desc()).limit(1)).scalars().first()
                libgenli_time = conn.execute(select(LibgenliFiles.time_last_modified).order_by(LibgenliFiles.f_id.desc()).limit(1)).scalars().first()
                latest_time = max([libgenrs_time, libgenli_time])
                return latest_time.date()
            except:
                return ''

    translations_with_english_fallback = set()
    @app.before_request
    def before_req():
        # Add English as a fallback language to all translations.
        translations = get_translations()
        if translations not in translations_with_english_fallback:
            with force_locale('en'):
                translations.add_fallback(get_translations())
            translations_with_english_fallback.add(translations)

        g.base_domain = 'annas-archive.org'
        valid_other_domains = ['annas-archive.gs']
        if app.debug:
            valid_other_domains.append('localtest.me:8000')
            valid_other_domains.append('localhost:8000')
        for valid_other_domain in valid_other_domains:
            if request.headers['Host'].endswith(valid_other_domain):
                g.base_domain = valid_other_domain
                break

        g.domain_lang_code = allthethings.utils.get_domain_lang_code(get_locale())
        g.full_lang_code = allthethings.utils.get_full_lang_code(get_locale())

        g.secure_domain = g.base_domain not in ['localtest.me:8000', 'localhost:8000']
        g.full_domain = g.base_domain
        if g.domain_lang_code != 'en':
            g.full_domain = g.domain_lang_code + '.' + g.base_domain
        if g.secure_domain:
            g.full_domain = 'https://' + g.full_domain
        else:
            g.full_domain = 'http://' + g.full_domain

        g.languages = [(allthethings.utils.get_domain_lang_code(locale), locale.get_display_name()) for locale in babel.list_translations()]
        g.languages.sort()

        g.last_data_refresh_date = last_data_refresh_date()


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
    app.wsgi_app = BlogMiddleware(ProxyFix(app.wsgi_app))

    return None


celery_app = create_celery_app()
