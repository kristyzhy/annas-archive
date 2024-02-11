import os

from flask_babel import Babel
from flask_debugtoolbar import DebugToolbarExtension
from flask_static_digest import FlaskStaticDigest
from sqlalchemy import Column, Integer, ForeignKey, inspect, create_engine, Text
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.ext.declarative import DeferredReflection
from elasticsearch import Elasticsearch
from flask_mail import Mail
from config.settings import ELASTICSEARCH_HOST, ELASTICSEARCHAUX_HOST

debug_toolbar = DebugToolbarExtension()
flask_static_digest = FlaskStaticDigest()
Base = declarative_base()
es = Elasticsearch(hosts=[ELASTICSEARCH_HOST], max_retries=2, retry_on_timeout=True)
es_aux = Elasticsearch(hosts=[ELASTICSEARCHAUX_HOST], max_retries=2, retry_on_timeout=True)
babel = Babel()
mail = Mail()

mariadb_user = os.getenv("MARIADB_USER", "allthethings")
mariadb_password = os.getenv("MARIADB_PASSWORD", "password")
mariadb_host = os.getenv("MARIADB_HOST", "mariadb")
mariadb_port = os.getenv("MARIADB_PORT", "3306")
mariadb_db = os.getenv("MARIADB_DATABASE", mariadb_user)
mariadb_url = f"mysql+pymysql://{mariadb_user}:{mariadb_password}@{mariadb_host}:{mariadb_port}/{mariadb_db}?read_timeout=120&write_timeout=120"
mariadb_url_no_timeout = f"mysql+pymysql://root:{mariadb_password}@{mariadb_host}:{mariadb_port}/{mariadb_db}"
if os.getenv("DATA_IMPORTS_MODE", "") == "1":
    mariadb_url = mariadb_url_no_timeout
engine = create_engine(mariadb_url, future=True, isolation_level="AUTOCOMMIT", pool_size=5, max_overflow=0, pool_recycle=300, pool_pre_ping=True)

mariapersist_user = os.getenv("MARIAPERSIST_USER", "allthethings")
mariapersist_password = os.getenv("MARIAPERSIST_PASSWORD", "password")
mariapersist_host = os.getenv("MARIAPERSIST_HOST", "mariapersist")
mariapersist_port = os.getenv("MARIAPERSIST_PORT", "3333")
mariapersist_db = os.getenv("MARIAPERSIST_DATABASE", mariapersist_user)
mariapersist_url = f"mysql+pymysql://{mariapersist_user}:{mariapersist_password}@{mariapersist_host}:{mariapersist_port}/{mariapersist_db}?read_timeout=120&write_timeout=120"
mariapersist_engine = create_engine(mariapersist_url, future=True, isolation_level="AUTOCOMMIT", pool_size=5, max_overflow=0, pool_recycle=300, pool_pre_ping=True)

class Reflected(DeferredReflection, Base):
    __abstract__ = True
    def to_dict(self):
        unloaded = inspect(self).unloaded
        return dict((col.name, getattr(self, col.name)) for col in self.__table__.columns if col.name not in unloaded)

class ReflectedMariapersist(DeferredReflection, Base):
    __abstract__ = True
    def to_dict(self):
        unloaded = db.inspect(self).unloaded
        return dict((col.name, getattr(self, col.name)) for col in self.__table__.columns if col.name not in unloaded)

class ZlibBook(Reflected):
    __tablename__ = "zlib_book"
    isbns = relationship("ZlibIsbn", lazy="selectin")
class ZlibIsbn(Reflected):
    __tablename__ = "zlib_isbn"
    zlibrary_id = Column(Integer, ForeignKey("zlib_book.zlibrary_id"))

class IsbndbIsbns(Reflected):
    __tablename__ = "isbndb_isbns"

class LibgenliFiles(Reflected):
    __tablename__ = "libgenli_files"
    add_descrs = relationship("LibgenliFilesAddDescr", lazy="selectin")
    editions = relationship("LibgenliEditions", lazy="selectin", secondary="libgenli_editions_to_files")
class LibgenliFilesAddDescr(Reflected):
    __tablename__ = "libgenli_files_add_descr"
    f_id = Column(Integer, ForeignKey("libgenli_files.f_id"))
class LibgenliEditionsToFiles(Reflected):
    __tablename__ = "libgenli_editions_to_files"
    f_id = Column(Integer, ForeignKey("libgenli_files.f_id"))
    e_id = Column(Integer, ForeignKey("libgenli_editions.e_id"))
class LibgenliEditions(Reflected):
    __tablename__ = "libgenli_editions"
    issue_s_id = Column(Integer, ForeignKey("libgenli_series.s_id"))
    series = relationship("LibgenliSeries", lazy="joined")
    add_descrs = relationship("LibgenliEditionsAddDescr", lazy="selectin")
class LibgenliEditionsAddDescr(Reflected):
    __tablename__ = "libgenli_editions_add_descr"
    e_id = Column(Integer, ForeignKey("libgenli_editions.e_id"))
    publisher = relationship("LibgenliPublishers", lazy="joined", primaryjoin="(remote(LibgenliEditionsAddDescr.value) == foreign(LibgenliPublishers.p_id)) & (LibgenliEditionsAddDescr.key == 308)")
class LibgenliPublishers(Reflected):
    __tablename__ = "libgenli_publishers"
class LibgenliSeries(Reflected):
    __tablename__ = "libgenli_series"
    issn_add_descrs = relationship("LibgenliSeriesAddDescr", lazy="joined", primaryjoin="(LibgenliSeries.s_id == LibgenliSeriesAddDescr.s_id) & (LibgenliSeriesAddDescr.key == 501)")
class LibgenliSeriesAddDescr(Reflected):
    __tablename__ = "libgenli_series_add_descr"
    s_id = Column(Integer, ForeignKey("libgenli_series.s_id"))
class LibgenliElemDescr(Reflected):
    __tablename__ = "libgenli_elem_descr"

class LibgenrsDescription(Reflected):
    __tablename__ = "libgenrs_description"
class LibgenrsHashes(Reflected):
    __tablename__ = "libgenrs_hashes"
class LibgenrsTopics(Reflected):
    __tablename__ = "libgenrs_topics"
class LibgenrsUpdated(Reflected):
    __tablename__ = "libgenrs_updated"

class LibgenrsFiction(Reflected):
    __tablename__ = "libgenrs_fiction"
class LibgenrsFictionDescription(Reflected):
    __tablename__ = "libgenrs_fiction_description"
class LibgenrsFictionHashes(Reflected):
    __tablename__ = "libgenrs_fiction_hashes"

class OlBase(Reflected):
    __tablename__ = "ol_base"

class AaIa202306Metadata(Reflected):
    __tablename__ = "aa_ia_2023_06_metadata"
class AaIa202306Files(Reflected):
    __tablename__ = "aa_ia_2023_06_files"
class Ia2Records(Reflected):
    __tablename__ = "annas_archive_meta__aacid__ia2_records"
class Ia2AcsmpdfFiles(Reflected):
    __tablename__ = "annas_archive_meta__aacid__ia2_acsmpdf_files"


class MariapersistDownloadsTotalByMd5(ReflectedMariapersist):
    __tablename__ = "mariapersist_downloads_total_by_md5"
class MariapersistAccounts(ReflectedMariapersist):
    __tablename__ = "mariapersist_accounts"
class MariapersistDownloads(ReflectedMariapersist):
    __tablename__ = "mariapersist_downloads"
class MariapersistDownloadsHourlyByMd5(ReflectedMariapersist):
    __tablename__ = "mariapersist_downloads_hourly_by_md5"
class MariapersistDownloadsHourly(ReflectedMariapersist):
    __tablename__ = "mariapersist_downloads_hourly"
class MariapersistMd5Report(ReflectedMariapersist):
    __tablename__ = "mariapersist_md5_report"
class MariapersistComments(ReflectedMariapersist):
    __tablename__ = "mariapersist_comments"
class MariapersistReactions(ReflectedMariapersist):
    __tablename__ = "mariapersist_reactions"
class MariapersistLists(ReflectedMariapersist):
    __tablename__ = "mariapersist_lists"
class MariapersistListEntries(ReflectedMariapersist):
    __tablename__ = "mariapersist_list_entries"
class MariapersistDonations(ReflectedMariapersist):
    __tablename__ = "mariapersist_donations"
class MariapersistCopyrightClaims(ReflectedMariapersist):
    __tablename__ = "mariapersist_copyright_claims"
class MariapersistFastDownloadAccess(ReflectedMariapersist):
    __tablename__ = "mariapersist_fast_download_access"
class MariapersistSmallFiles(ReflectedMariapersist):
    __tablename__ = "mariapersist_small_files"
# class MariapersistSearches(ReflectedMariapersist):
#     __tablename__ = "mariapersist_searches"


