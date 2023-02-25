import datetime
from rfeed import *
from flask import Blueprint, request, render_template, make_response

# Note that /blog is not a real path; we do a trick with BlogMiddleware in app.py to rewrite annas-blog.org here.
blog = Blueprint("blog", __name__, template_folder="templates", url_prefix="/blog")

@blog.get("/")
def index():
    return render_template("index.html")

@blog.get("/annas-update-open-source-elasticsearch-covers.html")
def annas_update_open_source_elasticsearch_covers():
    return render_template("annas-update-open-source-elasticsearch-covers.html")
@blog.get("/help-seed-zlibrary-on-ipfs.html")
def help_seed_zlibrary_on_ipfs():
    return render_template("help-seed-zlibrary-on-ipfs.html")
@blog.get("/putting-5,998,794-books-on-ipfs.html")
def putting_5998794_books_on_ipfs():
    return render_template("putting-5,998,794-books-on-ipfs.html")
@blog.get("/blog-isbndb-dump-how-many-books-are-preserved-forever.html")
def blog_isbndb_dump_how_many_books_are_preserved_forever():
    return render_template("blog-isbndb-dump-how-many-books-are-preserved-forever.html")
@blog.get("/blog-how-to-become-a-pirate-archivist.html")
def blog_how_to_become_a_pirate_archivist():
    return render_template("blog-how-to-become-a-pirate-archivist.html")
@blog.get("/blog-3x-new-books.html")
def blog_3x_new_books():
    return render_template("blog-3x-new-books.html")
@blog.get("/blog-introducing.html")
def blog_introducing():
    return render_template("blog-introducing.html")

@blog.get("/rss.xml")
def rss_xml():
    items = [
        Item(
            title = "Introducing the Pirate Library Mirror: Preserving 7TB of books (that are not in Libgen)",
            link = "https://annas-blog.org/blog-introducing.html",
            description = "The first library that we have mirrored is Z-Library. This is a popular (and illegal) library.",
            author = "Anna and the Pirate Library Mirror team",
            pubDate = datetime.datetime(2022,7,1),
        ),
        Item(
            title = "3x new books added to the Pirate Library Mirror (+24TB, 3.8 million books)",
            link = "https://annas-blog.org/blog-3x-new-books.html",
            description = "We have also gone back and scraped some books that we missed the first time around. All in all, this new collection is about 24TB, which is much bigger than the last one (7TB).",
            author = "Anna and the Pirate Library Mirror team",
            pubDate = datetime.datetime(2022,9,25),
        ),
        Item(
            title = "How to become a pirate archivist",
            link = "https://annas-blog.org/blog-how-to-become-a-pirate-archivist.html",
            description = "The first challenge might be a supriring one. It is not a technical problem, or a legal problem. It is a psychological problem.",
            author = "Anna and the Pirate Library Mirror team",
            pubDate = datetime.datetime(2022,10,17),
        ),
        Item(
            title = "ISBNdb dump, or How Many Books Are Preserved Forever?",
            link = "https://annas-blog.org/blog-isbndb-dump-how-many-books-are-preserved-forever.html",
            description = "If we were to properly deduplicate the files from shadow libraries, what percentage of all the books in the world have we preserved?",
            author = "Anna and the Pirate Library Mirror team",
            pubDate = datetime.datetime(2022,10,31),
        ),
        Item(
            title = "Putting 5,998,794 books on IPFS",
            link = "https://annas-blog.org/putting-5,998,794-books-on-ipfs.html",
            description = "Putting dozens of terabytes of data on IPFS is no joke.",
            author = "Anna and the Pirate Library Mirror team",
            pubDate = datetime.datetime(2022,11,19),
        ),
        Item(
            title = "Help seed Z-Library on IPFS",
            link = "https://annas-blog.org/help-seed-zlibrary-on-ipfs.html",
            description = "YOU can help preserve access to this collection.",
            author = "Anna and the Pirate Library Mirror team",
            pubDate = datetime.datetime(2022,11,22),
        ),
        Item(
            title = "Anna’s Update: fully open source archive, ElasticSearch, 300GB+ of book covers",
            link = "https://annas-blog.org/annas-update-open-source-elasticsearch-covers.html",
            description = "We’ve been working around the clock to provide a good alternative with Anna’s Archive. Here are some of the things we achieved recently.",
            author = "Anna and the Pirate Library Mirror team",
            pubDate = datetime.datetime(2022,12,9),
        ),
    ]

    feed = Feed(
        title = "Anna’s Blog",
        link = "https://annas-blog.org/",
        description = "Hi, I’m Anna. I created Anna’s Archive. This is my personal blog, in which I and my teammates write about piracy, digital preservation, and more.",
        language = "en-US",
        lastBuildDate = datetime.datetime.now(),
        items = items,
    )

    print(feed.rss())
     
    response = make_response(feed.rss())
    response.headers['Content-Type'] = 'application/rss+xml; charset=utf-8'
    return response
