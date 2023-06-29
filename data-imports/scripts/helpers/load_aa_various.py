#!/bin/python3 

# Run with PYTHONIOENCODING=UTF8:ignore

import os
import sys
import gzip
import tarfile
import orjson
import pymysql
import pymysql.cursors
from more_itertools import ichunked

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


db = pymysql.connect(host='localhost', user='allthethings', password='password', database='allthethings', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
cursor = db.cursor()
cursor.execute('DROP TABLE IF EXISTS aa_ia_2023_06_metadata')
cursor.execute('CREATE TABLE aa_ia_2023_06_metadata (`ia_id` VARCHAR(100) NOT NULL, `has_thumb` TINYINT(1) NOT NULL, `json` JSON NULL, PRIMARY KEY(`ia_id`)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;')
db.commit()

thumbs_set = set()
with gzip.open('/temp-dir/annas-archive-ia-2023-06-thumbs.txt.gz', 'rt') as thumbs_files:
    thumbs_list = thumbs_files.read().splitlines()
    thumbs_set = set(thumbs_list)

i = 0
json_tar_file = tarfile.open('/temp-dir/annas-archive-ia-2023-06-metadata-json.tar.gz', 'r|*')
for json_file_chunk in ichunked(json_tar_file, 1):

    save_data = []
    for index, json_file in enumerate(json_file_chunk):
        if index == 0:
            print(f"Saving chunk from tar file starting with {json_file.name}...")
        json = orjson.loads(json_tar_file.extractfile(json_file).read())
        aa_shorter_files = [file_json for file_json in (json.get('files', None) or []) if os.path.splitext(file_json.get('name', None) or '')[1] in ['.jpg','.pdf','.epub','.lcpdf']]
        json['files'] = []
        json['aa_shorter_files'] = aa_shorter_files

        ia_id = json_file.name.removeprefix('./').removesuffix('.json')

        has_thumb = ia_id in thumbs_set
        if has_thumb:
            thumbs_set.remove(ia_id)

        save_data.append((ia_id, (1 if has_thumb else 0), orjson.dumps(json)))

    cursor.executemany("INSERT INTO aa_ia_2023_06_metadata (ia_id, has_thumb, json) VALUES (%s, %s, %s);", save_data)
    db.commit()

for ia_id_chunk in chunked(thumbs_set, 100000):
    print(f"Saving leftover chunk from thumbs...")
    cursor.executemany("INSERT INTO aa_ia_2023_06_metadata (ia_id, has_thumb, json) VALUES (%s, 1, NULL);", [(ia_id,) for ia_id in ia_id_chunk])
    db.commit()
