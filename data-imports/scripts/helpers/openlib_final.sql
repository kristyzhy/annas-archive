-- ~37 mins
ALTER TABLE allthethings.ol_base ADD PRIMARY KEY(ol_key);

-- ~20mins
-- When re-enabling, note that this doesn't include all ISBNs, since many books have only ISBN10.
-- CREATE TABLE allthethings.ol_isbn13 (PRIMARY KEY(isbn, ol_key)) ENGINE=MyISAM IGNORE SELECT x.isbn AS isbn, ol_key FROM allthethings.ol_base b CROSS JOIN JSON_TABLE(b.json, '$.isbn_13[*]' COLUMNS (isbn CHAR(13) PATH '$')) x WHERE ol_key LIKE '/books/OL%';
