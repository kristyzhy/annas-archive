DROP FUNCTION IF EXISTS `ISBN10to13`;
delimiter //
CREATE FUNCTION `ISBN10to13`(isbn10 VARCHAR(50)) RETURNS varchar(50) CHARSET utf8
BEGIN
    DECLARE isbn13 VARCHAR(13);
    DECLARE i   INT;
    DECLARE chk INT;

    IF (LENGTH(ISBN10) > 10) THEN 
        RETURN ISBN10;
    ELSE 
        SET isbn10=SUBSTRING(ISBN10,1,10);
    END IF;

    # set ISBN10    = '0123456479';
    SET isbn13  = CONCAT('978' , LEFT(isbn10, 9));
    SET i = 1, chk  = 0;

    # 9*1+7*3+8*1=38
    SET chk = (38 + 3*LEFT(isbn10,1) 
        + RIGHT(LEFT(isbn10,2),1)
        + 3*RIGHT(LEFT(isbn10,3),1)
        + RIGHT(LEFT(isbn10,4),1)
        + 3*RIGHT(LEFT(isbn10,5),1)
        + RIGHT(LEFT(isbn10,6),1)
        + 3*RIGHT(LEFT(isbn10,7),1) 
        + RIGHT(LEFT(isbn10,8),1) 
        + 3*LEFT(RIGHT(isbn10,2),1));

    SET chk = 10 - (chk % 10);
    IF (chk<>10) then
        SET isbn13 = concat(isbn13 , CONVERT(chk, CHAR(1)));
    ELSE
        SET isbn13 = concat(isbn13 , '0');
    END IF;
    RETURN isbn13;
END //
delimiter ;
# DELIMITER FOR cli/views.py

-- ~37 mins
ALTER TABLE allthethings.ol_base ADD PRIMARY KEY(ol_key);

-- TODO: change to VARCHAR and ascii?
-- Note that many books have only ISBN10.
-- ~20mins
DROP TABLE IF EXISTS allthethings.ol_isbn13;
CREATE TABLE allthethings.ol_isbn13 (isbn CHAR(13), ol_key CHAR(200), PRIMARY KEY(isbn, ol_key)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin IGNORE SELECT x.isbn AS isbn, ol_key FROM allthethings.ol_base b CROSS JOIN JSON_TABLE(b.json, '$.isbn_13[*]' COLUMNS (isbn VARCHAR(100) PATH '$')) x WHERE ol_key LIKE '/books/OL%' AND LENGTH(x.isbn) = 13 AND x.isbn REGEXP '[0-9]{12}[0-9X]';
-- ~60mins
INSERT IGNORE INTO allthethings.ol_isbn13 (isbn, ol_key) SELECT ISBN10to13(x.isbn) AS isbn, ol_key FROM allthethings.ol_base b CROSS JOIN JSON_TABLE(b.json, '$.isbn_10[*]' COLUMNS (isbn CHAR(10) PATH '$')) x WHERE ol_key LIKE '/books/OL%' AND LENGTH(x.isbn) = 10 AND x.isbn REGEXP '[0-9]{9}[0-9X]';

-- ~10mins
DROP TABLE IF EXISTS allthethings.ol_ocaid;
CREATE TABLE allthethings.ol_ocaid (ocaid VARCHAR(500), ol_key VARCHAR(200), PRIMARY KEY(ocaid, ol_key)) ENGINE=MyISAM DEFAULT CHARSET=ascii COLLATE=ascii_bin SELECT JSON_UNQUOTE(JSON_EXTRACT(json, '$.ocaid')) AS ocaid, ol_key FROM ol_base WHERE JSON_UNQUOTE(JSON_EXTRACT(json, '$.ocaid')) IS NOT NULL AND ol_key LIKE '/books/OL%';

DROP TABLE IF EXISTS allthethings.ol_annas_archive;
CREATE TABLE allthethings.ol_annas_archive (annas_archive_md5 CHAR(32), ol_key CHAR(200), PRIMARY KEY(annas_archive_md5, ol_key)) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin IGNORE SELECT LOWER(x.annas_archive_md5) AS annas_archive_md5, ol_key FROM allthethings.ol_base b CROSS JOIN JSON_TABLE(b.json, '$.identifiers.annas_archive[*]' COLUMNS (annas_archive_md5 VARCHAR(100) PATH '$')) x WHERE ol_key LIKE '/books/OL%' AND LENGTH(x.annas_archive_md5) = 32 AND x.annas_archive_md5 REGEXP '[0-9A-Fa-f]{32}';
