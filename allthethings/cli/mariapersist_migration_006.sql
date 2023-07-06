# When adding one of these, be sure to update mariapersist_reset_internal!

CREATE TABLE mariapersist_fast_download_access (
    `account_id` CHAR(7) NOT NULL,
    `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    `md5` BINARY(16) NOT NULL,
    `ip` BINARY(16) NOT NULL,
    PRIMARY KEY (`account_id`, `timestamp`, `md5`, `ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
