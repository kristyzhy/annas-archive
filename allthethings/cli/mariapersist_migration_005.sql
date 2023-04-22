# When adding one of these, be sure to update mariapersist_reset_internal and mariapersist_drop_all.sql!

CREATE TABLE mariapersist_download_tests (
    `download_test_id` BIGINT NOT NULL AUTO_INCREMENT,
    `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `md5` BINARY(16) NOT NULL,
    `server` VARCHAR(255) NOT NULL,
    `url` VARCHAR(255) NOT NULL,
    `filesize` BIGINT NOT NULL,
    `elapsed_sec` BIGINT NOT NULL,
    `kbps` BIGINT NOT NULL,
    PRIMARY KEY (`download_test_id`),
    INDEX (`created`),
    INDEX (`md5`,`created`),
    INDEX (`server`,`created`),
    INDEX (`url`,`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
