# When adding one of these, be sure to update mariapersist_reset_internal and mariapersist_drop_all.sql!

CREATE TABLE mariapersist_copyright_claims (
    `copyright_claim_id` BIGINT NOT NULL AUTO_INCREMENT,
    `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `ip` BINARY(16) NOT NULL,
    `json` JSON NOT NULL,
    PRIMARY KEY (`copyright_claim_id`),
    INDEX (`created`),
    INDEX (`ip`,`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE mariapersist_md5_report (
    `md5_report_id` BIGINT NOT NULL AUTO_INCREMENT,
    `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `md5` BINARY(16) NOT NULL,
    `account_id` CHAR(7) NOT NULL,
    `type` CHAR(10) NOT NULL,
    `better_md5` BINARY(16) NULL,
    PRIMARY KEY (`md5_report_id`),
    INDEX (`created`),
    INDEX (`account_id`,`created`),
    INDEX (`md5`,`created`),
    INDEX (`better_md5`,`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
ALTER TABLE mariapersist_md5_report ADD CONSTRAINT `mariapersist_md5_report_account_id` FOREIGN KEY(`account_id`) REFERENCES `mariapersist_accounts` (`account_id`);

ALTER TABLE mariapersist_accounts DROP INDEX display_name;

CREATE TABLE mariapersist_comments (
    `comment_id` BIGINT NOT NULL AUTO_INCREMENT,
    `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `account_id` CHAR(7) NOT NULL,
    `resource` VARCHAR(255) NOT NULL,
    `content` TEXT NOT NULL,
    PRIMARY KEY (`comment_id`),
    INDEX (`created`),
    INDEX (`account_id`,`created`),
    INDEX (`resource`,`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
ALTER TABLE mariapersist_comments ADD CONSTRAINT `mariapersist_comments_account_id` FOREIGN KEY(`account_id`) REFERENCES `mariapersist_accounts` (`account_id`);

