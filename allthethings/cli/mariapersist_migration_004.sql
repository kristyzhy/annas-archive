# When adding one of these, be sure to update mariapersist_reset_internal!

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

CREATE TABLE mariapersist_reactions (
    `reaction_id` BIGINT NOT NULL AUTO_INCREMENT,
    `account_id` CHAR(7) NOT NULL,
    `resource` VARCHAR(255) NOT NULL,
    `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `type` TINYINT(1) NOT NULL, # 0=unset, 1=abuse, 2=thumbsup, 3=thumbsdown
    PRIMARY KEY (`reaction_id`),
    UNIQUE INDEX (`account_id`,`resource`),
    INDEX (`updated`),
    INDEX (`account_id`,`updated`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
ALTER TABLE mariapersist_reactions ADD CONSTRAINT `mariapersist_reactions_account_id` FOREIGN KEY(`account_id`) REFERENCES `mariapersist_accounts` (`account_id`);

CREATE TABLE mariapersist_lists (
    `list_id` CHAR(7) NOT NULL,
    `account_id` CHAR(7) NOT NULL,
    `name` VARCHAR(255) NOT NULL,
    `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`list_id`),
    INDEX (`updated`),
    INDEX (`account_id`,`updated`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
ALTER TABLE mariapersist_lists ADD CONSTRAINT `mariapersist_lists_account_id` FOREIGN KEY(`account_id`) REFERENCES `mariapersist_accounts` (`account_id`);

CREATE TABLE mariapersist_list_entries (
    `list_entry_id` BIGINT NOT NULL AUTO_INCREMENT,
    `account_id` CHAR(7) NOT NULL,
    `list_id` CHAR(7) NOT NULL,
    `resource` VARCHAR(255) NOT NULL,
    `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`list_entry_id`),
    UNIQUE INDEX (`resource`,`list_id`),
    INDEX (`updated`),
    INDEX (`list_id`,`updated`),
    INDEX (`account_id`,`updated`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
ALTER TABLE mariapersist_list_entries ADD CONSTRAINT `mariapersist_list_entries_account_id` FOREIGN KEY(`account_id`) REFERENCES `mariapersist_accounts` (`account_id`);
ALTER TABLE mariapersist_list_entries ADD CONSTRAINT `mariapersist_list_entries_list_id` FOREIGN KEY(`list_id`) REFERENCES `mariapersist_lists` (`list_id`);

