CREATE TABLE `mariapersist_downloads_hourly_by_ip` ( `ip` BINARY(16), `hour_since_epoch` BIGINT, `count` INT, PRIMARY KEY(ip, hour_since_epoch) ) ENGINE=InnoDB;

CREATE TABLE `mariapersist_downloads_hourly_by_md5` ( `md5` BINARY(16), `hour_since_epoch` BIGINT, `count` INT, PRIMARY KEY(md5, hour_since_epoch) ) ENGINE=InnoDB;

CREATE TABLE `mariapersist_downloads_total_by_md5` ( `md5` BINARY(16), `count` INT, PRIMARY KEY(md5) ) ENGINE=InnoDB;

CREATE TABLE mariapersist_downloads (
    `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    `md5` BINARY(16) NOT NULL,
    `ip` BINARY(16) NOT NULL,
    PRIMARY KEY (`timestamp`, `md5`, `ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `mariapersist_downloads_hourly` ( `hour_since_epoch` BIGINT, `count` INT, PRIMARY KEY(hour_since_epoch) ) ENGINE=InnoDB;

CREATE TABLE mariapersist_accounts (
    `account_id` CHAR(7) NOT NULL,
    `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `email_verified` VARCHAR(255) NOT NULL,
    `display_name` VARCHAR(255) NOT NULL,
    `newsletter_unsubscribe` TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (`account_id`),
    UNIQUE INDEX (`email_verified`),
    UNIQUE INDEX (`display_name`),
    INDEX (`created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE mariapersist_account_logins (
    `account_id` CHAR(7) NOT NULL,
    `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `ip` BINARY(16) NOT NULL,
    PRIMARY KEY (`account_id`, `created`, `ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE mariapersist_downloads ADD COLUMN `account_id` CHAR(7) NULL;
ALTER TABLE mariapersist_downloads ADD CONSTRAINT `mariapersist_downloads_account_id` FOREIGN KEY(`account_id`) REFERENCES `mariapersist_accounts` (`account_id`);
ALTER TABLE mariapersist_account_logins ADD CONSTRAINT `mariapersist_account_logins_account_id` FOREIGN KEY(`account_id`) REFERENCES `mariapersist_accounts` (`account_id`);
ALTER TABLE mariapersist_downloads ADD INDEX `account_id_timestamp` (`account_id`, `timestamp`);

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

CREATE TABLE mariapersist_donations (
    `donation_id` CHAR(22) NOT NULL,
    `created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `account_id` CHAR(7) NOT NULL,
    `cost_cents_usd` INT NOT NULL,
    `cost_cents_native_currency` INT NOT NULL,
    `native_currency_code` CHAR(10) NOT NULL,
    `processing_status` TINYINT NOT NULL, # 0=unpaid, 1=paid, 2=cancelled, 3=expired, 4=manualconfirm
    `donation_type` SMALLINT NOT NULL, # 0=manual
    `ip` BINARY(16) NOT NULL,
    `json` JSON NOT NULL,
    PRIMARY KEY (`donation_id`),
    INDEX (`created`),
    INDEX (`account_id`, `processing_status`, `created`),
    INDEX (`donation_type`, `created`),
    INDEX (`processing_status`, `created`),
    INDEX (`cost_cents_usd`, `created`),
    INDEX (`cost_cents_native_currency`, `created`),
    INDEX (`native_currency_code`, `created`),
    INDEX (`ip`, `created`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

ALTER TABLE mariapersist_accounts ADD COLUMN `membership_tier` CHAR(7) NOT NULL DEFAULT 0;
ALTER TABLE mariapersist_accounts ADD COLUMN `membership_expiration` TIMESTAMP NULL;

ALTER TABLE mariapersist_accounts MODIFY `email_verified` VARCHAR(255) NULL;

CREATE TABLE mariapersist_fast_download_access (
    `account_id` CHAR(7) NOT NULL,
    `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    `md5` BINARY(16) NOT NULL,
    `ip` BINARY(16) NOT NULL,
    PRIMARY KEY (`account_id`, `timestamp`, `md5`, `ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
