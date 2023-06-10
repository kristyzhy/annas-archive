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
