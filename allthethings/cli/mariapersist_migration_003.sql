# When adding one of these, be sure to update mariapersist_reset_internal and mariapersist_drop_all.sql!

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

