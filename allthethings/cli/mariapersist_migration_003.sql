# When adding one of these, be sure to update mariapersist_reset_internal and mariapersist_drop_all.sql!

CREATE TABLE mariapersist_accounts (
    `id` CHAR(7) NOT NULL,
    `email_verified` VARCHAR(255) NOT NULL,
    `display_name` VARCHAR(255) NOT NULL,
    `newsletter_unsubscribe` TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE INDEX (`email_verified`),
    UNIQUE INDEX (`display_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
