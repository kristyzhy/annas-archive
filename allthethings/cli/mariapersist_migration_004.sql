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
