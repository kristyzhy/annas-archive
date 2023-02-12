CREATE TABLE mariapersist_downloads (
    `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    `md5` BINARY(16) NOT NULL,
    `ip` BINARY(16) NOT NULL,
    PRIMARY KEY (`timestamp`, `md5`, `ip`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `mariapersist_downloads_hourly` ( `hour_since_epoch` BIGINT, `count` INT, PRIMARY KEY(hour_since_epoch) ) ENGINE=InnoDB;
