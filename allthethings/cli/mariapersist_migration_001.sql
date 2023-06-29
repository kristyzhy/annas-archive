# When adding one of these, be sure to update mariapersist_reset_internal!

CREATE TABLE `mariapersist_downloads_hourly_by_ip` ( `ip` BINARY(16), `hour_since_epoch` BIGINT, `count` INT, PRIMARY KEY(ip, hour_since_epoch) ) ENGINE=InnoDB;

CREATE TABLE `mariapersist_downloads_hourly_by_md5` ( `md5` BINARY(16), `hour_since_epoch` BIGINT, `count` INT, PRIMARY KEY(md5, hour_since_epoch) ) ENGINE=InnoDB;

CREATE TABLE `mariapersist_downloads_total_by_md5` ( `md5` BINARY(16), `count` INT, PRIMARY KEY(md5) ) ENGINE=InnoDB;
