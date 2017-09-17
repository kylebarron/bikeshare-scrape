SHOW DATABASES;
SET sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';
CREATE DATABASE hubway;
USE hubway;
SHOW tables;
CREATE TABLE station_status (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    station_id SMALLINT UNSIGNED,
    num_bikes_available TINYINT UNSIGNED,
    num_bikes_disabled TINYINT UNSIGNED,
    num_docks_available TINYINT UNSIGNED,
    num_docks_disabled TINYINT UNSIGNED,
    is_installed BOOLEAN,
    is_renting BOOLEAN,
    is_returning BOOLEAN,
    last_updated TIMESTAMP, -- GBFS last updated
    last_reported TIMESTAMP, -- Station by station reporting 
    eightd_has_available_keys BOOLEAN
);
SHOW TABLES;
DESCRIBE station_status;




