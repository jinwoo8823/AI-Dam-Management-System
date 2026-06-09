CREATE TABLE IF NOT EXISTS downstream_station (
    station_code VARCHAR(20) PRIMARY KEY,
    station_name VARCHAR(100) NOT NULL,
    basin VARCHAR(50),
    longitude DECIMAL(12, 8),
    latitude DECIMAL(12, 8),
    observation_start_year INT,
    source VARCHAR(50) DEFAULT 'K_WATER_WATER_LEVEL_STATION_SPEC',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dam_downstream_station_map (
    dam_code INT NOT NULL,
    dam_name VARCHAR(50) NOT NULL,
    station_code VARCHAR(20) NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    basin VARCHAR(50),
    distance_km DECIMAL(8, 2),
    rank_no INT NOT NULL DEFAULT 1,
    mapping_type VARCHAR(30) DEFAULT 'DISTANCE_CANDIDATE',
    note VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (dam_code, station_code),
    INDEX idx_station_code (station_code),
    CONSTRAINT fk_downstream_station_map_station
        FOREIGN KEY (station_code) REFERENCES downstream_station(station_code)
        ON UPDATE CASCADE
) DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS downstream_water_level_observation (
    station_code VARCHAR(20) NOT NULL,
    observed_at DATETIME NOT NULL,
    water_level DECIMAL(12, 4),
    flow_rate DECIMAL(14, 4),
    source VARCHAR(50) DEFAULT 'K_WATER_EXCLLNCOBSRVT',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (station_code, observed_at),
    CONSTRAINT fk_downstream_water_level_station
        FOREIGN KEY (station_code) REFERENCES downstream_station(station_code)
        ON UPDATE CASCADE
) DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dam_representative_downstream_station (
    dam_code INT PRIMARY KEY,
    dam_name VARCHAR(50) NOT NULL,
    station_code VARCHAR(20) NOT NULL,
    station_name VARCHAR(100) NOT NULL,
    basin VARCHAR(50),
    distance_km DECIMAL(8, 2),
    latitude DECIMAL(12, 8),
    longitude DECIMAL(12, 8),
    selection_type VARCHAR(30) DEFAULT 'AUTO_SELECTED',
    selection_reason VARCHAR(255),
    note VARCHAR(255),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_station_code (station_code),
    CONSTRAINT fk_representative_downstream_station
        FOREIGN KEY (station_code) REFERENCES downstream_station(station_code)
        ON UPDATE CASCADE
) DEFAULT CHARSET=utf8mb4;
