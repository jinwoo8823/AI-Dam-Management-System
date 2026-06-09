CREATE TABLE IF NOT EXISTS dam_code (
    dam_code INT NOT NULL PRIMARY KEY,
    dam_name VARCHAR(30) NOT NULL,
    latitude DOUBLE NOT NULL,
    longitude DOUBLE NOT NULL,
    grid_x INT NOT NULL,
    grid_y INT NOT NULL,
    is_multipurpose BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dam_realtime_observation (
    dam_code INT NOT NULL,
    obsrdt DATETIME NOT NULL,
    inflowqy DOUBLE NULL,
    lowlevel DOUBLE NULL,
    rf DOUBLE NULL,
    rsvwtqy DOUBLE NULL,
    rsvwtrt DOUBLE NULL,
    totdcwtrqy DOUBLE NULL,
    tmp DOUBLE NULL,
    rain DOUBLE NULL,
    snow DOUBLE NULL,
    source VARCHAR(30) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (dam_code, obsrdt),
    CONSTRAINT fk_dam_realtime_observation_dam_code
        FOREIGN KEY (dam_code) REFERENCES dam_code(dam_code)
);

CREATE TABLE IF NOT EXISTS dam_weather_forecast (
    dam_code INT NOT NULL,
    base_datetime DATETIME NOT NULL,
    forecast_datetime DATETIME NOT NULL,
    grid_x INT NOT NULL,
    grid_y INT NOT NULL,
    tmp DOUBLE NULL,
    rain DOUBLE NULL,
    snow DOUBLE NULL,
    pty VARCHAR(20) NULL,
    source VARCHAR(30) NOT NULL,
    raw_payload JSON NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (dam_code, base_datetime, forecast_datetime, source),
    CONSTRAINT fk_dam_weather_forecast_dam_code
        FOREIGN KEY (dam_code) REFERENCES dam_code(dam_code)
);

CREATE TABLE IF NOT EXISTS dam_prediction_result (
    dam_code INT NOT NULL,
    pred_base_time DATETIME NOT NULL,
    pred_target_time DATETIME NOT NULL,
    predicted_inflow_3h DOUBLE NULL,
    predicted_discharge_3h DOUBLE NULL,
    discharge_change_level VARCHAR(20) NULL,
    release_review VARCHAR(20) NULL,
    confidence_level VARCHAR(20) NULL,
    model_version VARCHAR(50) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dam_code, pred_base_time, pred_target_time),
    CONSTRAINT fk_dam_prediction_result_dam_code
        FOREIGN KEY (dam_code) REFERENCES dam_code(dam_code)
);
