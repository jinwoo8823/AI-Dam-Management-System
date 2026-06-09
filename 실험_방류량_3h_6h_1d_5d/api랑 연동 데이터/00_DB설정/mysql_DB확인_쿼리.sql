USE `ai댐 프로젝트`;

-- 1. 테이블 목록 확인
SHOW TABLES;

-- 2. 20개 댐 마스터 확인
SELECT dam_code, dam_name, latitude, longitude, grid_x, grid_y
FROM dam_code
ORDER BY dam_code;

-- 3. 수문 운영 정보 저장 건수/시간 범위 확인
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT dam_code) AS dam_count,
    MIN(obsrdt) AS min_obsrdt,
    MAX(obsrdt) AS max_obsrdt
FROM dam_realtime_observation;

-- 4. 댐별 수문 운영 정보 확인
SELECT
    d.dam_name,
    COUNT(*) AS row_count,
    MIN(o.obsrdt) AS min_obsrdt,
    MAX(o.obsrdt) AS max_obsrdt
FROM dam_realtime_observation o
JOIN dam_code d ON d.dam_code = o.dam_code
GROUP BY d.dam_name
ORDER BY d.dam_name;

-- 5. 기상청 초단기 자료 저장 건수/시간 범위 확인
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT dam_code) AS dam_count,
    MIN(forecast_datetime) AS min_forecast_datetime,
    MAX(forecast_datetime) AS max_forecast_datetime
FROM dam_weather_forecast;

-- 6. 댐별 기상청 초단기 자료 확인
SELECT
    d.dam_name,
    w.source,
    COUNT(*) AS row_count,
    MIN(w.forecast_datetime) AS min_forecast_datetime,
    MAX(w.forecast_datetime) AS max_forecast_datetime
FROM dam_weather_forecast w
JOIN dam_code d ON d.dam_code = w.dam_code
GROUP BY d.dam_name, w.source
ORDER BY d.dam_name, w.source;
