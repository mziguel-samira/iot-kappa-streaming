-- Initialisation de la base de donnees IoT PostgreSQL

-- Table 1 : Mesures brutes de tous les capteurs
CREATE TABLE IF NOT EXISTS sensor_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    sensor_id VARCHAR(50),
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    status VARCHAR(20)
);

-- Table 2 : Journal des alertes et anomalies (2 niveaux + ML)
CREATE TABLE IF NOT EXISTS sensor_alerts (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP,
    sensor_id VARCHAR(50),
    temperature DOUBLE PRECISION,
    alert_level VARCHAR(20),
    message TEXT
);

-- Table 3 : Agregations par fenetre temporelle de 5 minutes (Statistiques completes)
CREATE TABLE IF NOT EXISTS sensor_windowed_metrics (
    id SERIAL PRIMARY KEY,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    sensor_id VARCHAR(50),
    average_temp DOUBLE PRECISION,
    min_temp DOUBLE PRECISION,
    max_temp DOUBLE PRECISION,
    stddev_temp DOUBLE PRECISION,
    average_humid DOUBLE PRECISION,
    average_pressure DOUBLE PRECISION,
    reading_count BIGINT,
    alerte VARCHAR(10)
);

-- Index pour optimiser les requetes SQL et Grafana
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON sensor_metrics(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_sensor ON sensor_metrics(sensor_id);
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON sensor_alerts(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_windowed_timestamp ON sensor_windowed_metrics(window_start DESC);
CREATE INDEX IF NOT EXISTS idx_windowed_sensor ON sensor_windowed_metrics(sensor_id);
