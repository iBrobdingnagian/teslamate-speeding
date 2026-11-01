-- Hourly weather per drive, filled by weather/fetch_weather.py (Open-Meteo, no API key).
-- speeding/speeding_incidents.py reads it when SPEEDING_CORRELATE contains "weather".
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    car_id INTEGER REFERENCES cars(id),
    date TIMESTAMPTZ NOT NULL,
    latitude NUMERIC(10,6),
    longitude NUMERIC(10,6),
    temperature NUMERIC(5,2),
    feels_like NUMERIC(5,2),
    humidity INTEGER,
    pressure INTEGER,
    weather_main VARCHAR(50),
    weather_description VARCHAR(100),
    clouds INTEGER,
    visibility INTEGER,
    wind_speed NUMERIC(5,2),
    wind_deg INTEGER,
    rain_1h NUMERIC(5,2),
    snow_1h NUMERIC(5,2),
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_weather_date ON weather_data (date);
CREATE INDEX IF NOT EXISTS idx_weather_car_date ON weather_data (car_id, date);
CREATE INDEX IF NOT EXISTS idx_weather_conditions ON weather_data (weather_main);
CREATE UNIQUE INDEX IF NOT EXISTS idx_weather_unique ON weather_data (car_id, date);
