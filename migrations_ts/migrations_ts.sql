-- #TODO: Create new TS hypertable
-- 
-- depends: 

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS sensor_data ( id integer, velocity float, temperature float, humidity float, battery_level float NOT NULL, last_seen timestamptz NOT NULL);
SELECT create_hypertable('sensor_data', by_range('last_seen'));