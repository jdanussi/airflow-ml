CREATE TABLE IF NOT EXISTS agg_dep_delay_by_date (
    id TEXT PRIMARY KEY,
	origin TEXT NOT NULL,
	fl_date TEXT NOT NULL,
	mean_dep_delay DECIMAL(8,2),
	anomaly_if NUMERIC(1, 0) DEFAULT 0,
	anomaly_arima NUMERIC(1, 0) DEFAULT 0,
	last_update TIMESTAMP NOT NULL DEFAULT now()
);