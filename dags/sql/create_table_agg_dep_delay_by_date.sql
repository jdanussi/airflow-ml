CREATE TABLE IF NOT EXISTS agg_dep_delay_by_date (
    id TEXT PRIMARY KEY,
	origin TEXT NOT NULL,
	fl_date TEXT NOT NULL,
	mean_dep_delay DECIMAL(8,2),
	anomaly NUMERIC,
	last_update TIMESTAMP NOT NULL DEFAULT now()
);