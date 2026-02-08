-- ETL run and step logging for observability.
CREATE TABLE IF NOT EXISTS stg.etl_run_log (
    run_id INTEGER PRIMARY KEY,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    status VARCHAR(20),
    git_sha VARCHAR(40)
);

CREATE TABLE IF NOT EXISTS stg.etl_step_log (
    run_id INTEGER,
    step_name VARCHAR(512),
    file_name VARCHAR(256),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    status VARCHAR(20),
    rows_affected INTEGER,
    notes VARCHAR(1024)
);
