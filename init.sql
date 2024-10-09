
CREATE TABLE IF NOT EXISTS toot_window_counts (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    count BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS avg_toot_length (
    user_id VARCHAR NOT NULL,
    avg_toot_length FLOAT NOT NULL
);
