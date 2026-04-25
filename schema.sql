CREATE TABLE IF NOT EXISTS league (
    league_id INT AUTO_INCREMENT PRIMARY KEY,
    league_code VARCHAR(16) NOT NULL UNIQUE,
    league_name VARCHAR(128) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS season (
    season_id INT AUTO_INCREMENT PRIMARY KEY,
    season_label VARCHAR(16) NOT NULL UNIQUE,
    start_year SMALLINT NOT NULL,
    end_year SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS team (
    team_id INT AUTO_INCREMENT PRIMARY KEY,
    team_name VARCHAR(128) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS player_dim (
    provider_player_id BIGINT PRIMARY KEY,
    player_name VARCHAR(128) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_player_name (player_name)
);

CREATE TABLE IF NOT EXISTS player_team_history (
    provider_player_id BIGINT NOT NULL,
    provider_team_id BIGINT NOT NULL,
    first_seen_at DATETIME NOT NULL,
    last_seen_at DATETIME NOT NULL,
    observations INT NOT NULL DEFAULT 1,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (provider_player_id, provider_team_id),
    KEY idx_player_team_history_team (provider_team_id),
    KEY idx_player_team_history_last_seen (last_seen_at),
    CONSTRAINT fk_player_team_history_player FOREIGN KEY (provider_player_id) REFERENCES player_dim(provider_player_id)
);

CREATE TABLE IF NOT EXISTS player_name_alias (
    provider_player_id BIGINT NOT NULL,
    alias_name VARCHAR(128) NOT NULL,
    source_table VARCHAR(32) NOT NULL,
    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (provider_player_id, alias_name, source_table),
    KEY idx_player_alias_name (alias_name),
    KEY idx_player_alias_player (provider_player_id),
    CONSTRAINT fk_player_alias_player FOREIGN KEY (provider_player_id) REFERENCES player_dim(provider_player_id)
);

CREATE TABLE IF NOT EXISTS ingest_batch (
    batch_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    source_file VARCHAR(255) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    row_count INT NOT NULL DEFAULT 0,
    file_sha256 CHAR(64) NOT NULL,
    UNIQUE KEY uq_ingest_file_hash (source_file, file_sha256)
);

CREATE TABLE IF NOT EXISTS raw_match_row (
    raw_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    source_row_num INT NOT NULL,
    league_code VARCHAR(16) NULL,
    season_label VARCHAR(16) NULL,
    raw_json JSON NOT NULL,
    raw_csv_hash CHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_raw_batch FOREIGN KEY (batch_id) REFERENCES ingest_batch(batch_id),
    UNIQUE KEY uq_raw_batch_row (batch_id, source_row_num),
    KEY idx_raw_league_season (league_code, season_label)
);

CREATE TABLE IF NOT EXISTS match_game (
    match_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    raw_id BIGINT NOT NULL UNIQUE,
    league_id INT NOT NULL,
    season_id INT NULL,
    match_date DATE NULL,
    kickoff_time TIME NULL,
    home_team_id INT NOT NULL,
    away_team_id INT NOT NULL,
    ft_home_goals SMALLINT NULL,
    ft_away_goals SMALLINT NULL,
    ft_result CHAR(1) NULL,
    ht_home_goals SMALLINT NULL,
    ht_away_goals SMALLINT NULL,
    ht_result CHAR(1) NULL,
    attendance INT NULL,
    referee VARCHAR(128) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_match_raw FOREIGN KEY (raw_id) REFERENCES raw_match_row(raw_id),
    CONSTRAINT fk_match_league FOREIGN KEY (league_id) REFERENCES league(league_id),
    CONSTRAINT fk_match_season FOREIGN KEY (season_id) REFERENCES season(season_id),
    CONSTRAINT fk_match_home_team FOREIGN KEY (home_team_id) REFERENCES team(team_id),
    CONSTRAINT fk_match_away_team FOREIGN KEY (away_team_id) REFERENCES team(team_id),
    KEY idx_match_league_season_date (league_id, season_id, match_date),
    KEY idx_match_home_team (home_team_id),
    KEY idx_match_away_team (away_team_id)
);

CREATE TABLE IF NOT EXISTS match_stats (
    match_id BIGINT PRIMARY KEY,
    home_shots SMALLINT NULL,
    away_shots SMALLINT NULL,
    home_shots_on_target SMALLINT NULL,
    away_shots_on_target SMALLINT NULL,
    home_fouls SMALLINT NULL,
    away_fouls SMALLINT NULL,
    home_corners SMALLINT NULL,
    away_corners SMALLINT NULL,
    home_yellow_cards SMALLINT NULL,
    away_yellow_cards SMALLINT NULL,
    home_red_cards SMALLINT NULL,
    away_red_cards SMALLINT NULL,
    home_offsides SMALLINT NULL,
    away_offsides SMALLINT NULL,
    home_hit_woodwork SMALLINT NULL,
    away_hit_woodwork SMALLINT NULL,
    home_booking_points SMALLINT NULL,
    away_booking_points SMALLINT NULL,
    home_free_kicks_conceded SMALLINT NULL,
    away_free_kicks_conceded SMALLINT NULL,
    CONSTRAINT fk_stats_match FOREIGN KEY (match_id) REFERENCES match_game(match_id)
);

CREATE TABLE IF NOT EXISTS odds_quote (
    quote_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    match_id BIGINT NOT NULL,
    bookmaker_code VARCHAR(64) NOT NULL,
    market VARCHAR(16) NOT NULL,
    period ENUM('open', 'close') NOT NULL,
    selection VARCHAR(16) NOT NULL,
    line_value DECIMAL(8,3) NULL,
    odds_value DECIMAL(8,3) NOT NULL,
    source_column VARCHAR(128) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_odds_match FOREIGN KEY (match_id) REFERENCES match_game(match_id),
    UNIQUE KEY uq_odds_quote (match_id, bookmaker_code, market, period, selection),
    KEY idx_odds_match_market_period (match_id, market, period),
    KEY idx_odds_book_market_sel (bookmaker_code, market, selection)
);

CREATE TABLE IF NOT EXISTS ingest_error (
    error_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id BIGINT NOT NULL,
    source_row_num INT NOT NULL,
    error_message VARCHAR(1024) NOT NULL,
    raw_json JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_error_batch FOREIGN KEY (batch_id) REFERENCES ingest_batch(batch_id),
    KEY idx_error_batch_row (batch_id, source_row_num)
);

CREATE TABLE IF NOT EXISTS event_ingest_state (
    state_key VARCHAR(64) PRIMARY KEY,
    state_value VARCHAR(255) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS event_api_call_log (
    call_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    endpoint VARCHAR(128) NOT NULL,
    query_params TEXT NULL,
    response_code INT NOT NULL,
    requests_remaining INT NULL,
    calls_used INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_event_api_call_created (created_at)
);

CREATE TABLE IF NOT EXISTS event_fixture (
    provider_fixture_id BIGINT PRIMARY KEY,
    league_id INT NOT NULL,
    league_name VARCHAR(128) NULL,
    season_year INT NOT NULL,
    fixture_date_utc DATETIME NULL,
    status_short VARCHAR(16) NULL,
    status_long VARCHAR(64) NULL,
    home_team_id BIGINT NULL,
    home_team_name VARCHAR(128) NULL,
    away_team_id BIGINT NULL,
    away_team_name VARCHAR(128) NULL,
    goals_home SMALLINT NULL,
    goals_away SMALLINT NULL,
    events_polled_at DATETIME NULL,
    last_events_http_code INT NULL,
    last_events_count INT NULL,
    last_events_attempt_at DATETIME NULL,
    events_attempt_count INT NOT NULL DEFAULT 0,
    next_retry_after DATETIME NULL,
    raw_json JSON NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_event_fixture_league_season_date (league_id, season_year, fixture_date_utc),
    KEY idx_event_fixture_status (status_short),
    KEY idx_event_fixture_retry (next_retry_after),
    KEY idx_event_fixture_sync_polling (
        league_id,
        season_year,
        status_short,
        events_polled_at,
        last_events_http_code,
        next_retry_after,
        fixture_date_utc
    )
);

CREATE TABLE IF NOT EXISTS event_goal (
    goal_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    provider_fixture_id BIGINT NOT NULL,
    event_hash CHAR(64) NOT NULL,
    team_id BIGINT NULL,
    team_name VARCHAR(128) NULL,
    player_id BIGINT NULL,
    player_name VARCHAR(128) NULL,
    assist_id BIGINT NULL,
    assist_name VARCHAR(128) NULL,
    elapsed_minute SMALLINT NOT NULL,
    extra_minute SMALLINT NULL,
    event_type VARCHAR(32) NULL,
    event_detail VARCHAR(64) NULL,
    comments VARCHAR(255) NULL,
    raw_json JSON NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_event_goal_hash (event_hash),
    KEY idx_event_goal_fixture (provider_fixture_id),
    KEY idx_event_goal_team (team_name),
    CONSTRAINT fk_event_goal_fixture FOREIGN KEY (provider_fixture_id) REFERENCES event_fixture(provider_fixture_id)
);

CREATE TABLE IF NOT EXISTS event_timeline (
    event_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    provider_fixture_id BIGINT NOT NULL,
    event_hash CHAR(64) NOT NULL,
    team_id BIGINT NULL,
    team_name VARCHAR(128) NULL,
    player_id BIGINT NULL,
    player_name VARCHAR(128) NULL,
    assist_id BIGINT NULL,
    assist_name VARCHAR(128) NULL,
    elapsed_minute SMALLINT NULL,
    extra_minute SMALLINT NULL,
    event_type VARCHAR(32) NULL,
    event_detail VARCHAR(64) NULL,
    comments VARCHAR(255) NULL,
    raw_json JSON NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_event_timeline_hash (event_hash),
    KEY idx_event_timeline_fixture (provider_fixture_id),
    KEY idx_event_timeline_type_detail (event_type, event_detail),
    KEY idx_event_timeline_team (team_id),
    KEY idx_event_timeline_elapsed (elapsed_minute),
    CONSTRAINT fk_event_timeline_fixture FOREIGN KEY (provider_fixture_id) REFERENCES event_fixture(provider_fixture_id)
);

CREATE TABLE IF NOT EXISTS event_fixture_match_map (
    provider_fixture_id BIGINT PRIMARY KEY,
    match_id BIGINT NULL,
    confidence_score DECIMAL(5,2) NOT NULL DEFAULT 0.00,
    match_method VARCHAR(64) NULL,
    notes VARCHAR(255) NULL,
    linked_at TIMESTAMP NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_event_match_map_match_id (match_id),
    CONSTRAINT fk_event_match_map_fixture FOREIGN KEY (provider_fixture_id) REFERENCES event_fixture(provider_fixture_id),
    CONSTRAINT fk_event_match_map_match FOREIGN KEY (match_id) REFERENCES match_game(match_id)
);

CREATE TABLE IF NOT EXISTS event_fixture_enrichment_state (
    provider_fixture_id BIGINT PRIMARY KEY,
    stats_polled_at DATETIME NULL,
    lineups_polled_at DATETIME NULL,
    last_stats_http_code INT NULL,
    last_lineups_http_code INT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_event_fixture_enrichment_stats (stats_polled_at, last_stats_http_code),
    KEY idx_event_fixture_enrichment_lineups (lineups_polled_at, last_lineups_http_code),
    CONSTRAINT fk_event_fixture_enrichment_fixture FOREIGN KEY (provider_fixture_id) REFERENCES event_fixture(provider_fixture_id)
);

CREATE TABLE IF NOT EXISTS team_badge (
    badge_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    provider_team_id BIGINT NOT NULL,
    team_name VARCHAR(128) NOT NULL,
    league_id INT NOT NULL,
    league_name VARCHAR(64) NOT NULL,
    season_year INT NOT NULL,
    badge_url VARCHAR(512) NOT NULL,
    badge_image LONGBLOB NOT NULL,
    content_type VARCHAR(128) NULL,
    image_size_bytes INT NOT NULL,
    image_sha256 CHAR(64) NOT NULL,
    downloaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_team_badge_league_season (provider_team_id, league_id, season_year),
    KEY idx_team_badge_league_season (league_id, season_year),
    KEY idx_team_badge_team (provider_team_id)
);

CREATE TABLE IF NOT EXISTS team_provider_dim (
    provider_team_id BIGINT PRIMARY KEY,
    canonical_team_name VARCHAR(128) NOT NULL,
    canonical_team_id INT NULL,
    chosen_source VARCHAR(32) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_team_provider_canonical_name (canonical_team_name),
    KEY idx_team_provider_canonical_team (canonical_team_id),
    CONSTRAINT fk_team_provider_canonical_team FOREIGN KEY (canonical_team_id) REFERENCES team(team_id)
);

CREATE TABLE IF NOT EXISTS team_name_alias (
    provider_team_id BIGINT NOT NULL,
    alias_name VARCHAR(128) NOT NULL,
    source_table VARCHAR(32) NOT NULL,
    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (provider_team_id, alias_name, source_table),
    KEY idx_team_alias_name (alias_name),
    KEY idx_team_alias_provider (provider_team_id)
);

CREATE TABLE IF NOT EXISTS backfill_task (
    task_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    day_no INT NOT NULL,
    item_type VARCHAR(32) NOT NULL,
    league_code VARCHAR(16) NOT NULL,
    league_name VARCHAR(128) NULL,
    api_league_id INT NULL,
    start_year INT NULL,
    estimated_calls INT NOT NULL DEFAULT 0,
    status ENUM('pending','in_progress','completed','skipped','blocked') NOT NULL DEFAULT 'pending',
    notes VARCHAR(512) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_backfill_task (day_no, item_type, league_code, start_year),
    KEY idx_backfill_task_status (status),
    KEY idx_backfill_task_day (day_no)
);

CREATE TABLE IF NOT EXISTS backfill_day_log (
    day_no INT PRIMARY KEY,
    planned_calls INT NOT NULL DEFAULT 0,
    actual_calls INT NULL,
    api_remaining INT NULL,
    status ENUM('pending','in_progress','completed','blocked') NOT NULL DEFAULT 'pending',
    notes VARCHAR(512) NULL,
    started_at DATETIME NULL,
    completed_at DATETIME NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE OR REPLACE VIEW v_event_timeline_normalized AS
SELECT
    et.event_id,
    et.provider_fixture_id,
    et.event_hash,
    et.team_id,
    COALESCE(tp.canonical_team_name, et.team_name) AS team_name_canonical,
    et.team_name AS team_name_raw,
    tp.canonical_team_id,
    et.player_id,
    et.player_name,
    et.assist_id,
    et.assist_name,
    et.elapsed_minute,
    et.extra_minute,
    et.event_type,
    et.event_detail,
    et.comments,
    et.raw_json,
    et.created_at,
    et.updated_at
FROM event_timeline et
LEFT JOIN team_provider_dim tp ON tp.provider_team_id = et.team_id;

CREATE OR REPLACE VIEW v_player_event_timeline AS
SELECT
    et.event_id,
    et.provider_fixture_id,
    et.team_id,
    COALESCE(tp.canonical_team_name, et.team_name) AS team_name_canonical,
    et.event_type,
    et.event_detail,
    et.elapsed_minute,
    et.extra_minute,
    et.comments,
    et.player_id,
    COALESCE(pd.player_name, et.player_name) AS player_name_canonical,
    et.player_name AS player_name_raw,
    et.assist_id,
    COALESCE(ad.player_name, et.assist_name) AS assist_name_canonical,
    et.assist_name AS assist_name_raw,
    et.created_at,
    et.updated_at
FROM event_timeline et
LEFT JOIN team_provider_dim tp ON tp.provider_team_id = et.team_id
LEFT JOIN player_dim pd ON pd.provider_player_id = et.player_id
LEFT JOIN player_dim ad ON ad.provider_player_id = et.assist_id;

-- Optional least-privilege runtime user template (manual execution only).
-- Replace EVENT_SYNC_PASSWORD before running these statements.
--
-- CREATE USER IF NOT EXISTS 'event_sync_user'@'localhost' IDENTIFIED BY 'EVENT_SYNC_PASSWORD';
-- CREATE USER IF NOT EXISTS 'event_sync_user'@'127.0.0.1' IDENTIFIED BY 'EVENT_SYNC_PASSWORD';
--
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_ingest_state TO 'event_sync_user'@'localhost';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_api_call_log TO 'event_sync_user'@'localhost';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_fixture TO 'event_sync_user'@'localhost';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_goal TO 'event_sync_user'@'localhost';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_timeline TO 'event_sync_user'@'localhost';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_fixture_match_map TO 'event_sync_user'@'localhost';
--
-- GRANT SELECT ON historic_football_data.match_game TO 'event_sync_user'@'localhost';
-- GRANT SELECT ON historic_football_data.team TO 'event_sync_user'@'localhost';
-- GRANT SELECT ON historic_football_data.league TO 'event_sync_user'@'localhost';
-- GRANT SELECT ON historic_football_data.season TO 'event_sync_user'@'localhost';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.player_dim TO 'event_sync_user'@'localhost';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.player_team_history TO 'event_sync_user'@'localhost';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.player_name_alias TO 'event_sync_user'@'localhost';
--
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_ingest_state TO 'event_sync_user'@'127.0.0.1';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_api_call_log TO 'event_sync_user'@'127.0.0.1';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_fixture TO 'event_sync_user'@'127.0.0.1';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_goal TO 'event_sync_user'@'127.0.0.1';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_timeline TO 'event_sync_user'@'127.0.0.1';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.event_fixture_match_map TO 'event_sync_user'@'127.0.0.1';
--
-- GRANT SELECT ON historic_football_data.match_game TO 'event_sync_user'@'127.0.0.1';
-- GRANT SELECT ON historic_football_data.team TO 'event_sync_user'@'127.0.0.1';
-- GRANT SELECT ON historic_football_data.league TO 'event_sync_user'@'127.0.0.1';
-- GRANT SELECT ON historic_football_data.season TO 'event_sync_user'@'127.0.0.1';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.player_dim TO 'event_sync_user'@'127.0.0.1';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.player_team_history TO 'event_sync_user'@'127.0.0.1';
-- GRANT SELECT, INSERT, UPDATE ON historic_football_data.player_name_alias TO 'event_sync_user'@'127.0.0.1';
--
-- FLUSH PRIVILEGES;
