CREATE UNIQUE INDEX ux_grid_generations_one_active
    ON grid_generations(strategy_id)
    WHERE status = 'active';

CREATE UNIQUE INDEX ux_orders_one_unfinished_logical_slot
    ON orders(logical_slot_key)
    WHERE local_state <> 'terminal';

CREATE UNIQUE INDEX ux_orders_one_unfinished_semantic_slot
    ON orders(strategy_id, generation_id, level_id, cycle_no, leg_role)
    WHERE local_state <> 'terminal' AND ownership = 'BOT';

CREATE TABLE bot_runs (
    run_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('STARTING', 'RUNNING', 'RECOVERING', 'STOPPING', 'STOPPED', 'FAILED', 'CRASHED')),
    started_at_ms INTEGER NOT NULL CHECK (typeof(started_at_ms) = 'integer'),
    heartbeat_at_ms INTEGER NOT NULL CHECK (typeof(heartbeat_at_ms) = 'integer'),
    ended_at_ms INTEGER,
    recovery_required INTEGER NOT NULL DEFAULT 1 CHECK (recovery_required IN (0, 1)),
    error TEXT,
    CHECK (error IS NULL OR (
        instr(lower(error), 'listenkey') = 0 AND
        instr(lower(error), 'listen_key') = 0 AND
        instr(lower(error), 'api_secret') = 0 AND
        instr(lower(error), 'signature=') = 0
    ))
) STRICT;

CREATE INDEX ix_bot_runs_status_heartbeat ON bot_runs(status, heartbeat_at_ms);

CREATE TABLE strategy_leases (
    strategy_id TEXT PRIMARY KEY REFERENCES strategies(strategy_id) ON DELETE RESTRICT,
    run_id TEXT NOT NULL REFERENCES bot_runs(run_id) ON DELETE RESTRICT,
    fencing_token INTEGER NOT NULL CHECK (typeof(fencing_token) = 'integer' AND fencing_token > 0),
    acquired_at_ms INTEGER NOT NULL CHECK (typeof(acquired_at_ms) = 'integer'),
    renewed_at_ms INTEGER NOT NULL CHECK (typeof(renewed_at_ms) = 'integer'),
    expires_at_ms INTEGER NOT NULL CHECK (typeof(expires_at_ms) = 'integer' AND expires_at_ms > renewed_at_ms),
    released INTEGER NOT NULL DEFAULT 0 CHECK (released IN (0, 1)),
    UNIQUE (run_id, strategy_id)
) STRICT;

CREATE INDEX ix_strategy_leases_expiry ON strategy_leases(expires_at_ms);
