CREATE TABLE strategies (
    strategy_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    market_type TEXT NOT NULL DEFAULT 'COIN_M_PERP' CHECK (market_type = 'COIN_M_PERP'),
    mode TEXT NOT NULL CHECK (mode IN ('neutral', 'long', 'short')),
    spacing_mode TEXT NOT NULL CHECK (spacing_mode IN ('arithmetic', 'geometric')),
    initial_position_contracts INTEGER NOT NULL DEFAULT 0 CHECK (typeof(initial_position_contracts) = 'integer'),
    client_id_namespace TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('CREATED', 'RECOVERING', 'RUNNING', 'PAUSED', 'STOPPING', 'STOPPED', 'BLOCKED', 'ERROR')),
    config_revision INTEGER NOT NULL DEFAULT 1 CHECK (typeof(config_revision) = 'integer' AND config_revision > 0),
    created_at_ms INTEGER NOT NULL CHECK (typeof(created_at_ms) = 'integer'),
    updated_at_ms INTEGER NOT NULL CHECK (typeof(updated_at_ms) = 'integer'),
    last_error TEXT,
    UNIQUE (account_id, name),
    CHECK (last_error IS NULL OR (
        instr(lower(last_error), 'listenkey') = 0 AND
        instr(lower(last_error), 'listen_key') = 0 AND
        instr(lower(last_error), 'api_secret') = 0 AND
        instr(lower(last_error), 'signature=') = 0
    ))
) STRICT;

CREATE INDEX ix_strategies_account_symbol_status ON strategies(account_id, symbol, status);

CREATE TABLE grid_generations (
    generation_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL REFERENCES strategies(strategy_id) ON DELETE RESTRICT,
    generation_no INTEGER NOT NULL CHECK (typeof(generation_no) = 'integer' AND generation_no > 0),
    lower_price TEXT NOT NULL CHECK (typeof(lower_price) = 'text' AND length(trim(lower_price)) > 0),
    upper_price TEXT NOT NULL CHECK (typeof(upper_price) = 'text' AND length(trim(upper_price)) > 0),
    logical_level_count INTEGER NOT NULL CHECK (typeof(logical_level_count) = 'integer' AND logical_level_count > 0),
    strategy_mode TEXT NOT NULL CHECK (strategy_mode IN ('neutral', 'long', 'short')),
    spacing_mode TEXT NOT NULL CHECK (spacing_mode IN ('arithmetic', 'geometric')),
    arithmetic_step TEXT,
    geometric_ratio TEXT,
    order_contracts INTEGER NOT NULL CHECK (typeof(order_contracts) = 'integer' AND order_contracts > 0),
    max_active_orders INTEGER NOT NULL CHECK (typeof(max_active_orders) = 'integer' AND max_active_orders > 0),
    status TEXT NOT NULL CHECK (status IN ('draft', 'preparing', 'active', 'draining', 'retired', 'failed')),
    change_reason TEXT,
    created_at_ms INTEGER NOT NULL CHECK (typeof(created_at_ms) = 'integer'),
    activated_at_ms INTEGER,
    retired_at_ms INTEGER,
    UNIQUE (strategy_id, generation_no),
    CHECK (
        (spacing_mode = 'arithmetic' AND arithmetic_step IS NOT NULL AND geometric_ratio IS NULL) OR
        (spacing_mode = 'geometric' AND geometric_ratio IS NOT NULL AND arithmetic_step IS NULL)
    ),
    CHECK (max_active_orders <= logical_level_count)
) STRICT;

CREATE INDEX ix_grid_generations_strategy_status ON grid_generations(strategy_id, status);

CREATE TABLE grid_levels (
    level_id TEXT PRIMARY KEY,
    generation_id TEXT NOT NULL REFERENCES grid_generations(generation_id) ON DELETE RESTRICT,
    level_index INTEGER NOT NULL CHECK (typeof(level_index) = 'integer' AND level_index >= 0),
    price TEXT NOT NULL CHECK (typeof(price) = 'text' AND length(trim(price)) > 0),
    planned_contracts INTEGER NOT NULL CHECK (typeof(planned_contracts) = 'integer' AND planned_contracts > 0),
    state TEXT NOT NULL CHECK (state IN ('dormant', 'armed', 'active', 'blocked', 'retired')),
    cycle_no INTEGER NOT NULL DEFAULT 0 CHECK (typeof(cycle_no) = 'integer' AND cycle_no >= 0),
    version INTEGER NOT NULL DEFAULT 0 CHECK (typeof(version) = 'integer' AND version >= 0),
    updated_at_ms INTEGER NOT NULL CHECK (typeof(updated_at_ms) = 'integer'),
    UNIQUE (generation_id, level_index),
    UNIQUE (generation_id, price)
) STRICT;

CREATE INDEX ix_grid_levels_generation_state ON grid_levels(generation_id, state);

CREATE TABLE recovery_checkpoints (
    checkpoint_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    strategy_id TEXT REFERENCES strategies(strategy_id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL,
    symbol TEXT,
    reason TEXT NOT NULL CHECK (reason IN ('STARTUP', 'WS_RECONNECT', 'PERIODIC', 'MANUAL')),
    status TEXT NOT NULL CHECK (status IN ('STARTED', 'SNAPSHOT_COMPLETE', 'REPLAY_COMPLETE', 'RECONCILED', 'COMPLETE', 'FAILED', 'BLOCKED')),
    rest_server_time_ms INTEGER,
    open_orders_observed_at_ms INTEGER,
    position_observed_at_ms INTEGER,
    margin_observed_at_ms INTEGER,
    fills_from_ms INTEGER,
    fills_through_ms INTEGER,
    last_binance_trade_id TEXT,
    ws_buffer_from_ms INTEGER,
    ws_buffer_through_ms INTEGER,
    orders_seen INTEGER NOT NULL DEFAULT 0 CHECK (typeof(orders_seen) = 'integer' AND orders_seen >= 0),
    fills_seen INTEGER NOT NULL DEFAULT 0 CHECK (typeof(fills_seen) = 'integer' AND fills_seen >= 0),
    mismatch_count INTEGER NOT NULL DEFAULT 0 CHECK (typeof(mismatch_count) = 'integer' AND mismatch_count >= 0),
    started_at_ms INTEGER NOT NULL CHECK (typeof(started_at_ms) = 'integer'),
    completed_at_ms INTEGER,
    error TEXT,
    CHECK (error IS NULL OR (
        instr(lower(error), 'listenkey') = 0 AND
        instr(lower(error), 'listen_key') = 0 AND
        instr(lower(error), 'api_secret') = 0 AND
        instr(lower(error), 'signature=') = 0
    ))
) STRICT;

CREATE INDEX ix_recovery_checkpoints_strategy_completed ON recovery_checkpoints(strategy_id, completed_at_ms DESC);
CREATE INDEX ix_recovery_checkpoints_status_started ON recovery_checkpoints(status, started_at_ms);

CREATE TABLE orders (
    local_order_id TEXT PRIMARY KEY,
    strategy_id TEXT REFERENCES strategies(strategy_id) ON DELETE RESTRICT,
    generation_id TEXT REFERENCES grid_generations(generation_id) ON DELETE RESTRICT,
    level_id TEXT REFERENCES grid_levels(level_id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    logical_slot_key TEXT NOT NULL CHECK (length(trim(logical_slot_key)) > 0),
    cycle_no INTEGER NOT NULL DEFAULT 0 CHECK (typeof(cycle_no) = 'integer' AND cycle_no >= 0),
    leg_role TEXT NOT NULL CHECK (length(trim(leg_role)) > 0),
    attempt_no INTEGER NOT NULL DEFAULT 1 CHECK (typeof(attempt_no) = 'integer' AND attempt_no > 0),
    ownership TEXT NOT NULL DEFAULT 'BOT' CHECK (ownership IN ('BOT', 'EXTERNAL', 'UNCLAIMED')),
    intent TEXT NOT NULL CHECK (intent IN ('GRID_BUY', 'GRID_SELL', 'INITIAL_POSITION', 'CLOSE_POSITION', 'REBALANCE', 'RECOVERY_CANCEL')),
    client_order_id TEXT NOT NULL UNIQUE CHECK (
        length(client_order_id) BETWEEN 1 AND 36 AND
        client_order_id NOT GLOB '*[^A-Za-z0-9._:/-]*'
    ),
    exchange_order_id TEXT,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    position_side TEXT NOT NULL DEFAULT 'both' CHECK (position_side IN ('both', 'long', 'short')),
    order_type TEXT NOT NULL DEFAULT 'LIMIT',
    time_in_force TEXT,
    reduce_only INTEGER NOT NULL DEFAULT 0 CHECK (reduce_only IN (0, 1)),
    price TEXT NOT NULL CHECK (typeof(price) = 'text' AND length(trim(price)) > 0),
    quantity_contracts INTEGER NOT NULL CHECK (typeof(quantity_contracts) = 'integer' AND quantity_contracts > 0),
    local_state TEXT NOT NULL CHECK (local_state IN ('planned', 'submitting', 'ack_unknown', 'active', 'cancel_pending', 'terminal', 'blocked')),
    exchange_status TEXT NOT NULL DEFAULT 'unknown' CHECK (exchange_status IN ('unknown', 'new', 'partially_filled', 'filled', 'canceled', 'rejected', 'expired', 'expired_in_match')),
    cumulative_filled_contracts INTEGER NOT NULL DEFAULT 0 CHECK (
        typeof(cumulative_filled_contracts) = 'integer' AND
        cumulative_filled_contracts >= 0 AND
        cumulative_filled_contracts <= quantity_contracts
    ),
    avg_fill_price TEXT,
    exchange_update_ms INTEGER,
    submitted_at_ms INTEGER,
    terminal_at_ms INTEGER,
    last_source TEXT,
    version INTEGER NOT NULL DEFAULT 0 CHECK (typeof(version) = 'integer' AND version >= 0),
    created_at_ms INTEGER NOT NULL CHECK (typeof(created_at_ms) = 'integer'),
    updated_at_ms INTEGER NOT NULL CHECK (typeof(updated_at_ms) = 'integer'),
    UNIQUE (account_id, symbol, exchange_order_id),
    UNIQUE (level_id, cycle_no, intent, attempt_no),
    CHECK (
        ownership <> 'BOT' OR (
            strategy_id IS NOT NULL AND
            generation_id IS NOT NULL AND
            level_id IS NOT NULL
        )
    ),
    CHECK (
        (local_state IN ('planned', 'submitting', 'ack_unknown') AND
            exchange_status = 'unknown' AND cumulative_filled_contracts = 0) OR
        (local_state IN ('active', 'cancel_pending') AND
            exchange_status IN ('new', 'partially_filled')) OR
        (local_state = 'blocked' AND
            exchange_status IN ('unknown', 'new', 'partially_filled')) OR
        (local_state = 'terminal' AND
            exchange_status IN ('filled', 'canceled', 'rejected', 'expired', 'expired_in_match'))
    ),
    CHECK (
        (exchange_status IN ('unknown', 'new') AND cumulative_filled_contracts = 0) OR
        (exchange_status = 'partially_filled' AND
            cumulative_filled_contracts > 0 AND
            cumulative_filled_contracts < quantity_contracts) OR
        (exchange_status = 'filled' AND cumulative_filled_contracts = quantity_contracts) OR
        exchange_status IN ('canceled', 'rejected', 'expired', 'expired_in_match')
    )
) STRICT;

CREATE INDEX ix_orders_strategy_exchange_status ON orders(strategy_id, exchange_status);
CREATE INDEX ix_orders_symbol_exchange_status ON orders(account_id, symbol, exchange_status);
CREATE INDEX ix_orders_level_cycle ON orders(level_id, cycle_no);

CREATE TABLE events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL CHECK (source IN ('BINANCE_WS', 'BINANCE_REST', 'LOCAL', 'OPERATOR')),
    dedupe_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    aggregate_type TEXT,
    aggregate_id TEXT,
    strategy_id TEXT REFERENCES strategies(strategy_id) ON DELETE RESTRICT,
    order_id TEXT REFERENCES orders(local_order_id) ON DELETE RESTRICT,
    exchange_event_ms INTEGER,
    received_at_ms INTEGER NOT NULL CHECK (typeof(received_at_ms) = 'integer'),
    payload_json TEXT NOT NULL CHECK (
        instr(lower(payload_json), 'listenkey') = 0 AND
        instr(lower(payload_json), 'listen_key') = 0 AND
        instr(lower(payload_json), 'api_secret') = 0 AND
        instr(lower(payload_json), 'api-key') = 0 AND
        instr(lower(payload_json), 'apikey') = 0 AND
        instr(lower(payload_json), 'signature=') = 0 AND
        instr(lower(payload_json), '"secret"') = 0
    ),
    payload_hash TEXT NOT NULL,
    processing_state TEXT NOT NULL DEFAULT 'RECEIVED' CHECK (processing_state IN ('RECEIVED', 'APPLIED', 'IGNORED', 'ERROR')),
    processed_at_ms INTEGER,
    correlation_id TEXT,
    causation_event_id INTEGER REFERENCES events(event_id) ON DELETE RESTRICT,
    error TEXT,
    UNIQUE (source, dedupe_key),
    CHECK (error IS NULL OR (
        instr(lower(error), 'listenkey') = 0 AND
        instr(lower(error), 'listen_key') = 0 AND
        instr(lower(error), 'api_secret') = 0 AND
        instr(lower(error), 'signature=') = 0
    ))
) STRICT;

CREATE INDEX ix_events_processing_event ON events(processing_state, event_id);
CREATE INDEX ix_events_aggregate_event ON events(aggregate_type, aggregate_id, event_id);
CREATE INDEX ix_events_exchange_time ON events(exchange_event_ms);

CREATE TABLE fills (
    fill_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL REFERENCES orders(local_order_id) ON DELETE RESTRICT,
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    binance_trade_id TEXT NOT NULL,
    exchange_order_id TEXT,
    client_order_id TEXT,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    price TEXT NOT NULL CHECK (typeof(price) = 'text' AND length(trim(price)) > 0),
    fill_contracts INTEGER NOT NULL CHECK (typeof(fill_contracts) = 'integer' AND fill_contracts > 0),
    commission TEXT,
    commission_asset TEXT,
    realized_pnl TEXT,
    is_maker INTEGER CHECK (is_maker IS NULL OR is_maker IN (0, 1)),
    trade_time_ms INTEGER NOT NULL CHECK (typeof(trade_time_ms) = 'integer'),
    source TEXT NOT NULL CHECK (source IN ('USER_STREAM', 'REST_BACKFILL')),
    event_id INTEGER REFERENCES events(event_id) ON DELETE RESTRICT,
    created_at_ms INTEGER NOT NULL CHECK (typeof(created_at_ms) = 'integer'),
    UNIQUE (account_id, symbol, binance_trade_id)
) STRICT;

CREATE INDEX ix_fills_order_trade_time ON fills(order_id, trade_time_ms);
CREATE INDEX ix_fills_account_symbol_trade_time ON fills(account_id, symbol, trade_time_ms);

CREATE TABLE positions (
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    position_side TEXT NOT NULL CHECK (position_side IN ('both', 'long', 'short')),
    quantity_contracts INTEGER NOT NULL CHECK (typeof(quantity_contracts) = 'integer'),
    entry_price TEXT,
    break_even_price TEXT,
    mark_price TEXT,
    unrealized_pnl TEXT,
    leverage INTEGER,
    margin_type TEXT,
    margin_asset TEXT,
    isolated INTEGER CHECK (isolated IS NULL OR isolated IN (0, 1)),
    liquidation_price TEXT,
    exchange_update_ms INTEGER NOT NULL CHECK (typeof(exchange_update_ms) = 'integer'),
    observed_at_ms INTEGER NOT NULL CHECK (typeof(observed_at_ms) = 'integer'),
    checkpoint_id INTEGER REFERENCES recovery_checkpoints(checkpoint_id) ON DELETE RESTRICT,
    source TEXT NOT NULL CHECK (source IN ('REST', 'USER_STREAM')),
    PRIMARY KEY (account_id, symbol, position_side)
) WITHOUT ROWID, STRICT;
