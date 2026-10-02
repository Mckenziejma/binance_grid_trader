ALTER TABLE recovery_checkpoints
    ADD COLUMN recovery_epoch INTEGER NOT NULL DEFAULT 0
        CHECK (typeof(recovery_epoch) = 'integer' AND recovery_epoch >= 0);

ALTER TABLE recovery_checkpoints
    ADD COLUMN snapshot_observed_at_ms INTEGER
        CHECK (snapshot_observed_at_ms IS NULL OR typeof(snapshot_observed_at_ms) = 'integer');

ALTER TABLE recovery_checkpoints
    ADD COLUMN trades_complete INTEGER NOT NULL DEFAULT 0
        CHECK (trades_complete IN (0, 1));

ALTER TABLE recovery_checkpoints
    ADD COLUMN next_trade_cursor TEXT;

ALTER TABLE recovery_checkpoints
    ADD COLUMN pagination_watermark TEXT;

ALTER TABLE recovery_checkpoints
    ADD COLUMN position_mode TEXT
        CHECK (position_mode IS NULL OR position_mode IN ('one_way', 'hedge'));

ALTER TABLE recovery_checkpoints
    ADD COLUMN rules_hash TEXT;

CREATE TABLE instrument_rules (
    instrument_rule_id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    pair TEXT NOT NULL,
    contract_type TEXT NOT NULL,
    status TEXT NOT NULL,
    contract_size TEXT NOT NULL,
    margin_asset TEXT NOT NULL,
    tick_size TEXT NOT NULL,
    quantity_step INTEGER NOT NULL
        CHECK (typeof(quantity_step) = 'integer' AND quantity_step > 0),
    min_qty INTEGER NOT NULL
        CHECK (typeof(min_qty) = 'integer' AND min_qty > 0),
    max_qty INTEGER
        CHECK (max_qty IS NULL OR (typeof(max_qty) = 'integer' AND max_qty >= min_qty)),
    min_price TEXT,
    max_price TEXT,
    supported_order_types_json TEXT NOT NULL,
    observed_at_ms INTEGER NOT NULL CHECK (typeof(observed_at_ms) = 'integer'),
    payload_hash TEXT NOT NULL,
    rules_hash TEXT NOT NULL,
    UNIQUE (symbol, rules_hash)
) STRICT;

CREATE INDEX ix_instrument_rules_symbol_observed
    ON instrument_rules(symbol, observed_at_ms DESC);

CREATE TABLE position_mode_observations (
    position_mode_observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('one_way', 'hedge')),
    observed_at_ms INTEGER NOT NULL CHECK (typeof(observed_at_ms) = 'integer'),
    checkpoint_id INTEGER REFERENCES recovery_checkpoints(checkpoint_id) ON DELETE RESTRICT,
    source TEXT NOT NULL DEFAULT 'REST' CHECK (source = 'REST'),
    UNIQUE (checkpoint_id, account_id)
) STRICT;

CREATE INDEX ix_position_mode_account_observed
    ON position_mode_observations(account_id, observed_at_ms DESC);

CREATE TABLE exchange_observations (
    exchange_observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint_id INTEGER NOT NULL
        REFERENCES recovery_checkpoints(checkpoint_id) ON DELETE RESTRICT,
    observation_type TEXT NOT NULL CHECK (observation_type IN (
        'INSTRUMENT_RULES', 'POSITION_MODE', 'OPEN_ORDERS',
        'POSITIONS', 'MARGIN_ACCOUNT', 'USER_TRADES'
    )),
    account_id TEXT NOT NULL,
    symbol TEXT,
    observed_at_ms INTEGER NOT NULL CHECK (typeof(observed_at_ms) = 'integer'),
    server_time_ms INTEGER,
    item_count INTEGER NOT NULL DEFAULT 0
        CHECK (typeof(item_count) = 'integer' AND item_count >= 0),
    complete INTEGER NOT NULL DEFAULT 1 CHECK (complete IN (0, 1)),
    next_cursor TEXT,
    pagination_watermark TEXT,
    payload_hash TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    UNIQUE (
        checkpoint_id,
        observation_type,
        symbol,
        server_time_ms,
        pagination_watermark,
        next_cursor,
        payload_hash
    )
) STRICT;

CREATE INDEX ix_exchange_observations_checkpoint_type
    ON exchange_observations(checkpoint_id, observation_type);

CREATE UNIQUE INDEX ux_exchange_observations_idempotent
    ON exchange_observations(
        checkpoint_id,
        observation_type,
        COALESCE(symbol, ''),
        COALESCE(server_time_ms, -1),
        COALESCE(pagination_watermark, ''),
        COALESCE(next_cursor, ''),
        payload_hash
    );

-- Account trade evidence that cannot (and must not) be forced into a BOT
-- order row still needs durable, idempotent storage.  ``fills`` remains the
-- strategy-owned relational ledger; this table is the complete REST fact log.
CREATE TABLE exchange_trade_observations (
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    binance_trade_id TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL,
    client_order_id TEXT,
    side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
    position_side TEXT NOT NULL CHECK (position_side IN ('both', 'long', 'short')),
    price TEXT NOT NULL CHECK (typeof(price) = 'text' AND length(trim(price)) > 0),
    fill_contracts INTEGER NOT NULL
        CHECK (typeof(fill_contracts) = 'integer' AND fill_contracts > 0),
    commission TEXT NOT NULL,
    commission_asset TEXT NOT NULL,
    realized_pnl TEXT NOT NULL,
    is_maker INTEGER CHECK (is_maker IS NULL OR is_maker IN (0, 1)),
    trade_time_ms INTEGER NOT NULL CHECK (typeof(trade_time_ms) = 'integer'),
    first_checkpoint_id INTEGER NOT NULL
        REFERENCES recovery_checkpoints(checkpoint_id) ON DELETE RESTRICT,
    last_checkpoint_id INTEGER NOT NULL
        REFERENCES recovery_checkpoints(checkpoint_id) ON DELETE RESTRICT,
    first_observed_at_ms INTEGER NOT NULL
        CHECK (typeof(first_observed_at_ms) = 'integer'),
    last_observed_at_ms INTEGER NOT NULL
        CHECK (typeof(last_observed_at_ms) = 'integer'),
    payload_hash TEXT NOT NULL,
    PRIMARY KEY (account_id, symbol, binance_trade_id)
) WITHOUT ROWID, STRICT;

CREATE INDEX ix_exchange_trade_observations_order
    ON exchange_trade_observations(account_id, symbol, exchange_order_id);
