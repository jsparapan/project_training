-- ==========================================================================
-- init.sql — run this once against your Aurora Postgres cluster
-- to bootstrap the orders schema.
--
-- The Lambda handler also calls CREATE TABLE IF NOT EXISTS on cold start,
-- so this file is mainly for inspection, manual admin, and adding indexes
-- or constraints that go beyond what the handler sets up.
-- ==========================================================================

-- -----------------------------------------------------------------------
-- Schema
-- -----------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS public;

-- -----------------------------------------------------------------------
-- orders table
-- -----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    order_id    VARCHAR(64)     PRIMARY KEY,
    user_id     VARCHAR(64)     NOT NULL,
    total       NUMERIC(12, 2)  NOT NULL CHECK (total > 0),
    status      VARCHAR(32)     NOT NULL DEFAULT 'placed',
    items       JSONB,
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  orders              IS 'One row per order event received from Kinesis';
COMMENT ON COLUMN orders.order_id     IS 'Client-generated unique order identifier';
COMMENT ON COLUMN orders.user_id      IS 'The customer who placed the order';
COMMENT ON COLUMN orders.total        IS 'Order total in the account currency';
COMMENT ON COLUMN orders.status       IS 'placed | confirmed | shipped | cancelled';
COMMENT ON COLUMN orders.items        IS 'Array of {sku, qty, price} objects';
COMMENT ON COLUMN orders.created_at   IS 'When the order was first received';
COMMENT ON COLUMN orders.updated_at   IS 'Last time the row was modified';

-- -----------------------------------------------------------------------
-- Indexes
-- -----------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_orders_user_id
    ON orders (user_id);

CREATE INDEX IF NOT EXISTS idx_orders_status
    ON orders (status);

CREATE INDEX IF NOT EXISTS idx_orders_created_at
    ON orders (created_at DESC);

-- Useful for filtering orders by status per user
CREATE INDEX IF NOT EXISTS idx_orders_user_status
    ON orders (user_id, status);

-- -----------------------------------------------------------------------
-- Auto-update updated_at on every row change
-- -----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_orders_updated_at ON orders;
CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- -----------------------------------------------------------------------
-- Handy views for Phase 3 (Glue will read from these)
-- -----------------------------------------------------------------------
CREATE OR REPLACE VIEW orders_daily_summary AS
SELECT
    DATE(created_at)  AS order_date,
    status,
    COUNT(*)          AS total_orders,
    SUM(total)        AS revenue
FROM orders
GROUP BY DATE(created_at), status
ORDER BY order_date DESC, status;

COMMENT ON VIEW orders_daily_summary IS 'Daily order counts and revenue by status — used by Glue export job';