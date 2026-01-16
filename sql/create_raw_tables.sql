-- =====================================================
-- Raw tables for CartPanda Orders ingestion
-- =====================================================

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.orders (
    id SERIAL PRIMARY KEY,
    data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
