-- =============================================================================
-- FAME Data Space - PostgreSQL Initialization Script
-- =============================================================================
-- This script creates the transaction database schema for Source 4

-- Create transactions table
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sender_id VARCHAR(50) NOT NULL,
    sender_name VARCHAR(200),
    sender_country CHAR(2),
    sender_iban VARCHAR(34),
    sender_bank VARCHAR(100),
    receiver_id VARCHAR(50) NOT NULL,
    receiver_name VARCHAR(200),
    receiver_country CHAR(2),
    receiver_iban VARCHAR(34),
    receiver_bank VARCHAR(100),
    amount DECIMAL(18, 2) NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'EUR',
    amount_eur DECIMAL(18, 2),
    transaction_type VARCHAR(50) NOT NULL,
    description TEXT,
    reference VARCHAR(100),
    channel VARCHAR(50),
    status VARCHAR(20) DEFAULT 'PENDING',
    is_cross_border BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    metadata JSONB
);

-- Create index for performance
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(created_at);
CREATE INDEX IF NOT EXISTS idx_transactions_sender ON transactions(sender_id);
CREATE INDEX IF NOT EXISTS idx_transactions_receiver ON transactions(receiver_id);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(status);
CREATE INDEX IF NOT EXISTS idx_transactions_type ON transactions(transaction_type);
CREATE INDEX IF NOT EXISTS idx_transactions_country ON transactions(sender_country, receiver_country);

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    country CHAR(2),
    iban VARCHAR(34),
    bank_name VARCHAR(100),
    customer_type VARCHAR(50) DEFAULT 'INDIVIDUAL',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- Create banks table
CREATE TABLE IF NOT EXISTS banks (
    bank_id SERIAL PRIMARY KEY,
    bank_name VARCHAR(200) NOT NULL UNIQUE,
    swift_code VARCHAR(11),
    country CHAR(2),
    is_sepa_member BOOLEAN DEFAULT TRUE
);

-- Insert sample banks
INSERT INTO banks (bank_name, swift_code, country, is_sepa_member) VALUES
    ('BNP Paribas', 'BNPAFRPP', 'FR', TRUE),
    ('Deutsche Bank', 'DEUTDEFF', 'DE', TRUE),
    ('ING Bank', 'INGBNL2A', 'NL', TRUE),
    ('Santander', 'BSCHESMM', 'ES', TRUE),
    ('UniCredit', 'UNCRITMM', 'IT', TRUE),
    ('KBC Bank', 'KREDBEBB', 'BE', TRUE),
    ('Erste Bank', 'GIBAATWW', 'AT', TRUE),
    ('Millennium BCP', 'BCOMPTPL', 'PT', TRUE),
    ('Barclays', 'BARCGB22', 'GB', FALSE),
    ('JPMorgan Chase', 'CHASUS33', 'US', FALSE)
ON CONFLICT (bank_name) DO NOTHING;

-- Create exchange rates table for FX conversion
CREATE TABLE IF NOT EXISTS exchange_rates (
    rate_id SERIAL PRIMARY KEY,
    base_currency CHAR(3) DEFAULT 'EUR',
    target_currency CHAR(3) NOT NULL,
    rate DECIMAL(18, 6) NOT NULL,
    reference_date DATE NOT NULL,
    source VARCHAR(50) DEFAULT 'ECB',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(base_currency, target_currency, reference_date)
);

-- Insert sample exchange rates
INSERT INTO exchange_rates (base_currency, target_currency, rate, reference_date, source) VALUES
    ('EUR', 'USD', 1.0850, CURRENT_DATE, 'ECB'),
    ('EUR', 'GBP', 0.8650, CURRENT_DATE, 'ECB'),
    ('EUR', 'CHF', 0.9450, CURRENT_DATE, 'ECB'),
    ('EUR', 'JPY', 163.50, CURRENT_DATE, 'ECB'),
    ('EUR', 'CAD', 1.4720, CURRENT_DATE, 'ECB'),
    ('EUR', 'AUD', 1.6520, CURRENT_DATE, 'ECB'),
    ('EUR', 'CNY', 7.8250, CURRENT_DATE, 'ECB'),
    ('EUR', 'INR', 90.250, CURRENT_DATE, 'ECB')
ON CONFLICT DO NOTHING;

-- Create audit log table
CREATE TABLE IF NOT EXISTS audit_log (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50) NOT NULL,
    operation VARCHAR(20) NOT NULL,
    record_id VARCHAR(100),
    old_values JSONB,
    new_values JSONB,
    user_id VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create function for transaction audit
CREATE OR REPLACE FUNCTION log_transaction_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        INSERT INTO audit_log (table_name, operation, record_id, new_values)
        VALUES ('transactions', 'INSERT', NEW.transaction_id::text, row_to_json(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO audit_log (table_name, operation, record_id, old_values, new_values)
        VALUES ('transactions', 'UPDATE', NEW.transaction_id::text, row_to_json(OLD), row_to_json(NEW));
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO audit_log (table_name, operation, record_id, old_values)
        VALUES ('transactions', 'DELETE', OLD.transaction_id::text, row_to_json(OLD));
        RETURN OLD;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for audit
DROP TRIGGER IF EXISTS transactions_audit_trigger ON transactions;
CREATE TRIGGER transactions_audit_trigger
AFTER INSERT OR UPDATE OR DELETE ON transactions
FOR EACH ROW EXECUTE FUNCTION log_transaction_changes();

-- Create view for cross-border transaction analysis
CREATE OR REPLACE VIEW v_cross_border_transactions AS
SELECT 
    t.transaction_id,
    t.sender_name,
    t.sender_country,
    t.receiver_name,
    t.receiver_country,
    t.amount,
    t.currency,
    t.amount_eur,
    t.transaction_type,
    t.status,
    t.created_at,
    sb.bank_name AS sender_bank_name,
    rb.bank_name AS receiver_bank_name
FROM transactions t
LEFT JOIN banks sb ON t.sender_bank = sb.bank_name
LEFT JOIN banks rb ON t.receiver_bank = rb.bank_name
WHERE t.sender_country != t.receiver_country;

-- Create view for transaction statistics
CREATE OR REPLACE VIEW v_transaction_stats AS
SELECT 
    DATE(created_at) AS tx_date,
    transaction_type,
    sender_country,
    COUNT(*) AS tx_count,
    SUM(amount_eur) AS total_volume_eur,
    AVG(amount_eur) AS avg_amount_eur,
    COUNT(*) FILTER (WHERE status = 'COMPLETED') AS completed_count,
    COUNT(*) FILTER (WHERE status = 'FAILED') AS failed_count
FROM transactions
GROUP BY DATE(created_at), transaction_type, sender_country;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO fame_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO fame_user;

-- Display success message
DO $$
BEGIN
    RAISE NOTICE '✅ FAME Database initialized successfully!';
    RAISE NOTICE '   Tables: transactions, customers, banks, exchange_rates, audit_log';
    RAISE NOTICE '   Views: v_cross_border_transactions, v_transaction_stats';
END $$;
