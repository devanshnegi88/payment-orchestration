-- Run after alembic upgrade head to seed reference data.
-- Alembic creates the schema; this seeds the config tables.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Gateway configuration (capabilities and fee structure)
INSERT INTO gateway_config (
    id, gateway_name, is_active, base_url,
    supports_auth_capture, supports_partial_capture, supports_partial_refund,
    supported_payment_methods, supported_currencies,
    cb_failure_threshold, cb_success_threshold, cb_timeout_seconds,
    fee_percentage, fee_fixed_paise, rate_limit_per_second, auth_timeout_seconds
) VALUES
(uuid_generate_v4(), 'razorpay', true, 'https://api.razorpay.com',
 true, true, true,
 '["CARD","UPI","NETBANKING","WALLET"]', '["INR"]',
 5, 2, 30, 2.0, 200, 200, 30),

(uuid_generate_v4(), 'stripe', true, 'https://api.stripe.com',
 true, true, true,
 '["CARD","EMI"]', '["INR","USD","GBP","EUR"]',
 5, 2, 30, 2.5, 300, 100, 30),

(uuid_generate_v4(), 'payu', true, 'https://secure.payu.in',
 false, false, true,
 '["CARD","NETBANKING","WALLET"]', '["INR"]',
 5, 2, 30, 1.8, 150, 150, 45),

(uuid_generate_v4(), 'upi', true, 'https://api.phonepe.com/apis/hermes',
 false, false, false,
 '["UPI"]', '["INR"]',
 5, 2, 30, 0.0, 0, 100, 300)
ON CONFLICT (gateway_name) DO NOTHING;

-- Default routing weights
INSERT INTO routing_config (id, config_key, config_value, description)
VALUES (
    uuid_generate_v4(),
    'routing_weights',
    '{"success":0.35,"latency":0.20,"cost":0.20,"health":0.15,"fit":0.10}',
    'Default weights per Section A3.1. Update via PUT /api/v1/routing/config'
)
ON CONFLICT (config_key) DO NOTHING;

-- Seed historical metrics so router has data from first request
-- Based on Section A3.4 performance dataset (12:00-18:00 peak window)
INSERT INTO gateway_health_metrics
    (gateway, window_start, window_end, total_requests, successful_requests,
     failed_requests, p95_latency_ms, avg_latency_ms)
VALUES
('razorpay', NOW()-INTERVAL '10 min', NOW()-INTERVAL '9 min',  533, 501, 32, 780, 520),
('razorpay', NOW()-INTERVAL '9 min',  NOW()-INTERVAL '8 min',  541, 509, 32, 760, 510),
('stripe',   NOW()-INTERVAL '10 min', NOW()-INTERVAL '9 min',  475, 463, 12, 420, 310),
('stripe',   NOW()-INTERVAL '9 min',  NOW()-INTERVAL '8 min',  483, 471, 12, 415, 305),
('payu',     NOW()-INTERVAL '10 min', NOW()-INTERVAL '9 min',  258, 230, 28, 950, 680),
('payu',     NOW()-INTERVAL '9 min',  NOW()-INTERVAL '8 min',  262, 234, 28, 940, 670),
('upi',      NOW()-INTERVAL '10 min', NOW()-INTERVAL '9 min',  633, 620, 13, 350, 260),
('upi',      NOW()-INTERVAL '9 min',  NOW()-INTERVAL '8 min',  641, 628, 13, 345, 255)
ON CONFLICT DO NOTHING;
