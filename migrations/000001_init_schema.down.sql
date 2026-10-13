-- Remove tabelas operacionais
DROP TABLE IF EXISTS coverage_areas CASCADE;
DROP TABLE IF EXISTS reputation_ledger CASCADE;
DROP TABLE IF EXISTS payments CASCADE;
DROP TABLE IF EXISTS outbox_events CASCADE;
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;

-- Remove tabelas de perfil
DROP TABLE IF EXISTS merchant_profiles CASCADE;
DROP TABLE IF EXISTS deliverer_profiles CASCADE;

-- Remove tabelas de autenticação (Zero Trust)
DROP TABLE IF EXISTS sessions CASCADE;
DROP TABLE IF EXISTS devices CASCADE;
DROP TABLE IF EXISTS user_login_state CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Remove Tipos (Enums)
DROP TYPE IF EXISTS outbox_status;
DROP TYPE IF EXISTS package_size;
DROP TYPE IF EXISTS order_status;
DROP TYPE IF EXISTS vehicle_type;
DROP TYPE IF EXISTS account_status;
DROP TYPE IF EXISTS user_role;

-- Remove Extensões
DROP EXTENSION IF EXISTS postgis;
DROP EXTENSION IF EXISTS "uuid-ossp";
