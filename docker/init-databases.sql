-- Safe to run repeatedly, including against an existing Postgres data directory.
SELECT 'CREATE DATABASE horse_stock'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'horse_stock')\gexec

SELECT 'CREATE DATABASE horse_orders'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'horse_orders')\gexec

SELECT 'CREATE DATABASE horse_saga'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'horse_saga')\gexec
