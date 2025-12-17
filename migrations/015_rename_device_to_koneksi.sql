-- Migration: Rename table device to koneksi
-- This migration renames the device table to koneksi

-- SQLite doesn't support ALTER TABLE RENAME directly, so we need to:
-- 1. Create new table with new name
-- 2. Copy data from old table
-- 3. Drop old table
-- 4. Recreate indexes and foreign keys

-- Step 1: Create new table koneksi with same structure as device
CREATE TABLE IF NOT EXISTS "koneksi" (
    `id` integer PRIMARY KEY AUTOINCREMENT,
    `nama` text NOT NULL,
    `type` text NOT NULL,
    `enabled` numeric DEFAULT true,
    `description` text,
    `port` text,
    `baud` integer,
    `data_bits` integer,
    `parity` text,
    `stop_bits` integer,
    `ip_address` text,
    `tcp_port` integer,
    `slave_id` integer,
    `polling_ms` integer DEFAULT 1000,
    `timeout_ms` integer DEFAULT 1000,
    `retry_count` integer DEFAULT 3,
    `created_at` datetime,
    `updated_at` datetime
);

-- Step 2: Copy data from device to koneksi
INSERT INTO "koneksi" SELECT * FROM "device";

-- Step 3: Update foreign key in parameters table
-- SQLite doesn't support ALTER TABLE DROP CONSTRAINT, so we need to recreate the table
-- But we'll handle this in the application layer with GORM migrations

-- Step 4: Drop old table
DROP TABLE IF EXISTS "device";

-- Step 5: Create index
CREATE INDEX IF NOT EXISTS `idx_koneksi_type` ON `koneksi`(`type`);
CREATE INDEX IF NOT EXISTS `idx_koneksi_enabled` ON `koneksi`(`enabled`);

