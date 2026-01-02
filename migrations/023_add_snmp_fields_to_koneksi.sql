-- Add SNMP-specific fields to koneksi table
-- These fields store the default community and version for SNMP connections
-- Individual parameters can override these values in snmp_config table

ALTER TABLE koneksi ADD COLUMN community VARCHAR(100);
ALTER TABLE koneksi ADD COLUMN version VARCHAR(10);

-- Update existing SNMP connections to have default values
-- Note: This migration doesn't populate existing data as we don't have a reliable source
-- Users will need to update their SNMP connections manually or via the UI

