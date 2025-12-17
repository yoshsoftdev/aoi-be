-- Create device table (master table)
CREATE TABLE IF NOT EXISTS device (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- Basic Information
    nama VARCHAR(100) NOT NULL,
    type VARCHAR(50) NOT NULL, -- 'modbus_rtu', 'modbus_tcp', 'snmp', 'analog', 'digital'
    enabled BOOLEAN DEFAULT 1, -- Device status: true = active polling, false = skip polling
    description VARCHAR(255),
    
    -- Modbus RTU Configuration (required if type = 'modbus_rtu')
    port VARCHAR(50), -- Serial port: '/dev/ttyUSB0', 'COM3', etc.
    baud INTEGER, -- Baud rate: 9600, 19200, 38400, etc.
    data_bits INTEGER, -- Data bits: typically 8
    parity VARCHAR(1), -- Parity: 'N' (none), 'E' (even), 'O' (odd)
    stop_bits INTEGER, -- Stop bits: typically 1 or 2
    
    -- Modbus TCP Configuration (required if type = 'modbus_tcp')
    ip_address VARCHAR(50), -- Device IP address
    tcp_port INTEGER DEFAULT 502, -- TCP port (default: 502)
    
    -- Common Modbus Settings (RTU/TCP)
    slave_id INTEGER, -- Modbus slave/unit ID
    
    -- Polling Configuration
    polling_ms INTEGER DEFAULT 1000, -- Default polling interval in milliseconds (per device)
    timeout_ms INTEGER DEFAULT 1000, -- Connection timeout in milliseconds
    retry_count INTEGER DEFAULT 3, -- Number of retry attempts on failure
    
    -- Timestamps
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_device_type ON device(type);
CREATE INDEX idx_device_enabled ON device(enabled);

