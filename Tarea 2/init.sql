-- init.sql
-- Database initialization script for traffic analysis system

-- Create database and user (if not exists)
-- Note: This assumes the database and user are already created by Docker

-- Create traffic incidents table
CREATE TABLE IF NOT EXISTS traffic_incidents (
    id SERIAL PRIMARY KEY,
    incident_id VARCHAR(255) UNIQUE NOT NULL,
    incident_type VARCHAR(100) NOT NULL,
    description TEXT,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    street VARCHAR(255),
    city VARCHAR(100),
    comuna VARCHAR(100),
    severity INTEGER DEFAULT 0,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    source VARCHAR(50) DEFAULT 'waze',
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_incident_timestamp ON traffic_incidents(timestamp);
CREATE INDEX IF NOT EXISTS idx_incident_comuna ON traffic_incidents(comuna);
CREATE INDEX IF NOT EXISTS idx_incident_type ON traffic_incidents(incident_type);
CREATE INDEX IF NOT EXISTS idx_incident_processed ON traffic_incidents(processed);
CREATE INDEX IF NOT EXISTS idx_incident_location ON traffic_incidents(latitude, longitude);

-- Create processed incidents summary table
CREATE TABLE IF NOT EXISTS incident_summaries (
    id SERIAL PRIMARY KEY,
    summary_date DATE NOT NULL,
    comuna VARCHAR(100),
    incident_type VARCHAR(100),
    total_incidents INTEGER DEFAULT 0,
    avg_severity FLOAT DEFAULT 0,
    max_severity INTEGER DEFAULT 0,
    peak_hour INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for summaries
CREATE INDEX IF NOT EXISTS idx_summary_date ON incident_summaries(summary_date);
CREATE INDEX IF NOT EXISTS idx_summary_comuna ON incident_summaries(comuna);
CREATE INDEX IF NOT EXISTS idx_summary_type ON incident_summaries(incident_type);

-- Create hotspots table for geographic analysis
CREATE TABLE IF NOT EXISTS traffic_hotspots (
    id SERIAL PRIMARY KEY,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    incident_count INTEGER DEFAULT 0,
    average_severity FLOAT DEFAULT 0,
    dominant_type VARCHAR(100),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create spatial index for hotspots
CREATE INDEX IF NOT EXISTS idx_hotspot_location ON traffic_hotspots(latitude, longitude);

-- Create processing logs table
CREATE TABLE IF NOT EXISTS processing_logs (
    id SERIAL PRIMARY KEY,
    process_type VARCHAR(50) NOT NULL,
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    end_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR(20) DEFAULT 'running',
    records_processed INTEGER DEFAULT 0,
    errors_count INTEGER DEFAULT 0,
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create cache performance table
CREATE TABLE IF NOT EXISTS cache_performance (
    id SERIAL PRIMARY KEY,
    cache_key VARCHAR(255) NOT NULL,
    hit_count INTEGER DEFAULT 0,
    miss_count INTEGER DEFAULT 0,
    last_access TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Insert initial data for testing (optional)
INSERT INTO traffic_incidents (
    incident_id, incident_type, description, latitude, longitude, 
    street, city, comuna, severity, timestamp, source
) VALUES 
    ('test_001', 'ACCIDENTE', 'Accidente de tránsito en Providencia', -33.4372, -70.6506, 'Av. Providencia', 'Santiago', 'Providencia', 5, NOW() - INTERVAL '1 hour', 'waze'),
    ('test_002', 'ATASCO', 'Congestión vehicular en Las Condes', -33.4025, -70.5693, 'Av. Las Condes', 'Santiago', 'Las Condes', 3, NOW() - INTERVAL '30 minutes', 'waze'),
    ('test_003', 'CONSTRUCCION', 'Trabajo en construcción', -33.4488, -70.6693, 'Av. Libertador', 'Santiago', 'Santiago', 2, NOW() - INTERVAL '2 hours', 'waze')
ON CONFLICT (incident_id) DO NOTHING;

-- Create functions for automated processing
CREATE OR REPLACE FUNCTION update_incident_timestamp()
RETURNS TRIGGER AS $
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$ LANGUAGE plpgsql;

-- Create trigger for automatic timestamp updates
DROP TRIGGER IF EXISTS update_incident_timestamp_trigger ON traffic_incidents;
CREATE TRIGGER update_incident_timestamp_trigger
    BEFORE UPDATE ON traffic_incidents
    FOR EACH ROW
    EXECUTE FUNCTION update_incident_timestamp();

-- Create view for incident analytics
CREATE OR REPLACE VIEW incident_analytics AS
SELECT 
    comuna,
    incident_type,
    COUNT(*) as total_incidents,
    AVG(severity) as avg_severity,
    MAX(severity) as max_severity,
    MIN(timestamp) as first_incident,
    MAX(timestamp) as last_incident,
    DATE_TRUNC('hour', timestamp) as incident_hour
FROM traffic_incidents 
WHERE processed = TRUE
GROUP BY comuna, incident_type, DATE_TRUNC('hour', timestamp);

-- Create view for hotspot analysis
CREATE OR REPLACE VIEW hotspot_analysis AS
SELECT 
    ROUND(latitude::numeric, 3) as lat_rounded,
    ROUND(longitude::numeric, 3) as lon_rounded,
    COUNT(*) as incident_count,
    AVG(severity) as avg_severity,
    array_agg(DISTINCT incident_type) as incident_types,
    array_agg(DISTINCT comuna) as comunas
FROM traffic_incidents 
WHERE processed = TRUE
GROUP BY ROUND(latitude::numeric, 3), ROUND(longitude::numeric, 3)
HAVING COUNT(*) > 5
ORDER BY incident_count DESC;

-- Create view for temporal analysis
CREATE OR REPLACE VIEW temporal_analysis AS
SELECT 
    EXTRACT(hour FROM timestamp) as hour_of_day,
    EXTRACT(dow FROM timestamp) as day_of_week,
    COUNT(*) as incident_count,
    AVG(severity) as avg_severity,
    array_agg(DISTINCT incident_type) as common_types
FROM traffic_incidents 
WHERE processed = TRUE
GROUP BY EXTRACT(hour FROM timestamp), EXTRACT(dow FROM timestamp)
ORDER BY hour_of_day, day_of_week;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO traffic_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO traffic_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO traffic_user;