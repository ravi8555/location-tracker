-- Location Tracker Database Initialization
\c location_tracker;

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create schema for tracking data
CREATE SCHEMA IF NOT EXISTS tracking;

-- Set search path
SET search_path TO tracking, public;

-- Create users table (sync with OIDC users)
CREATE TABLE IF NOT EXISTS tracking.users (
    id VARCHAR(255) PRIMARY KEY,
    email VARCHAR(322) NOT NULL UNIQUE,
    name VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_active_at TIMESTAMP WITH TIME ZONE
);

-- Create location history table
CREATE TABLE IF NOT EXISTS tracking.location_history (
    id BIGSERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL REFERENCES tracking.users(id) ON DELETE CASCADE,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    accuracy DOUBLE PRECISION,
    altitude DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    heading DOUBLE PRECISION,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create user sessions table
CREATE TABLE IF NOT EXISTS tracking.user_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(255) NOT NULL REFERENCES tracking.users(id) ON DELETE CASCADE,
    socket_id VARCHAR(255),
    started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ended_at TIMESTAMP WITH TIME ZONE,
    is_active BOOLEAN DEFAULT TRUE
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_location_history_user_id ON tracking.location_history(user_id);
CREATE INDEX IF NOT EXISTS idx_location_history_timestamp ON tracking.location_history(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_location_history_user_timestamp ON tracking.location_history(user_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_user_sessions_user_active ON tracking.user_sessions(user_id, is_active);
CREATE INDEX IF NOT EXISTS idx_user_sessions_socket_id ON tracking.user_sessions(socket_id);
CREATE INDEX IF NOT EXISTS idx_users_email ON tracking.users(email);
CREATE INDEX IF NOT EXISTS idx_users_last_active ON tracking.users(last_active_at DESC);

-- Create function to update last_active
CREATE OR REPLACE FUNCTION tracking.update_last_active()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE tracking.users 
    SET last_active_at = NEW.timestamp 
    WHERE id = NEW.user_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger to update last_active
CREATE TRIGGER update_user_last_active
    AFTER INSERT ON tracking.location_history
    FOR EACH ROW
    EXECUTE FUNCTION tracking.update_last_active();

-- Create function to get user's last known location
CREATE OR REPLACE FUNCTION tracking.get_user_last_location(p_user_id VARCHAR(255))
RETURNS TABLE (
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timestamp TIMESTAMP WITH TIME ZONE
) AS $$
BEGIN
    RETURN QUERY
    SELECT lh.latitude, lh.longitude, lh.timestamp
    FROM tracking.location_history lh
    WHERE lh.user_id = p_user_id
    ORDER BY lh.timestamp DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Create function to get active users with their last location
CREATE OR REPLACE FUNCTION tracking.get_active_users()
RETURNS TABLE (
    user_id VARCHAR(255),
    email VARCHAR(322),
    name VARCHAR(255),
    last_location JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT DISTINCT ON (us.user_id)
        us.user_id,
        u.email,
        u.name,
        jsonb_build_object(
            'latitude', lh.latitude,
            'longitude', lh.longitude,
            'timestamp', lh.timestamp
        ) as last_location
    FROM tracking.user_sessions us
    JOIN tracking.users u ON u.id = us.user_id
    LEFT JOIN LATERAL (
        SELECT latitude, longitude, timestamp
        FROM tracking.location_history lh2
        WHERE lh2.user_id = us.user_id
        ORDER BY lh2.timestamp DESC
        LIMIT 1
    ) lh ON true
    WHERE us.is_active = true
    ORDER BY us.user_id, us.started_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Create function to cleanup old location history (keep last 30 days)
CREATE OR REPLACE FUNCTION tracking.cleanup_old_locations()
RETURNS void AS $$
BEGIN
    DELETE FROM tracking.location_history
    WHERE timestamp < CURRENT_TIMESTAMP - INTERVAL '30 days';
    
    RAISE NOTICE 'Cleaned up location records older than 30 days';
END;
$$ LANGUAGE plpgsql;

-- Create function to get location statistics
CREATE OR REPLACE FUNCTION tracking.get_location_stats()
RETURNS TABLE (
    total_users BIGINT,
    active_sessions BIGINT,
    total_locations BIGINT,
    locations_last_hour BIGINT,
    avg_locations_per_user NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        (SELECT COUNT(*) FROM tracking.users)::BIGINT,
        (SELECT COUNT(*) FROM tracking.user_sessions WHERE is_active = true)::BIGINT,
        (SELECT COUNT(*) FROM tracking.location_history)::BIGINT,
        (SELECT COUNT(*) FROM tracking.location_history WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour')::BIGINT,
        (SELECT AVG(location_count) FROM 
            (SELECT COUNT(*) as location_count 
             FROM tracking.location_history 
             GROUP BY user_id) sub)::NUMERIC(10,2);
END;
$$ LANGUAGE plpgsql;

-- Create materialized view for daily statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS tracking.daily_stats AS
SELECT 
    DATE(timestamp) as date,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(*) as total_updates,
    AVG(SPEED) FILTER (WHERE speed IS NOT NULL) as avg_speed
FROM tracking.location_history
GROUP BY DATE(timestamp);

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_stats_date ON tracking.daily_stats(date);

-- Create function to refresh materialized view
CREATE OR REPLACE FUNCTION tracking.refresh_daily_stats()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY tracking.daily_stats;
END;
$$ LANGUAGE plpgsql;

-- Create partition function for location history by month (optional for large datasets)
CREATE OR REPLACE FUNCTION tracking.create_monthly_partition(target_date DATE)
RETURNS void AS $$
DECLARE
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    start_date := DATE_TRUNC('month', target_date);
    end_date := start_date + INTERVAL '1 month';
    partition_name := 'location_history_' || TO_CHAR(start_date, 'YYYY_MM');
    
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS tracking.%I PARTITION OF tracking.location_history
        FOR VALUES FROM (%L) TO (%L)',
        partition_name, start_date, end_date
    );
END;
$$ LANGUAGE plpgsql;

-- Grant permissions (adjust as needed)
GRANT ALL PRIVILEGES ON SCHEMA tracking TO admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA tracking TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA tracking TO admin;