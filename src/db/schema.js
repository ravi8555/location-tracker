// -- Create database for location tracking
// CREATE DATABASE location_tracker;

// -- Connect to the new database
// \c location_tracker;

// -- Create schema for location data
// CREATE SCHEMA IF NOT EXISTS tracking;

// -- Set search path
// SET search_path TO tracking, public;

// -- Create users table (sync with OIDC users - just essential info)
// CREATE TABLE IF NOT EXISTS tracking.users (
//     id VARCHAR(255) PRIMARY KEY,
//     email VARCHAR(322) NOT NULL UNIQUE,
//     name VARCHAR(255),
//     created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
//     last_active_at TIMESTAMP WITH TIME ZONE
// );

// -- Create location history table
// CREATE TABLE IF NOT EXISTS tracking.location_history (
//     id BIGSERIAL PRIMARY KEY,
//     user_id VARCHAR(255) NOT NULL REFERENCES tracking.users(id) ON DELETE CASCADE,
//     latitude DOUBLE PRECISION NOT NULL,
//     longitude DOUBLE PRECISION NOT NULL,
//     accuracy DOUBLE PRECISION,
//     altitude DOUBLE PRECISION,
//     speed DOUBLE PRECISION,
//     heading DOUBLE PRECISION,
//     timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
//     created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
// );

// -- Create sessions table for active tracking sessions
// CREATE TABLE IF NOT EXISTS tracking.user_sessions (
//     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
//     user_id VARCHAR(255) NOT NULL REFERENCES tracking.users(id) ON DELETE CASCADE,
//     socket_id VARCHAR(255),
//     started_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
//     ended_at TIMESTAMP WITH TIME ZONE,
//     is_active BOOLEAN DEFAULT TRUE
// );

// -- Create indexes for performance
// CREATE INDEX IF NOT EXISTS idx_location_history_user_id ON tracking.location_history(user_id);
// CREATE INDEX IF NOT EXISTS idx_location_history_timestamp ON tracking.location_history(timestamp DESC);
// CREATE INDEX IF NOT EXISTS idx_location_history_user_timestamp ON tracking.location_history(user_id, timestamp DESC);
// CREATE INDEX IF NOT EXISTS idx_user_sessions_user_active ON tracking.user_sessions(user_id, is_active);
// CREATE INDEX IF NOT EXISTS idx_user_sessions_socket_id ON tracking.user_sessions(socket_id);

// -- Create function to update last_active
// CREATE OR REPLACE FUNCTION tracking.update_last_active()
// RETURNS TRIGGER AS $$
// BEGIN
//     UPDATE tracking.users 
//     SET last_active_at = NEW.timestamp 
//     WHERE id = NEW.user_id;
//     RETURN NEW;
// END;
// $$ LANGUAGE plpgsql;

// -- Create trigger to update last_active
// CREATE TRIGGER update_user_last_active
//     AFTER INSERT ON tracking.location_history
//     FOR EACH ROW
//     EXECUTE FUNCTION tracking.update_last_active();

// -- Create function to get user's last known location
// CREATE OR REPLACE FUNCTION tracking.get_user_last_location(p_user_id VARCHAR(255))
// RETURNS TABLE (
//     latitude DOUBLE PRECISION,
//     longitude DOUBLE PRECISION,
//     timestamp TIMESTAMP WITH TIME ZONE
// ) AS $$
// BEGIN
//     RETURN QUERY
//     SELECT lh.latitude, lh.longitude, lh.timestamp
//     FROM tracking.location_history lh
//     WHERE lh.user_id = p_user_id
//     ORDER BY lh.timestamp DESC
//     LIMIT 1;
// END;
// $$ LANGUAGE plpgsql;

// -- Create function to get active users
// CREATE OR REPLACE FUNCTION tracking.get_active_users()
// RETURNS TABLE (
//     user_id VARCHAR(255),
//     email VARCHAR(322),
//     name VARCHAR(255),
//     last_location JSON
// ) AS $$
// BEGIN
//     RETURN QUERY
//     SELECT DISTINCT ON (us.user_id)
//         us.user_id,
//         u.email,
//         u.name,
//         json_build_object(
//             'latitude', lh.latitude,
//             'longitude', lh.longitude,
//             'timestamp', lh.timestamp
//         ) as last_location
//     FROM tracking.user_sessions us
//     JOIN tracking.users u ON u.id = us.user_id
//     LEFT JOIN LATERAL (
//         SELECT latitude, longitude, timestamp
//         FROM tracking.location_history lh2
//         WHERE lh2.user_id = us.user_id
//         ORDER BY lh2.timestamp DESC
//         LIMIT 1
//     ) lh ON true
//     WHERE us.is_active = true
//     ORDER BY us.user_id, us.started_at DESC;
// END;
// $$ LANGUAGE plpgsql;