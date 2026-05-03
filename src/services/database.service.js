import { dbPool } from '../db/pool.js';

class DatabaseService {
    async ensureUserExists(userId, email, name) {
        const result = await dbPool.query(
            `INSERT INTO tracking.users (id, email, name)
             VALUES ($1, $2, $3)
             ON CONFLICT (id) DO UPDATE 
             SET email = EXCLUDED.email, 
                 name = COALESCE(EXCLUDED.name, tracking.users.name),
                 last_active_at = CURRENT_TIMESTAMP
             RETURNING *`,
            [userId, email, name]
        );
        return result.rows[0];
    }

    async upsertUser(userId, email, name) {
        return dbPool.query(
            `INSERT INTO tracking.users (id, email, name, last_active_at)
             VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
             ON CONFLICT (id) DO UPDATE 
             SET email = EXCLUDED.email,
                 name = COALESCE(EXCLUDED.name, tracking.users.name),
                 last_active_at = CURRENT_TIMESTAMP
             RETURNING *`,
            [userId, email, name]
        );
    }

    // async saveLocation(userId, email, name, locationData) {
    //     const { latitude, longitude, accuracy, altitude, speed, heading, timestamp } = locationData;
        
    //     // First ensure user exists
    //     await this.ensureUserExists(userId, email, name);
        
    //     // Insert location history
    //     const result = await dbPool.query(
    //         `INSERT INTO tracking.location_history 
    //          (user_id, latitude, longitude, accuracy, altitude, speed, heading, timestamp)
    //          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    //          RETURNING id`,
    //         [userId, latitude, longitude, accuracy, altitude, speed, heading, timestamp || new Date()]
    //     );
        
    //     // Cleanup old records (keep only last 10000 per user)
    //     await dbPool.query(
    //         `DELETE FROM tracking.location_history 
    //          WHERE user_id = $1 AND id NOT IN (
    //              SELECT id FROM tracking.location_history 
    //              WHERE user_id = $1 
    //              ORDER BY timestamp DESC 
    //              LIMIT 10000
    //          )`,
    //         [userId]
    //     );
        
    //     return result.rows[0];
    // }

    async saveLocation(userId, email, name, locationData) {
    const { latitude, longitude, accuracy, altitude, speed, heading, location_timestamp } = locationData;
    
    await this.ensureUserExists(userId, email, name);
    
    const result = await dbPool.query(
        `INSERT INTO tracking.location_history 
         (user_id, latitude, longitude, accuracy, altitude, speed, heading, location_timestamp)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING id`,
        [userId, latitude, longitude, accuracy, altitude, speed, heading, location_timestamp || new Date()]
    );
    
    // Cleanup old records
    await dbPool.query(
        `DELETE FROM tracking.location_history 
         WHERE user_id = $1 AND id NOT IN (
             SELECT id FROM tracking.location_history 
             WHERE user_id = $1 
             ORDER BY location_timestamp DESC 
             LIMIT 10000
         )`,
        [userId]
    );
    
    return result.rows[0];
}

    async startSession(userId, socketId) {
        const result = await dbPool.query(
            `INSERT INTO tracking.user_sessions (user_id, socket_id, started_at, is_active)
             VALUES ($1, $2, CURRENT_TIMESTAMP, true)
             RETURNING id`,
            [userId, socketId]
        );
        
        // End any other active sessions for this user
        await dbPool.query(
            `UPDATE tracking.user_sessions 
             SET ended_at = CURRENT_TIMESTAMP, is_active = false
             WHERE user_id = $1 AND id != $2 AND is_active = true`,
            [userId, result.rows[0].id]
        );
        
        return result.rows[0];
    }

    async endSession(socketId) {
        const result = await dbPool.query(
            `UPDATE tracking.user_sessions 
             SET ended_at = CURRENT_TIMESTAMP, is_active = false
             WHERE socket_id = $1 AND is_active = true
             RETURNING user_id`,
            [socketId]
        );
        return result.rows[0];
    }

    async getActiveUsers() {
        const result = await dbPool.query(
            `SELECT 
                u.id as user_id,
                u.email,
                u.name,
                u.last_active_at,
                json_build_object(
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
             ORDER BY u.name NULLS LAST`
        );
        return result.rows;
    }

    async getUserLocationHistory(userId, limit = 100, offset = 0) {
        const result = await dbPool.query(
            `SELECT 
                latitude, longitude, accuracy, altitude, 
                speed, heading, timestamp, created_at
             FROM tracking.location_history
             WHERE user_id = $1
             ORDER BY timestamp DESC
             LIMIT $2 OFFSET $3`,
            [userId, limit, offset]
        );
        return result.rows;
    }

    async getUserLastLocation(userId) {
        const result = await dbPool.query(
            `SELECT latitude, longitude, timestamp
             FROM tracking.location_history
             WHERE user_id = $1
             ORDER BY timestamp DESC
             LIMIT 1`,
            [userId]
        );
        return result.rows[0] || null;
    }

    async getActiveUserSessions() {
        const result = await dbPool.query(
            `SELECT 
                us.id as session_id,
                us.user_id,
                u.email,
                u.name,
                us.socket_id,
                us.started_at,
                EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - us.started_at)) as session_duration_seconds
             FROM tracking.user_sessions us
             JOIN tracking.users u ON u.id = us.user_id
             WHERE us.is_active = true`
        );
        return result.rows;
    }

    async cleanupOldSessions() {
        // End sessions older than 24 hours that are still marked as active
        const result = await dbPool.query(
            `UPDATE tracking.user_sessions 
             SET ended_at = started_at + INTERVAL '24 hours', 
                 is_active = false
             WHERE is_active = true 
               AND started_at < CURRENT_TIMESTAMP - INTERVAL '24 hours'
             RETURNING id`
        );
        return result.rowCount;
    }

    async getStatistics() {
        const result = await dbPool.query(
            `SELECT 
                (SELECT COUNT(*) FROM tracking.users) as total_users,
                (SELECT COUNT(*) FROM tracking.user_sessions WHERE is_active = true) as active_sessions,
                (SELECT COUNT(*) FROM tracking.location_history) as total_location_records,
                (SELECT COUNT(*) FROM tracking.location_history WHERE timestamp > CURRENT_TIMESTAMP - INTERVAL '1 hour') as last_hour_updates,
                (SELECT AVG(EXTRACT(EPOCH FROM (ended_at - started_at))) 
                 FROM tracking.user_sessions 
                 WHERE ended_at IS NOT NULL) as avg_session_duration_seconds
             FROM (SELECT 1) as dummy`
        );
        return result.rows[0];
    }
}

export const databaseService = new DatabaseService();