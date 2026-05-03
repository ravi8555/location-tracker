import pg from 'pg';
const { Pool } = pg;
import 'dotenv/config';

class DatabasePool {
    constructor() {
        this.pool = null;
        this.isConnected = false;
    }

    initialize() {
        if (this.pool) {
            console.log('Database pool already initialized');
            return this.pool;
        }

        this.pool = new Pool({
            user: process.env.DB_USER || 'admin',
            password: process.env.DB_PASSWORD || 'admin',
            host: process.env.DB_HOST || 'localhost',
            port: parseInt(process.env.DB_PORT || '5432'),
            database: process.env.DB_NAME || 'location_tracker',
            
            // Connection pool settings
            max: parseInt(process.env.DB_POOL_MAX || '20'),
            min: parseInt(process.env.DB_POOL_MIN || '2'),
            idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT || '30000'),
            connectionTimeoutMillis: parseInt(process.env.DB_CONNECTION_TIMEOUT || '5000'),
            
            // Statement timeout (10 seconds)
            statementTimeout: parseInt(process.env.DB_STATEMENT_TIMEOUT || '10000'),
            
            // Enable SSL if needed
            ssl: process.env.DB_SSL === 'true' ? {
                rejectUnauthorized: process.env.DB_SSL_REJECT_UNAUTHORIZED !== 'false'
            } : false
        });

        // Handle pool errors
        this.pool.on('error', (err) => {
            console.error('Unexpected database pool error:', err);
            this.isConnected = false;
        });

        this.pool.on('connect', () => {
            console.log('New database client connected');
        });

        this.pool.on('remove', () => {
            console.log('Database client removed from pool');
        });

        this.isConnected = true;
        console.log(`Database pool initialized for ${process.env.DB_NAME}`);
        
        return this.pool;
    }

    async getConnection() {
        if (!this.pool) {
            this.initialize();
        }
        
        try {
            const client = await this.pool.connect();
            return client;
        } catch (error) {
            console.error('Failed to get database connection:', error);
            throw error;
        }
    }

    async query(text, params) {
        const start = Date.now();
        try {
            const result = await this.pool.query(text, params);
            const duration = Date.now() - start;
            
            if (duration > 1000) {
                console.warn(`Slow query (${duration}ms):`, text);
            }
            
            return result;
        } catch (error) {
            console.error('Database query error:', error);
            throw error;
        }
    }

    async transaction(callback) {
        const client = await this.getConnection();
        
        try {
            await client.query('BEGIN');
            const result = await callback(client);
            await client.query('COMMIT');
            return result;
        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }
    }

    async healthCheck() {
        try {
            const result = await this.query('SELECT 1 as healthy');
            return result.rows[0]?.healthy === 1;
        } catch (error) {
            console.error('Database health check failed:', error);
            return false;
        }
    }

    async close() {
        if (this.pool) {
            await this.pool.end();
            this.pool = null;
            this.isConnected = false;
            console.log('Database pool closed');
        }
    }
}

// Singleton instance
export const dbPool = new DatabasePool();

// Initialize on import
dbPool.initialize();