// cache.js
const redis = require('redis');

// Create Redis client
let redisClient = null;
let redisAvailable = false;

// Initialize Redis client with better error handling
async function initRedisClient() {
    try {
        // Using Redis v4 client with promises
        redisClient = redis.createClient({
            url: process.env.REDIS_URL || 'redis://localhost:6379',
            socket: {
                connectTimeout: 5000, // Timeout after 5 seconds
                reconnectStrategy: (retries) => {
                    if (retries > 3) {
                        console.log('Failed to connect to Redis after multiple retries, using in-memory cache.');
                        return new Error('Redis connection failed after multiple retries');
                    }
                    return Math.min(retries * 100, 3000); // Increase delay between retries
                }
            }
        });
        
        redisClient.on('error', (err) => {
            console.warn('Redis connection error:', err.message);
            redisAvailable = false;
        });
        
        redisClient.on('connect', () => {
            console.log('Redis client connected');
            redisAvailable = true;
        });
        
        await redisClient.connect();
        redisAvailable = true;
        return redisClient;
    } catch (err) {
        console.warn('Failed to connect to Redis:', err.message);
        console.log('Using in-memory cache instead.');
        redisAvailable = false;
        return null;
    }
}

class CacheManager {
    constructor(maxSize = 100) {
        this.cache = new Map(); // Local cache as fallback or primary
        this.maxSize = maxSize;
        this.accessOrder = []; // For FCFS and LRU
        this.roundRobinIndex = 0; // For Round-Robin
        this.useRedis = false; // Start with in-memory as default
        
        // Try to initialize Redis but don't wait for it
        initRedisClient()
            .then(client => {
                if (client && client.isReady) {
                    this.useRedis = true;
                    console.log('CacheManager now using Redis');
                }
            })
            .catch(() => {
                console.log('CacheManager using in-memory storage only');
            });
    }
    
    // Generate a cache key from a query and connection params
    generateKey(query, connectionParams) {
        const normalizedQuery = query
          .trim()
          .replace(/;+$/, '')       // removes trailing semicolons
          .replace(/\s+/g, ' ')     // collapses all whitespace to single spaces
          .toLowerCase();           // optional but helps with consistency
      
        const connectionStr = `${connectionParams.account}-${connectionParams.database}-${connectionParams.schema}`;
        return `snowflake:${connectionStr}:${normalizedQuery}`;
      }
      
    
    // Check if Redis is available
    isRedisReady() {
        return this.useRedis && redisClient && redisClient.isReady && redisAvailable;
    }
    
    // Get data from cache
    async get(key, algorithm = 'fcfs') {
        if (this.isRedisReady()) {
            try {
                // Get from Redis
                const allKeys = await redisClient.keys('snowflake:*');
                console.log("[DEBUG] Available Redis keys:", allKeys);
                console.log("[DEBUG] Looking for key:", key);



                const data = await redisClient.get(key);
                if (!data) return null;
                
                // Update access metadata for algorithms
                await redisClient.hIncrBy(`${key}:meta`, 'accessCount', 1);
                await redisClient.hSet(`${key}:meta`, 'lastAccessed', Date.now());
                
                // For LRU/FCFS: Update access order
                if (algorithm === 'fcfs' || algorithm === 'lru') {
                    await redisClient.zAdd('accessOrder', { score: Date.now(), value: key });
                }
                
                return JSON.parse(data);
            } catch (err) {
                console.warn('Redis error in get:', err.message);
                // Fall back to in-memory cache
            }
        }
        
        // In-memory cache fallback
        if (!this.cache.has(key)) return null;
        
        const entry = this.cache.get(key);
        
        if (this.isExpired(entry)) {
            this.cache.delete(key);
            this.removeFromAccessOrder(key);
            return null;
        }
        
        // Update access metadata
        entry.accessCount++;
        entry.lastAccessed = Date.now();
        
        // For FCFS/LRU: Move to end of access order (most recently used)
        if (algorithm === 'fcfs' || algorithm === 'lru') {
            this.removeFromAccessOrder(key);
            this.accessOrder.push(key);
        }
        
        return entry.data;
    }
    
    // Store data in cache
    async set(key, data, ttl, algorithm = 'fcfs') {
        if (this.isRedisReady()) {
            try {
                // If cache is full, evict based on algorithm
                const cacheSize = await redisClient.dbSize();
                if (cacheSize >= this.maxSize) {
                    await this.evict(algorithm);
                }
                
                // Store in Redis with TTL
                await redisClient.setEx(key, ttl, JSON.stringify(data));
                
                // Store metadata
                await redisClient.hSet(`${key}:meta`, 
                    'timestamp', Date.now(),
                    'accessCount', 0,
                    'lastAccessed', Date.now()
                );
                console.log(`[CACHE] Stored in Redis: ${key}`);

                
                // For FCFS/LRU tracking
                if (algorithm === 'fcfs' || algorithm === 'lru') {
                    await redisClient.zAdd('accessOrder', { score: Date.now(), value: key });
                }
                
                return true;
            } catch (err) {
                console.warn('Redis error in set:', err.message);
                // Fall back to in-memory cache
            }
        }
        
        // In-memory cache fallback
        // If cache is full, evict based on algorithm
        if (this.cache.size >= this.maxSize) {
            this.evict(algorithm);
        }
        
        const entry = {
            key,
            data,
            timestamp: Date.now(),
            ttl,
            accessCount: 0,
            lastAccessed: Date.now()
        };
        
        this.cache.set(key, entry);
        
        // For FCFS tracking
        if (algorithm === 'fcfs' || algorithm === 'lru') {
            this.accessOrder.push(key);
        }
        
        return true;
    }
    
    // Evict entries based on algorithm
    async evict(algorithm) {
        if (this.isRedisReady()) {
            try {
                if (algorithm === 'fcfs' || algorithm === 'lru') {
                    // Get oldest entry from sorted set
                    const oldestEntries = await redisClient.zRange('accessOrder', 0, 0);
                    if (oldestEntries.length > 0) {
                        const oldestKey = oldestEntries[0];
                        await redisClient.del(oldestKey);
                        await redisClient.del(`${oldestKey}:meta`);
                        await redisClient.zRem('accessOrder', oldestKey);
                    }
                } else if (algorithm === 'roundrobin') {
                    // Round-Robin: Get all keys and remove one by index
                    const keys = await redisClient.keys('snowflake:*');
                    const filteredKeys = keys.filter(k => !k.endsWith(':meta'));
                    
                    if (filteredKeys.length > 0) {
                        // Get index from Redis or initialize it
                        let roundRobinIndex = parseInt(await redisClient.get('roundRobinIndex') || '0');
                        const keyToRemove = filteredKeys[roundRobinIndex % filteredKeys.length];
                        
                        await redisClient.del(keyToRemove);
                        await redisClient.del(`${keyToRemove}:meta`);
                        await redisClient.zRem('accessOrder', keyToRemove);
                        
                        // Update the index
                        roundRobinIndex++;
                        await redisClient.set('roundRobinIndex', roundRobinIndex);
                    }
                }
                return;
            } catch (err) {
                console.warn('Redis error in evict:', err.message);
                // Fall back to in-memory eviction
            }
        }
        
        // In-memory fallback
        if (algorithm === 'fcfs' || algorithm === 'lru') {
            // First-Come, First-Served or LRU: Remove oldest entry
            if (this.accessOrder.length > 0) {
                const oldestKey = this.accessOrder.shift();
                this.cache.delete(oldestKey);
            }
        } else if (algorithm === 'roundrobin') {
            // Round-Robin: Remove entries in rotation
            const keys = Array.from(this.cache.keys());
            if (keys.length > 0) {
                const keyToRemove = keys[this.roundRobinIndex % keys.length];
                this.cache.delete(keyToRemove);
                this.removeFromAccessOrder(keyToRemove);
                this.roundRobinIndex++;
            }
        }
    }
    
    // Check if entry is expired (for in-memory cache)
    isExpired(entry) {
        const now = Date.now();
        return (now - entry.timestamp) > (entry.ttl * 1000);
    }
    
    // Remove key from access order array (for in-memory cache)
    removeFromAccessOrder(key) {
        const index = this.accessOrder.indexOf(key);
        if (index !== -1) {
            this.accessOrder.splice(index, 1);
        }
    }
    
    // Clear expired entries
    async cleanExpired() {
        if (this.isRedisReady()) {
            // Redis handles TTL expiration automatically
            return;
        }
        
        // In-memory fallback
        for (const [key, entry] of this.cache.entries()) {
            if (this.isExpired(entry)) {
                this.cache.delete(key);
                this.removeFromAccessOrder(key);
            }
        }
    }
    
    // Get cache stats
    async getStats() {
        if (this.isRedisReady()) {
            try {
                const keys = await redisClient.keys('snowflake:*');
                const filteredKeys = keys.filter(k => !k.endsWith(':meta'));
                
                return {
                    size: filteredKeys.length,
                    maxSize: this.maxSize,
                    provider: 'Redis',
                    keys: filteredKeys.slice(0, 20), // Limit to first 20 keys
                    keysCount: filteredKeys.length
                };
            } catch (err) {
                console.warn('Redis error in getStats:', err.message);
                // Fall back to in-memory stats
            }
        }
        
        // In-memory fallback
        return {
            size: this.cache.size,
            maxSize: this.maxSize,
            provider: 'In-Memory',
            keys: Array.from(this.cache.keys()).slice(0, 20), // Limit to first 20 keys
            keysCount: this.cache.size
        };
    }
    
    // Clear the cache
    async clear() {
        if (this.isRedisReady()) {
            try {
                await redisClient.flushDb();
                return true;
            } catch (err) {
                console.warn('Redis error in clear:', err.message);
                // Fall back to in-memory clear
            }
        }
        
        // In-memory fallback
        this.cache.clear();
        this.accessOrder = [];
        return true;
    }
}

module.exports = new CacheManager();