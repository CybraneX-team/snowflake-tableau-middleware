const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const snowflake = require('snowflake-sdk');
const path = require('path');

const app = express();
const port = process.env.PORT || 3000;

// Import cache manager with Redis support
const cacheManager = require('./cache');

// Middleware setup
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

// Serve static files from 'public' directory
app.use(express.static(path.join(__dirname, 'public')));

// Helper function to connect to Snowflake
async function connectToSnowflake(params) {
  const connection = snowflake.createConnection({
    account: params.account,
    username: params.username,
    password: params.password,
    warehouse: params.warehouse,
    database: params.database,
    schema: params.schema
  });
  
  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        reject(err);
      } else {
        resolve(conn);
      }
    });
  });
}

// Helper function to execute a query
async function executeQuery(connection, query) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: query,
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      }
    });
  });
}

// Simple health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'ok',
    cacheProvider: cacheManager.isRedisReady() ? 'Redis' : 'In-Memory'
  });
});

// API endpoint for schema
// API endpoint for schema
app.post('/api/schema', async (req, res) => {
  try {
    const connectionData = req.body;
    const query = connectionData.query.trim();

    // Log incoming query
    console.log("Schema API - Received query:", query);

    // Only add LIMIT 1 if it's a simple SELECT query without existing LIMIT
    let schemaQuery = query.trim();

// Remove trailing semicolon if exists
if (schemaQuery.endsWith(";")) {
  schemaQuery = schemaQuery.slice(0, -1);
}

// Only append LIMIT 1 if it's a plain SELECT without LIMIT
if (/^select/i.test(schemaQuery) && !/limit\s+\d+$/i.test(schemaQuery)) {
  schemaQuery = `${schemaQuery} LIMIT 1`;
}


    const connection = await connectToSnowflake(connectionData);
    const rows = await executeQuery(connection, schemaQuery);

    if (!rows || rows.length === 0) {
      return res.status(400).json({ error: 'Query returned no rows' });
    }

    // Dynamically build the schema from the first row
    const sampleRow = rows[0];
    const columns = [];

    for (const key in sampleRow) {
      let dataType = 'string';
      const value = sampleRow[key];

      if (typeof value === 'number') {
        dataType = Number.isInteger(value) ? 'int' : 'float';
      } else if (typeof value === 'boolean') {
        dataType = 'bool';
      } else if (value instanceof Date) {
        dataType = 'datetime';
      }

      columns.push({
        id: key,
        dataType: dataType
      });
    }

    const tableSchema = {
      id: 'snowflakeData',
      alias: 'Snowflake Data',
      columns: columns
    };

    res.json(tableSchema);
  } catch (error) {
    console.error('Schema API error:', error);
    res.status(500).json({ error: error.message });
  }
});


// API endpoint for data with caching
app.post('/api/data', async (req, res) => {
  try {
    const startTotal = Date.now();

    const { query, cacheTime, cacheAlgorithm } = req.body;
    const cacheKey = cacheManager.generateKey(query, req.body);

    const startCache = Date.now();
    let data = await cacheManager.get(cacheKey, cacheAlgorithm);
    const endCache = Date.now();

    if (data) {
      console.log(`[TIMING] Cache HIT: ${(endCache - startCache)} ms`);
      console.log(`[TIMING] Total response time (from cache): ${Date.now() - startTotal} ms`);
      return res.json(data);
    }

    console.log(`[TIMING] Cache MISS: ${(endCache - startCache)} ms`);

    const connection = await connectToSnowflake(req.body);
    const startSnowflake = Date.now();
    data = await executeQuery(connection, query);
    const endSnowflake = Date.now();

    console.log(`[TIMING] Snowflake query time: ${endSnowflake - startSnowflake} ms`);

    await cacheManager.set(cacheKey, data, parseInt(cacheTime), cacheAlgorithm);
    console.log(`[TIMING] Total response time (after cache set): ${Date.now() - startTotal} ms`);

    res.json(data);
  } catch (error) {
    console.error('Data API error:', error);
    res.status(500).json({ error: error.message });
  }
});


// API endpoint to view cache stats
app.get('/api/cache/stats', async (req, res) => {
  try {
    const stats = await cacheManager.getStats();
    res.json(stats);
  } catch (error) {
    console.error('Cache stats error:', error);
    res.status(500).json({ error: error.message });
  }
});

// API endpoint to clear cache
app.post('/api/cache/clear', async (req, res) => {
  try {
    await cacheManager.clear();
    res.json({ message: 'Cache cleared successfully' });
  } catch (error) {
    console.error('Cache clear error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Start the server
app.listen(port, () => {
  console.log(`Server running on port ${port}`);
  console.log(`WDC URL: http://localhost:${port}`);
  console.log(`Cache using: ${cacheManager.isRedisReady() ? 'Redis' : 'In-Memory storage'}`);
});
