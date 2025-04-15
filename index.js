const express = require('express');
const cors = require('cors');
require('dotenv').config();
const mariadb = require('mariadb');
const { RDS } = require('@aws-sdk/client-rds');

const app = express();
app.use(cors());
app.use(express.json());

// Database configuration - fetched from environment variables
const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER, // IAM role name or specific DB user enabled for IAM auth
  database: process.env.DB_NAME,
  port: parseInt(process.env.DB_PORT, 10) || 3306,
  connectionLimit: 5,
  // ssl: { // uncomment and configure if SSL connection is required/enforced
  //   rejectUnauthorized: true,
  //   // ca: fs.readFileSync('/path/to/rds-ca-cert.pem') // path to your CA certificate
  // }
};

// Function to get IAM database auth token
async function getAuthToken() {
  const rdsClient = new RDS({
    region: process.env.AWS_REGION, // Ensure AWS_REGION is set in your environment
  });
  
  try {
    const token = await rdsClient.generateAuthenticationToken({
      hostname: dbConfig.host,
      port: dbConfig.port,
      username: dbConfig.user,
    });
    
    return token;
  } catch (err) {
    if (err.name === 'CredentialsProviderError' || 
        err.name === 'AccessDenied' || 
        err.message.includes('credentials') || 
        err.message.includes('permission') ||
        err.message.includes('access denied')) {
      console.error("PERMISSION DENIED: EC2 instance doesn't have appropriate IAM role for RDS access", err);
      throw new Error("Permission denied: EC2 instance doesn't have appropriate IAM role for RDS access");
    }
    throw err;
  }
}

// Create a function to get a database connection with a fresh auth token
async function getConnection() {
  try {
    // Get a fresh auth token
    const token = await getAuthToken();
    
    // Create a connection with the token as password
    return await mariadb.createConnection({
      ...dbConfig,
      password: token,
    });
  } catch (err) {
    // Check if this is an auth token error (likely permissions)
    if (err.message && err.message.includes("Permission denied")) {
      console.error("Database connection failed:", err.message);
      throw err; // Rethrow the permission error with clear message
    }
    
    // For MySQL/MariaDB specific permission errors
    if (err.code === 'ER_ACCESS_DENIED_ERROR') {
      console.error("PERMISSION DENIED: Database access denied. Check IAM permissions.");
      throw new Error("Permission denied: Database access denied. Check EC2 IAM role permissions.");
    }
    
    console.error("Error getting database connection:", err);
    throw err;
  }
}

// For connection pooling, we'll need to manage connections manually
let activeConnections = [];

// Function to get a connection from our manual pool
async function getPoolConnection() {
  // Reuse an existing connection if available
  if (activeConnections.length > 0) {
    const conn = activeConnections.pop();
    // Check if connection is still valid
    if (conn && !conn.isValid()) {
      try { conn.end(); } catch (e) { /* ignore close error */ }
      return getPoolConnection(); // Try again with a new connection
    }
    return conn;
  }
  
  // Create a new connection
  return await getConnection();
}

// Function to release a connection back to our pool
function releaseConnection(conn) {
  if (conn && conn.isValid()) {
    // Only keep connections up to our limit
    if (activeConnections.length < (dbConfig.connectionLimit || 5)) {
      activeConnections.push(conn);
    } else {
      try { conn.end(); } catch (e) { /* ignore close error */ }
    }
  }
}

// Test DB connection on startup
getConnection()
  .then(conn => {
    console.log("Successfully connected to the database.");
    try { conn.end(); } catch (e) { /* ignore close error */ }
  })
  .catch(err => {
    console.error("Error connecting to database:", err);
    // Potentially exit the application if DB connection is critical
    // process.exit(1);
  });

// --- Removed hardcoded students array ---

app.get('/health', async (req, res) => {
  try {
    // Try to establish a database connection as part of health check
    const conn = await getConnection();
    try { await conn.ping(); } finally { 
      try { conn.end(); } catch(e) { /* ignore */ } 
    }
    res.send("I am OK. Database connection successful.");
  } catch (err) {
    if (err.message && err.message.includes("Permission denied")) {
      // Return a 403 status code for permission issues
      return res.status(403).send({
        status: "ERROR",
        message: "Permission denied: EC2 instance doesn't have appropriate IAM role for RDS access"
      });
    }
    res.status(500).send({
      status: "ERROR", 
      message: "Health check failed: " + err.message
    });
  }
});

app.get('/students', async (req, res) => {
  let conn;
  try {
    conn = await getConnection();
    // Assuming a table named 'students' with columns 'id', 'name', 'email'
    const rows = await conn.query("SELECT id, name, email FROM students");
    res.send(rows);
  } catch (err) {
    console.error("Error fetching students:", err);
    
    // Check for permission denied errors
    if (err.message && (
        err.message.includes("Permission denied") || 
        err.message.includes("access denied") ||
        err.code === 'ER_ACCESS_DENIED_ERROR'
    )) {
      return res.status(403).send({ 
        error: "Permission denied: EC2 instance doesn't have appropriate IAM role for RDS access" 
      });
    }
    
    res.status(500).send({ error: "Failed to retrieve student data." });
  } finally {
    if (conn) {
      try { conn.end(); } catch (e) { /* ignore close error */ }
    }
  }
});

const port = process.env.PORT || 5005;
app.listen(port, () => console.log(`Listening on ${port}`));

// Update shutdown handlers to close any active connections
process.on('SIGTERM', () => {
  console.log('SIGTERM signal received: closing DB connections');
  Promise.all(activeConnections.map(conn => {
    try { return conn.end(); } catch (e) { return Promise.resolve(); }
  })).then(() => {
    console.log('DB connections closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('SIGINT signal received: closing DB connections');
  Promise.all(activeConnections.map(conn => {
    try { return conn.end(); } catch (e) { return Promise.resolve(); }
  })).then(() => {
    console.log('DB connections closed');
    process.exit(0);
  });
});