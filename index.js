const express = require('express');
const cors = require('cors');
require('dotenv').config();
const mariadb = require('mariadb');
const { Signer } = require('@aws-sdk/rds-signer');

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
  try {
    // Debug info
    console.log("AWS Region:", process.env.AWS_REGION);
    console.log("DB Host:", dbConfig.host);
    console.log("DB Port:", dbConfig.port);
    console.log("DB User:", dbConfig.user);
    
    // Try to get current AWS identity
    const { STSClient, GetCallerIdentityCommand } = require("@aws-sdk/client-sts");
    const stsClient = new STSClient({ region: process.env.AWS_REGION });
    try {
      const identityData = await stsClient.send(new GetCallerIdentityCommand({}));
      console.log("Current AWS Identity:", JSON.stringify(identityData));
    } catch (stsErr) {
      console.error("Failed to get AWS identity:", stsErr);
    }
    
    // Create a Signer instance with the correct package
    const signer = new Signer({
      region: process.env.AWS_REGION,
      hostname: dbConfig.host,
      port: dbConfig.port,
      username: dbConfig.user,
    });
    
    // Get the auth token
    console.log("Requesting auth token...");
    const token = await signer.getAuthToken();
    console.log("Auth token obtained successfully (token not printed for security)");
    return token;
  } catch (err) {
    console.error("Auth token error details:", JSON.stringify(err, null, 2));
    // Check for permission-related errors
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

// Test function to try standard auth if IAM auth fails
async function tryStandardAuth() {
  if (!process.env.DB_PASSWORD) {
    console.log("DB_PASSWORD not set, skipping standard auth test");
    return;
  }
  
  console.log("Attempting standard auth as fallback test...");
  try {
    const conn = await mariadb.createConnection({
      host: dbConfig.host,
      user: dbConfig.user,
      password: process.env.DB_PASSWORD,
      database: dbConfig.database,
      port: dbConfig.port
    });
    
    await conn.query("SELECT 1 as test");
    console.log("STANDARD AUTH SUCCEEDED - this confirms the database exists and is reachable");
    conn.end();
  } catch (err) {
    console.error("STANDARD AUTH FAILED:", err);
    console.log("This suggests either network/security group issues or incorrect credentials");
  }
}

// Create a function to get a database connection with a fresh auth token
async function getConnection() {
  try {
    // First check if IAM auth is enabled
    console.log("Checking for IAM auth...");
    
    // Get a fresh auth token
    const token = await getAuthToken();
    console.log("Creating connection with token...");
    
    // Create a connection with the token as password
    const conn = await mariadb.createConnection({
      ...dbConfig,
      password: token,
      connectTimeout: 10000, // Increase timeout for debugging
    });
    
    console.log("IAM AUTH CONNECTION SUCCESSFUL!");
    return conn;
  } catch (err) {
    console.error("Detailed connection error:", JSON.stringify(err, null, 2));
    
    // Try standard auth as a diagnostic step
    await tryStandardAuth();
    
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

// Function to initialize database tables and sample data
async function initializeDatabase() {
  let conn;
  try {
    console.log("Initializing database...");
    conn = await getConnection();
    
    // Check if students table exists
    const tables = await conn.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = ? 
      AND table_name = 'students'
    `, [dbConfig.database]);
    
    // Create students table if it doesn't exist
    if (tables.length === 0) {
      console.log("Creating students table...");
      await conn.query(`
        CREATE TABLE students (
          id INT AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(100) NOT NULL,
          email VARCHAR(100) NOT NULL UNIQUE
        )
      `);
      
      // Insert sample data
      console.log("Inserting sample data...");
      await conn.query(`
        INSERT INTO students (name, email) VALUES 
        ('John Doe', 'john@example.com'),
        ('Jane Smith', 'jane@example.com'),
        ('Bob Johnson', 'bob@example.com'),
        ('Alice Williams', 'alice@example.com'),
        ('Charlie Brown', 'charlie@example.com')
      `);
      
      console.log("Database initialization completed successfully.");
    } else {
      console.log("Students table already exists, skipping initialization.");
    }
  } catch (err) {
    console.error("Database initialization failed:", err);
    throw err;
  } finally {
    if (conn) {
      try { conn.end(); } catch (e) { /* ignore close error */ }
    }
  }
}

// Test DB connection on startup and initialize database
console.log("=============== STARTING DATABASE CONNECTION TEST ===============");
getConnection()
  .then(async conn => {
    console.log("Successfully connected to the database.");
    try { conn.end(); } catch (e) { /* ignore close error */ }
    
    // After successful connection, initialize database
    return initializeDatabase();
  })
  .catch(err => {
    console.error("Error connecting to database:", err);
    // Potentially exit the application if DB connection is critical
    // process.exit(1);
  })
  .finally(() => {
    console.log("=============== DATABASE CONNECTION TEST COMPLETE ===============");
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