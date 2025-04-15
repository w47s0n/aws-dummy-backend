const express = require('express');
const cors = require('cors');
require('dotenv').config();
const mariadb = require('mariadb');
const AWS = require('aws-sdk');

const app = express();
app.use(cors());
app.use(express.json());

// Database configuration - fetched from environment variables
const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER, // IAM role name or specific DB user enabled for IAM auth
  database: process.env.DB_NAME,
  port: process.env.DB_PORT || 3306,
  connectionLimit: 5,
  // ssl: { // uncomment and configure if SSL connection is required/enforced
  //   rejectUnauthorized: true,
  //   // ca: fs.readFileSync('/path/to/rds-ca-cert.pem') // path to your CA certificate
  // }
};

// Function to get IAM database auth token
async function getAuthToken() {
  const signer = new AWS.RDS.Signer({
    region: process.env.AWS_REGION, // Ensure AWS_REGION is set in your environment
    hostname: dbConfig.host,
    port: dbConfig.port,
    username: dbConfig.user,
  });

  // AWS SDK will automatically use credentials from the EC2 instance profile
  return new Promise((resolve, reject) => {
    signer.getAuthToken({}, (err, token) => {
      if (err) {
        return reject(err);
      }
      resolve(token);
    });
  });
}


// Create MariaDB connection pool
const pool = mariadb.createPool({
  ...dbConfig,
  // Use a function for password to dynamically generate the token
  password: () => getAuthToken(),
  // Important for IAM auth: ensure connection is reset before reuse
  // to prevent using expired tokens. Adjust idleTimeout accordingly.
  idleTimeout: 60000 // e.g., 1 minute idle timeout
});

// Test DB connection on startup (optional but recommended)
pool.getConnection()
  .then(conn => {
    console.log("Successfully connected to the database.");
    conn.release(); // release connection back to the pool
  })
  .catch(err => {
    console.error("Error connecting to database:", err);
    // Potentially exit the application if DB connection is critical
    // process.exit(1);
  });

// --- Removed hardcoded students array ---

app.get('/health', async (req, res) => {
  res.send("I am OK.");
});

app.get('/students', async (req, res) => {
  let conn;
  try {
    conn = await pool.getConnection();
    // Assuming a table named 'students' with columns 'id', 'name', 'email'
    const rows = await conn.query("SELECT id, name, email FROM students");
    res.send(rows);
  } catch (err) {
    console.error("Error fetching students:", err);
    res.status(500).send({ error: "Failed to retrieve student data." });
  } finally {
    if (conn) conn.release(); // Ensure connection is always released
  }
});

const port = process.env.PORT || 5005;
app.listen(port, () => console.log(`Listening on ${port}`));

// Graceful shutdown: Close the pool when the app terminates
process.on('SIGTERM', () => {
  console.log('SIGTERM signal received: closing DB pool');
  pool.end(() => {
    console.log('DB pool closed');
    process.exit(0);
  });
});

process.on('SIGINT', () => {
  console.log('SIGINT signal received: closing DB pool');
  pool.end(() => {
    console.log('DB pool closed');
    process.exit(0);
  });
});