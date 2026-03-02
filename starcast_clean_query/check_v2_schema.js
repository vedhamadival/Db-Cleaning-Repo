const { Client } = require('pg');

const client = new Client({
  user: "ved",
  host: "localhost",
  database: "v2starcast",
  password: "yourpassword",
  port: 5432,
});

async function checkSchema() {
  try {
    await client.connect();
    console.log("Connected to v2starcast");

    for (const table of ['User', 'Talent']) {
      console.log(`\n--- Schema for table "${table}" ---`);
      const res = await client.query(`
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '${table}'
      `);
      
      const timestamps = res.rows.filter(r => r.data_type.includes('timestamp') || r.data_type.includes('date'));
      console.log("Timestamp/Date columns:", timestamps.map(r => `${r.column_name} (${r.data_type})`));
    }

  } catch (err) {
    console.error(err);
  } finally {
    await client.end();
  }
}

checkSchema();
