const fs = require('fs');
const path = require('path');
const { Client } = require('pg');
const { stringify } = require('csv-stringify/sync');

const client = new Client({
  user: "ved",
  host: "localhost",
  database: "v2starcast",
  password: "yourpassword",
  port: 5432,
});

const OUTPUT_DIR = path.join(__dirname, 'exported_csvs');

async function exportTables() {
  try {
    await client.connect();
    console.log("Connected to database.");

    // Create output directory if it doesn't exist
    if (!fs.existsSync(OUTPUT_DIR)) {
      fs.mkdirSync(OUTPUT_DIR);
    }

    // Get list of tables
    // const res = await client.query(`
    //   SELECT table_name
    //   FROM information_schema.tables
    //   WHERE table_schema = 'public'
    //   AND table_type = 'BASE TABLE';
    // `);

    // const tables = res.rows.map(row => row.table_name);
    const tables = ['User', 'Talent'];
    console.log(`Found ${tables.length} tables: ${tables.join(', ')}`);

    for (const tableName of tables) {
      console.log(`Exporting table: ${tableName}...`);
      
      const query = `SELECT * FROM "${tableName}"`;
      const tableData = await client.query(query);
      
      if (tableData.rows.length === 0) {
        console.log(`  Table ${tableName} is empty. Skipping.`);
        continue;
      }

      // Convert to CSV
      const csvContent = stringify(tableData.rows, {
        header: true,
        columns: tableData.fields.map(f => f.name) // Ensure columns order matches
      });

      const filePath = path.join(OUTPUT_DIR, `${tableName}.csv`);
      fs.writeFileSync(filePath, csvContent);
      console.log(`  Saved to ${filePath}`);
    }

    console.log("Export complete.");

  } catch (err) {
    console.error("Error exporting tables:", err);
  } finally {
    await client.end();
  }
}

exportTables();
