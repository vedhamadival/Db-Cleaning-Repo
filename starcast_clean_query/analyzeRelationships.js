const { Client } = require("pg");

const client = new Client({
  user: "ved",
  host: "localhost",
  database: "starcast",
  password: "yourpassword",
  port: 5432,
});

async function analyzeRelationships() {
  try {
    await client.connect();

    const tables = [
      "users",
      "actordetails",
      "actormediadetails",
      "mediatags",
      "cities",
      "states",
      "nationality",
      "countries",
    ];

    console.log("\n" + "=".repeat(80));
    console.log("🔍 DATABASE SCHEMA ANALYSIS");
    console.log("=".repeat(80) + "\n");

    // Get all table schemas
    const schemaMap = {};

    for (const table of tables) {
      console.log(`📋 TABLE: ${table.toUpperCase()}`);
      console.log("-".repeat(80));

      try {
        // Get columns
        const columnsRes = await client.query(`
          SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
          FROM 
            information_schema.columns
          WHERE 
            table_name = $1
          ORDER BY 
            ordinal_position
        `, [table]);

        const columns = columnsRes.rows;
        schemaMap[table] = columns;

        console.log("Columns:");
        columns.forEach((col, idx) => {
          const nullable = col.is_nullable === "YES" ? "NULL" : "NOT NULL";
          console.log(
            `  ${idx + 1}. ${col.column_name} (${col.data_type}) - ${nullable}`
          );
        });

        // Get foreign keys
        const fkRes = await client.query(`
          SELECT 
            tc.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
          FROM 
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
          WHERE 
            tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = $1
        `, [table]);

        if (fkRes.rows.length > 0) {
          console.log("\n🔗 Foreign Keys:");
          fkRes.rows.forEach((fk) => {
            console.log(
              `  ${fk.column_name} → ${fk.foreign_table_name}.${fk.foreign_column_name}`
            );
          });
        }

        // Get primary key
        const pkRes = await client.query(`
          SELECT 
            column_name
          FROM 
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
          WHERE 
            tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_name = $1
        `, [table]);

        if (pkRes.rows.length > 0) {
          console.log("\n🔑 Primary Key:");
          pkRes.rows.forEach((pk) => {
            console.log(`  ${pk.column_name}`);
          });
        }

        console.log("\n");
      } catch (err) {
        console.log(`  ❌ Error analyzing table: ${err.message}\n`);
      }
    }

    // Now let's find potential links by column name patterns
    console.log("=".repeat(80));
    console.log("🔗 POTENTIAL RELATIONSHIPS (by column naming)");
    console.log("=".repeat(80) + "\n");

    for (const [table, columns] of Object.entries(schemaMap)) {
      const columnNames = columns.map((c) => c.column_name.toLowerCase());

      console.log(`${table}:`);

      // Look for ID columns that might link to other tables
      columnNames.forEach((col) => {
        if (col.includes("id") && col !== "id") {
          tables.forEach((otherTable) => {
            if (
              col.includes(otherTable) ||
              col ===
                `${otherTable}id` ||
              col === `${otherTable.slice(0, -1)}id`
            ) {
              console.log(
                `  ➜ ${col} might link to ${otherTable}`
              );
            }
          });
        }
      });
    }

    // Get sample counts
    console.log("\n" + "=".repeat(80));
    console.log("📊 TABLE RECORD COUNTS");
    console.log("=".repeat(80) + "\n");

    for (const table of tables) {
      try {
        const countRes = await client.query(`SELECT COUNT(*) FROM ${table}`);
        const count = countRes.rows[0].count;
        console.log(`${table}: ${count.toLocaleString()} records`);
      } catch (err) {
        console.log(`${table}: ❌ Error`);
      }
    }

    await client.end();
  } catch (error) {
    console.error("Error:", error.message);
    await client.end();
  }
}

analyzeRelationships();
