const fs = require("fs");
const path = require("path");
const { Client } = require("pg");
const { parse } = require("csv-parse/sync");

// ============================================
// DATABASE CONNECTION
// ============================================
const client = new Client({
  user: "ved",
  host: "localhost",
  database: "v2starcast",
  password: "yourpassword",
  port: 5432,
});

// ============================================
// CSV FILES TO IMPORT
// ============================================
// Add your CSV files here in the format: "path/to/file.csv"
// Supports both relative paths (from this script) and absolute paths
const CSV_FILES = [
  "exported_csvs/User.csv",
  "exported_csvs/Talent.csv",
];

// ============================================
// HELPER: Infer column type from data
// ============================================
function inferColumnType(value) {
  if (!value || value.trim() === "") return "VARCHAR(255)";

  // Check if it's a number
  if (!isNaN(value) && !isNaN(parseFloat(value))) {
    if (value.includes(".")) {
      return "NUMERIC";
    }
    return "INTEGER";
  }

  // Check if it's a boolean
  if (value.toLowerCase() === "true" || value.toLowerCase() === "false") {
    return "BOOLEAN";
  }

  // Check if it's a date/timestamp
  if (
    /^\d{4}-\d{2}-\d{2}/.test(value) ||
    /^\d{1,2}\/\d{1,2}\/\d{4}/.test(value)
  ) {
    return "TIMESTAMP";
  }

  // Check string length for VARCHAR limits
  if (value.length > 1000) {
    return "TEXT";
  }

  return "VARCHAR(255)";
}

// ============================================
// HELPER: Get most common type for column
// ============================================
function getMostCommonType(types) {
  const counts = {};
  types.forEach((type) => {
    counts[type] = (counts[type] || 0) + 1;
  });

  let mostCommon = types[0];
  let maxCount = 0;

  for (const [type, count] of Object.entries(counts)) {
    if (count > maxCount) {
      maxCount = count;
      mostCommon = type;
    }
  }

  // Prefer broader types
  if (counts["TEXT"] > 0) return "TEXT";
  if (counts["VARCHAR(255)"] > 0) return "VARCHAR(255)";
  if (counts["NUMERIC"] > 0) return "NUMERIC";
  if (counts["TIMESTAMP"] > 0) return "TIMESTAMP";
  if (counts["INTEGER"] > 0) return "INTEGER";

  return mostCommon;
}

// ============================================
// PARSE CSV FILE
// ============================================
async function parseCSVFile(filePath) {
  try {
    if (!fs.existsSync(filePath)) {
      console.log(`   ❌ File not found: ${filePath}`);
      return null;
    }

    const fileContent = fs.readFileSync(filePath, "utf-8");
    const records = parse(fileContent, {
      columns: true,
      skip_empty_lines: true,
      relax_quoted_strings: true,
    });

    if (records.length === 0) {
      console.log(`   ⚠️  CSV is empty: ${filePath}`);
      return null;
    }

    return records;
  } catch (error) {
    console.log(`   ❌ Error parsing CSV: ${error.message}`);
    return null;
  }
}

// ============================================
// INFER TABLE SCHEMA
// ============================================
function inferSchema(records) {
  if (!records || records.length === 0) return null;

  const headers = Object.keys(records[0]);
  const schema = {};

  headers.forEach((header) => {
    const columnTypes = [];

    records.forEach((row) => {
      const value = row[header];
      columnTypes.push(inferColumnType(value));
    });

    schema[header] = getMostCommonType(columnTypes);
  });

  return { headers, schema };
}

// ============================================
// CREATE TABLE
// ============================================
async function createTable(tableName, schema) {
  try {
    // Drop table if exists
    await client.query(`DROP TABLE IF EXISTS "${tableName}" CASCADE`);

    // Create column definitions
    const columnDefs = schema.headers
      .map((header) => {
        const type = schema.schema[header];
        // Escape header names that might be SQL keywords
        return `"${header}" ${type}`;
      })
      .join(",\n  ");

    const createTableSQL = `
      CREATE TABLE "${tableName}" (
        ${columnDefs}
      )
    `;

    await client.query(createTableSQL);
    console.log(`   ✅ Table created: "${tableName}"`);
    return true;
  } catch (error) {
    console.log(`   ❌ Error creating table: ${error.message}`);
    return false;
  }
}

// ============================================
// INSERT DATA INTO TABLE (BATCHED)
// ============================================
async function insertData(tableName, records, schema) {
  try {
    const headers = schema.headers;
    const BATCH_SIZE = 500; // Insert 500 records at a time
    let insertedCount = 0;
    let errorCount = 0;

    // Process records in batches
    for (let i = 0; i < records.length; i += BATCH_SIZE) {
      const batch = records.slice(i, Math.min(i + BATCH_SIZE, records.length));

      try {
        // Build multi-value INSERT statement
        const valuesList = [];
        const allValues = [];
        let paramIndex = 1;

        batch.forEach((record) => {
          const values = headers.map((header) => {
            let value = record[header];

            if (!value || value.trim() === "") {
              return null;
            }

            const type = schema.schema[header];

            if (type === "BOOLEAN") {
              return value.toLowerCase() === "true";
            }

            if (type === "INTEGER") {
              return parseInt(value, 10);
            }

            if (type === "NUMERIC") {
              return parseFloat(value);
            }

            return value;
          });

          const placeholders = values
            .map(() => `$${paramIndex++}`)
            .join(",");
          valuesList.push(`(${placeholders})`);
          allValues.push(...values);
        });

        const headerList = headers.map((h) => `"${h}"`).join(",");
        const insertSQL = `INSERT INTO "${tableName}" (${headerList}) VALUES ${valuesList.join(
          ","
        )}`;

        await client.query(insertSQL, allValues);
        insertedCount += batch.length;

        // Progress indicator
        process.stdout.write(
          `\r   📊 Inserting... ${insertedCount}/${records.length}`
        );
      } catch (err) {
        errorCount += batch.length;
      }
    }

    console.log(`\n   📊 Inserted ${insertedCount} records (${errorCount} errors)`);
    return insertedCount;
  } catch (error) {
    console.log(`   ❌ Error inserting data: ${error.message}`);
    return 0;
  }
}

// ============================================
// MAIN: Import CSV to Database
// ============================================
async function importCSVToDatabase(csvFilePath) {
  console.log(`\n📥 Processing: ${path.basename(csvFilePath)}`);
  console.log("─".repeat(60));

  try {
    // 1. Parse CSV
    console.log("   📖 Parsing CSV...");
    const records = await parseCSVFile(csvFilePath);
    if (!records) return false;
    console.log(`   ✅ Found ${records.length} records`);

    // 2. Infer schema
    console.log("   🔍 Inferring schema...");
    const schema = inferSchema(records);
    if (!schema) {
      console.log("   ❌ Could not infer schema");
      return false;
    }
    console.log(`   ✅ Detected ${schema.headers.length} columns`);

    // 3. Get table name from filename
    const tableName = path
      .basename(csvFilePath, ".csv")
      .toLowerCase()
      .replace(/[\s-]/g, "_");

    // 4. Create table
    console.log(`   🛠️  Creating table "${tableName}"...`);
    const created = await createTable(tableName, schema);
    if (!created) return false;

    // 5. Insert data
    console.log("   📤 Inserting data...");
    const inserted = await insertData(tableName, records, schema);
    if (inserted === 0) return false;

    console.log(`   ✅ Successfully imported "${tableName}"\n`);
    return true;
  } catch (error) {
    console.log(`   ❌ Error: ${error.message}\n`);
    return false;
  }
}

// ============================================
// MAIN EXECUTION
// ============================================
async function main() {
  console.log("\n" + "=".repeat(60));
  console.log("📊 CSV TO DATABASE IMPORT");
  console.log("=".repeat(60));

  if (CSV_FILES.length === 0) {
    console.log(
      "\n⚠️  No CSV files specified. Add files to CSV_FILES array in the script.\n"
    );
    return;
  }

  try {
    await client.connect();
    console.log("✅ Connected to PostgreSQL\n");

    let successCount = 0;
    let failureCount = 0;

    for (const csvFile of CSV_FILES) {
      // Support both relative and absolute paths
      const filePath = path.isAbsolute(csvFile)
        ? csvFile
        : path.join(__dirname, csvFile);

      const success = await importCSVToDatabase(filePath);
      if (success) {
        successCount++;
      } else {
        failureCount++;
      }
    }

    // Summary
    console.log("=".repeat(60));
    console.log("📊 IMPORT SUMMARY");
    console.log("=".repeat(60));
    console.log(`✅ Successful: ${successCount}`);
    console.log(`❌ Failed: ${failureCount}`);
    console.log(`📋 Total: ${CSV_FILES.length}\n`);
  } catch (error) {
    console.error("❌ Fatal error:", error.message);
  } finally {
    await client.end();
    console.log("🔌 Database connection closed.\n");
  }
}

// Export for use as module
module.exports = { importCSVToDatabase };

// Run if called directly
if (require.main === module) {
  main();
}
