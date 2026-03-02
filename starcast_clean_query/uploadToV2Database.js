const fs = require("fs");
const path = require("path");
const { Client } = require("pg");
const { parse } = require("csv-parse/sync");

// ============================================
// DATABASE CONNECTION - V2 DATABASE
// ============================================
const client = new Client({
  user: "ved",
  host: "localhost",
  database: "v2starcast",
  password: "yourpassword",
  port: 5432,
});

// ============================================
// CSV FILES TO IMPORT (Order matters - Users first, then Talents)
// ============================================
const CSV_FILES = [
  {
    path: "./exported_csvs/User.csv",
    tableName: "User",
  },
  {
    path: "./exported_csvs/Talent.csv",
    tableName: "Talent",
  },
];

// ============================================
// DELETE ALL DATA FROM TABLES
// ============================================
async function clearTables() {
  try {
    console.log("🗑️  Clearing existing data...");
    
    // Delete Talent first (if foreign key constraint exists)
    await client.query('DELETE FROM "Talent"');
    console.log("   ✅ Deleted all Talent records");
    
    // Delete User
    await client.query('DELETE FROM "User"');
    console.log("   ✅ Deleted all User records");
    
    console.log();
  } catch (error) {
    console.log(`   ❌ Error clearing tables: ${error.message}`);
    throw error;
  }
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
      trim: true,
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
// GET TABLE SCHEMA from database
// ============================================
async function getTableSchema(tableName) {
  try {
    const result = await client.query(
      `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position`,
      [tableName]
    );

    if (result.rows.length === 0) {
      console.log(`   ⚠️  Table "${tableName}" not found in database`);
      return null;
    }

    const schema = {};
    result.rows.forEach((row) => {
      schema[row.column_name] = {
        dataType: row.data_type,
        nullable: row.is_nullable === "YES",
      };
    });

    return schema;
  } catch (error) {
    console.log(`   ❌ Error getting table schema: ${error.message}`);
    return null;
  }
}

// ============================================
// CONVERT PYTHON LIST SYNTAX TO POSTGRESQL ARRAY
// ============================================
function convertPythonListToPostgreSQLArray(value) {
  if (!value || value.trim() === "") return null;

  const trimmed = value.trim();

  // Check if it's Python list syntax: ['item'] or ['item1', 'item2']
  if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
    try {
      // Replace single quotes with double quotes for JSON parsing
      const jsonStr = trimmed.replace(/'/g, '"');
      const items = JSON.parse(jsonStr);

      if (!Array.isArray(items)) return null;

      // Convert to PostgreSQL array format: {item1,item2}
      const pgArray = "{" + items.join(",") + "}";
      return pgArray;
    } catch (e) {
      return null;
    }
  }

  // If it's already PostgreSQL format, return as-is
  if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
    return trimmed;
  }

  return null;
}

// ============================================
// CONVERT VALUE TO PROPER TYPE
// ============================================
function convertValue(value, columnInfo) {
  // Handle empty/null values
  if (
    value === null ||
    value === undefined ||
    value === "" ||
    value === "null" ||
    value === "NULL" ||
    value === "undefined"
  ) {
    return null;
  }

  const strValue = String(value).trim();

  if (!strValue || strValue === "null" || strValue === "NULL" || strValue === "undefined") {
    return null;
  }

  const dataType = columnInfo.dataType;

  // Handle array types
  if (dataType.includes("ARRAY") || dataType.includes("array") || dataType === "text[]") {
    const converted = convertPythonListToPostgreSQLArray(strValue);
    return converted;
  }

  // Convert based on PostgreSQL data type
  if (
    dataType.includes("integer") ||
    dataType.includes("bigint") ||
    dataType.includes("smallint")
  ) {
    const cleaned = strValue.replace(/[^0-9.-]/g, "");
    if (!cleaned) return null;
    const num = parseInt(cleaned, 10);
    return isNaN(num) ? null : num;
  }

  if (
    dataType.includes("numeric") ||
    dataType.includes("decimal") ||
    dataType.includes("real") ||
    dataType.includes("double")
  ) {
    const cleaned = strValue.replace(/[^0-9.-]/g, "");
    if (!cleaned) return null;
    const num = parseFloat(cleaned);
    return isNaN(num) ? null : num;
  }

  if (dataType.includes("boolean")) {
    if (
      strValue === "true" ||
      strValue === "1" ||
      strValue === "t" ||
      strValue === "yes" ||
      strValue === "y"
    )
      return true;
    if (
      strValue === "false" ||
      strValue === "0" ||
      strValue === "f" ||
      strValue === "no" ||
      strValue === "n"
    )
      return false;
    return null;
  }

  if (dataType.includes("timestamp") || dataType.includes("date")) {
    // If it's a number (Unix timestamp in ms or s)
    if (!isNaN(strValue) && !isNaN(parseFloat(strValue))) {
       const numVal = parseFloat(strValue);
       // Heuristic: if > 1e11, assumes milliseconds (e.g. 1682...), otherwise seconds
       // Max seconds for year 3000 is about 3e10. so 1e11 check is safe
       const date = new Date(numVal > 1e11 ? numVal : numVal * 1000);
       return date.toISOString();
    }
    return strValue;
  }

  if (dataType.includes("json")) {
    try {
      JSON.parse(strValue);
      return strValue;
    } catch {
      return null;
    }
  }

  // String types
  return strValue;
}

// ============================================
// INSERT SINGLE RECORD
// ============================================
async function insertSingleRecord(tableName, record, schema, allColumns) {
  const values = [];
  const columns = [];

  for (const col of allColumns) {
    if (record.hasOwnProperty(col)) {
      const value = record[col];
      const convertedValue = convertValue(value, schema[col]);
      columns.push(col);
      values.push(convertedValue);
    }
  }

  if (columns.length === 0) {
    throw new Error("No valid columns to insert");
  }

  const columnList = columns.map((col) => `"${col}"`).join(",");
  const valuesList = columns.map((_, idx) => `$${idx + 1}`).join(",");
  const insertSQL = `INSERT INTO "${tableName}" (${columnList}) VALUES (${valuesList})`;

  await client.query(insertSQL, values);
}

// ============================================
// INSERT DATA - Row by Row with Error Tracking
// ============================================
async function insertDataRowByRow(tableName, records, schema) {
  try {
    const allColumns = Object.keys(schema);
    let insertedCount = 0;
    const failedRecords = [];

    console.log(`   📋 Total columns in table: ${allColumns.length}`);
    console.log(`   📊 Processing ${records.length} records...\n`);

    for (let i = 0; i < records.length; i++) {
      const record = records[i];

      try {
        await insertSingleRecord(tableName, record, schema, allColumns);
        insertedCount++;

        // Progress indicator every 50 records
        if ((i + 1) % 50 === 0 || i === 0 || i === records.length - 1) {
          process.stdout.write(
            `\r   📊 Inserting... ${insertedCount}/${records.length}`
          );
        }
      } catch (err) {
        failedRecords.push({
          rowNumber: i + 2,
          recordId: record.id || "N/A",
          error: err.message.split("\n")[0].substring(0, 100),
        });
      }
    }

    console.log(`\n   ✅ Successfully inserted: ${insertedCount}/${records.length}`);

    if (failedRecords.length > 0) {
      console.log(`\n   ⚠️  Failed records: ${failedRecords.length}`);
      console.log("\n   📋 Failed Record Details (first 10):");

      failedRecords.slice(0, 10).forEach((failed) => {
        console.log(`      Row ${failed.rowNumber} (ID: ${failed.recordId})`);
        console.log(`      Error: ${failed.error}`);
      });

      if (failedRecords.length > 10) {
        console.log(`      ... and ${failedRecords.length - 10} more failed records`);
      }

      // Save failed records to file
      const failureFile = `${tableName}_failures_${Date.now()}.json`;
      fs.writeFileSync(
        path.join(__dirname, failureFile),
        JSON.stringify(failedRecords, null, 2)
      );
      console.log(`\n   💾 Failed records saved to: ${failureFile}`);
    } else {
      console.log(`   🎉 NO ERRORS - All ${insertedCount} records inserted successfully!`);
    }

    console.log();
    return insertedCount;
  } catch (error) {
    console.log(`   ❌ Fatal error during insertion: ${error.message}`);
    return 0;
  }
}

// ============================================
// MAIN: Import CSV to Database
// ============================================
async function importCSVToDatabase(csvConfig) {
  const csvFile = csvConfig.path;
  const tableName = csvConfig.tableName;

  console.log(`\n📥 Processing: ${path.basename(csvFile)}`);
  console.log("─".repeat(60));

  try {
    // 1. Parse CSV
    console.log("   📖 Parsing CSV...");
    const records = await parseCSVFile(csvFile);
    if (!records) return false;
    console.log(`   ✅ Found ${records.length} records`);

    // 2. Get table schema from database
    console.log("   🔍 Getting table schema from database...");
    const schema = await getTableSchema(tableName);
    if (!schema) {
      console.log(`   ❌ Could not get schema for table "${tableName}"`);
      return false;
    }
    console.log(`   ✅ Retrieved schema (${Object.keys(schema).length} columns)`);

    // 3. Insert data row by row
    console.log("   📤 Inserting data...");
    const inserted = await insertDataRowByRow(tableName, records, schema);
    if (inserted === 0) {
      console.log(`   ❌ No records were inserted`);
      return false;
    }

    console.log(`   ✅ Successfully imported ${inserted} records into "${tableName}"`);
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
  console.log("📊 CSV TO V2 DATABASE IMPORT (FRESH START)");
  console.log("=".repeat(60));
  console.log();

  if (CSV_FILES.length === 0) {
    console.log("⚠️  No CSV files specified.\n");
    return;
  }

  try {
    await client.connect();
    console.log("✅ Connected to PostgreSQL (v2starcast_new)\n");

    // Clear all existing data
    await clearTables();

    let successCount = 0;
    let failureCount = 0;

    // Process each CSV file in order
    for (const csvConfig of CSV_FILES) {
      // Support both relative and absolute paths
      const filePath = path.isAbsolute(csvConfig.path)
        ? csvConfig.path
        : path.join(__dirname, csvConfig.path);

      const success = await importCSVToDatabase({
        ...csvConfig,
        path: filePath,
      });

      if (success) {
        successCount++;
      } else {
        failureCount++;
      }
    }

    // Final verification
    console.log("─".repeat(60));
    console.log("🔍 Final Verification:");
    console.log("─".repeat(60));

    const userCount = await client.query('SELECT COUNT(*) FROM "User"');
    const talentCount = await client.query('SELECT COUNT(*) FROM "Talent"');

    console.log(`   📊 User records in database: ${userCount.rows[0].count}`);
    console.log(`   📊 Talent records in database: ${talentCount.rows[0].count}\n`);

    // Summary
    console.log("=".repeat(60));
    console.log("📊 IMPORT SUMMARY");
    console.log("=".repeat(60));
    console.log(`✅ Successful: ${successCount}`);
    console.log(`❌ Failed: ${failureCount}`);
    console.log(`📋 Total: ${CSV_FILES.length}\n`);

    if (successCount === CSV_FILES.length) {
      console.log("🎉 ALL DATA SUCCESSFULLY UPLOADED!\n");
    }
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
