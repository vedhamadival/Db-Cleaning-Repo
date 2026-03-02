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
  database: "v2starcast_new",
  password: "yourpassword",
  port: 5432,
});

// ============================================
// CSV FILES TO IMPORT (Order matters - Users first, then Talents)
// ============================================
const CSV_FILES = [
  {
    path: "../User (2).csv",
    tableName: "User",
  },
  {
    path: "../Talent (3).csv",
    tableName: "Talent",
  },
];

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

    console.log(`   ✅ Retrieved schema for "${tableName}" (${result.rows.length} columns)`);
    return schema;
  } catch (error) {
    console.log(`   ❌ Error getting table schema: ${error.message}`);
    return null;
  }
}

// ============================================
// CONVERT VALUE TO PROPER TYPE
// ============================================
function convertValue(value, columnInfo) {
  // Handle empty/null values
  if (value === null || value === undefined || value === "" || value === "null" || value === "NULL" || value === "undefined") {
    return null;
  }

  const strValue = String(value).trim();

  if (!strValue || strValue === "null" || strValue === "NULL" || strValue === "undefined") {
    return null;
  }

  const dataType = columnInfo.dataType;

  // Convert based on PostgreSQL data type
  if (
    dataType.includes("integer") ||
    dataType.includes("bigint") ||
    dataType.includes("smallint")
  ) {
    // Remove any non-numeric characters except minus sign
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
    // PostgreSQL is flexible with date parsing, return as string
    return strValue;
  }

  if (dataType.includes("json")) {
    try {
      // Validate JSON
      JSON.parse(strValue);
      return strValue;
    } catch {
      // If not valid JSON, return null
      return null;
    }
  }

  // String types (character, varchar, text)
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
// INSERT DATA - With detailed error tracking
// ============================================
async function insertDataWithErrorTracking(tableName, records, schema) {
  try {
    const allColumns = Object.keys(schema);
    let insertedCount = 0;
    const failedRecords = [];

    console.log(`   📋 Total columns in table: ${allColumns.length}`);
    console.log(`   📊 CSV has ${Object.keys(records[0] || {}).length} columns in first row`);

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
          rowNumber: i + 2, // +2 because row 1 is headers, counting starts at 1
          recordId: record.id || "N/A",
          error: err.message.split("\n")[0].substring(0, 100),
          record: record, // Store for potential debugging
        });
      }
    }

    console.log(`\n   ✅ Successfully inserted: ${insertedCount}/${records.length}`);

    if (failedRecords.length > 0) {
      console.log(
        `\n   ⚠️  Failed records: ${failedRecords.length}`
      );
      console.log("\n   📋 Failed Record Details:");

      failedRecords.slice(0, 10).forEach((failed) => {
        console.log(`      Row ${failed.rowNumber} (ID: ${failed.recordId})`);
        console.log(`      Error: ${failed.error}`);

        // Show problematic values
        const problemCols = Object.entries(failed.record)
          .filter(
            ([key, val]) =>
              val === "undefined" ||
              val === null ||
              (typeof val === "string" && val.length > 500)
          )
          .slice(0, 3);

        if (problemCols.length > 0) {
          console.log(`      Problematic columns:`);
          problemCols.forEach(([col, val]) => {
            const preview = String(val).substring(0, 50);
            console.log(`        - ${col}: "${preview}${String(val).length > 50 ? "..." : ""}"`);
          });
        }
        console.log();
      });

      if (failedRecords.length > 10) {
        console.log(
          `      ... and ${failedRecords.length - 10} more failed records\n`
        );
      }

      // Save failed records to file for later analysis
      const failureFile = `${tableName}_failures_${Date.now()}.json`;
      fs.writeFileSync(
        path.join(__dirname, failureFile),
        JSON.stringify(failedRecords, null, 2)
      );
      console.log(`   💾 Failed records saved to: ${failureFile}\n`);
    } else {
      console.log(`   🎉 NO ERRORS - All ${insertedCount} records inserted successfully!\n`);
    }

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

    // 3. Insert data with detailed error tracking
    console.log("   📤 Inserting data...");
    const inserted = await insertDataWithErrorTracking(tableName, records, schema);
    if (inserted === 0) {
      console.log(`   ❌ No records were inserted`);
      return false;
    }

    console.log(`   ✅ Successfully imported ${inserted} records into "${tableName}"\n`);
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
  console.log("📊 CSV TO V2 DATABASE IMPORT (WITH ERROR DETAILS)");
  console.log("=".repeat(60));

  if (CSV_FILES.length === 0) {
    console.log("\n⚠️  No CSV files specified.\n");
    return;
  }

  try {
    await client.connect();
    console.log("✅ Connected to PostgreSQL (v2starcast_new)\n");

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
