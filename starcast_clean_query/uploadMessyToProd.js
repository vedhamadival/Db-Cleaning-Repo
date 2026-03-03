const { Client } = require('pg');
const fs = require('fs');
const path = require('path');
const parse = require('csv-parse/sync');

/**
 * SIMPLE UPLOAD - Messy Data to Development
 * 
 * Uploads two separate CSV files:
 * 1. messy_users.csv → dev_users table
 * 2. messy_talents.csv → dev_talent table
 */

// Database configuration
const DB_URL = 'postgresql://ved:yourpassword@localhost:5432/starcast';

const dbConfig = {
  connectionString: DB_URL,
};

// CSV file paths
const CSV_FILES = {
  users: path.join(__dirname, 'messy_users.csv'),
  talents: path.join(__dirname, 'messy_talents.csv'),
};

// Table names
const TABLES = {
  users: 'dev_user',
  talents: 'dev_talent',
};

// Column mappings (CSV column name -> table column name)
const COLUMN_MAPPING = {
  users: {
    'createdat': 'createdAt',
    'emailverified': 'emailVerified',
    'updatedat': 'updatedAt',
  },
  talents: {
    // messy_talents.csv likely has matching column names
  },
};

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Normalize record columns based on mapping
 */
function normalizeRecord(record, dataType) {
  const mapping = COLUMN_MAPPING[dataType] || {};
  const normalized = {};
  
  for (const [key, value] of Object.entries(record)) {
    const normalizedKey = mapping[key] || key;
    normalized[normalizedKey] = value;
  }
  
  return normalized;
}

/**
 * Convert CSV string value to proper database type
 */
function convertValue(value) {
  if (value === null || value === undefined || value === '') {
    return null;
  }

  const str = String(value).trim();
  if (str.toLowerCase() === 'null' || str === '') return null;

  // Handle timestamps with " AD" suffix
  if (str.includes(' AD')) {
    return str.replace(/ AD$/, '').trim();
  }

  // Boolean handling
  if (str.toLowerCase() === 'true') return true;
  if (str.toLowerCase() === 'false') return false;

  // Try numeric conversion
  const num = Number(str);
  if (!isNaN(num) && str !== '') return num;

  // Return as string
  return str;
}

/**
 * Parse CSV file synchronously
 */
function parseCSVFile(filePath) {
  try {
    const fileContent = fs.readFileSync(filePath, 'utf8');
    return parse.parse(fileContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
      relax_quoted_strings: true,
    });
  } catch (error) {
    throw new Error(`Failed to parse CSV file ${filePath}: ${error.message}`);
  }
}

/**
 * Upload records from CSV to table
 */
async function uploadDataSet(client, filePath, table, dataType) {
  console.log(`\n${'='.repeat(70)}`);
  console.log(`📤 UPLOADING ${dataType.toUpperCase()}`);
  console.log(`   Table: ${table}`);
  console.log(`   File:  ${path.basename(filePath)}`);
  console.log(`${'='.repeat(70)}`);

  // Check if file exists
  if (!fs.existsSync(filePath)) {
    console.error(`\n❌ [ERROR] CSV file not found: ${filePath}\n`);
    return { success: false, recordsProcessed: 0, recordsSkipped: 0, errors: 1 };
  }

  let records;
  try {
    records = parseCSVFile(filePath);
  } catch (error) {
    console.error(`\n❌ [FATAL] ${error.message}\n`);
    return { success: false, recordsProcessed: 0, recordsSkipped: 0, errors: 1 };
  }

  if (records.length === 0) {
    console.log(`⚠️  No records found in CSV file`);
    return { success: true, recordsProcessed: 0, recordsSkipped: 0, errors: 0 };
  }

  console.log(`📋 CSV Records: ${records.length}`);

  // Get existing IDs to skip duplicates
  const existingIds = new Set();
  try {
    const result = await client.query(`SELECT "id" FROM "${table}" WHERE "id" IS NOT NULL`);
    result.rows.forEach((row) => {
      existingIds.add(row.id);
    });
    console.log(`📊 Existing records in ${table}: ${existingIds.size}`);
  } catch (error) {
    // Table might be empty
    console.log(`📊 Existing records in ${table}: 0`);
  }

  console.log(`\n📝 Processing records...`);

  const errors = [];
  const skipped = [];
  let successCount = 0;
  let skippedCount = 0;

  // Insert each record
  for (let i = 0; i < records.length; i++) {
    let record = records[i];
    
    // Normalize column names based on dataType (case-insensitive comparison)
    const dataTypeLower = dataType.toLowerCase();
    if (dataTypeLower === 'messy users') {
      record = normalizeRecord(record, 'users');
    } else if (dataTypeLower === 'messy talents') {
      record = normalizeRecord(record, 'talents');
    }

    // Skip if ID already exists
    if (record.id && existingIds.has(record.id)) {
      skipped.push({
        row: i + 2,
        id: record.id,
        email: record.email || 'N/A',
        reason: 'ID already exists in database',
      });
      skippedCount++;
      
      // Progress indicator
      if ((successCount + skippedCount) % 50 === 0) {
        process.stdout.write('.');
      }
      continue;
    }

    try {
      const columns = Object.keys(record);
      const values = columns.map((col) => convertValue(record[col]));
      const placeholders = columns.map((_, i) => `$${i + 1}`).join(',');
      const quotedColumns = columns.map((col) => `"${col}"`).join(',');

      const insertQuery = `INSERT INTO "${table}" (${quotedColumns}) VALUES (${placeholders})`;
      await client.query(insertQuery, values);
      successCount++;

      // Progress indicator
      if ((successCount + skippedCount) % 50 === 0) {
        process.stdout.write('.');
      }
    } catch (error) {
      errors.push({
        row: i + 2,
        id: record.id || record.email || 'N/A',
        error: error.message.split('\n')[0].substring(0, 60),
      });
    }
  }

  console.log('\n');

  // Report results
  console.log(`✅ RESULT:`);
  console.log(`   ✓ Inserted: ${successCount}`);
  console.log(`   ⊘ Skipped:  ${skippedCount}`);
  console.log(`   ✗ Errors:   ${errors.length}`);

  if (skipped.length > 0) {
    console.log(`\n⊘ SKIPPED RECORDS:`);
    skipped.slice(0, 10).forEach((rec) => {
      console.log(`   • Row ${rec.row} (${rec.id}): ${rec.reason}`);
    });
    if (skipped.length > 10) {
      console.log(`   ... and ${skipped.length - 10} more`);
    }
  }

  if (errors.length > 0) {
    console.log(`\n❌ ERRORS:`);
    errors.slice(0, 5).forEach((err) => {
      console.log(`   • Row ${err.row} (${err.id}): ${err.error}`);
    });
    if (errors.length > 5) {
      console.log(`   ... and ${errors.length - 5} more`);
    }
  }

  // Final count
  try {
    const result = await client.query(`SELECT COUNT(*) as count FROM "${table}"`);
    console.log(`\n📊 Total in ${table}: ${result.rows[0].count} records`);
  } catch (error) {
    // Silent fail
  }

  return {
    success: errors.length === 0,
    recordsProcessed: successCount,
    recordsSkipped: skippedCount,
    errors: errors.length,
  };
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

/**
 * Main function
 */
async function main() {
  console.log(`
╔══════════════════════════════════════════════════════════════════════════╗
║           UPLOAD MESSY DATA TO DEVELOPMENT - SIMPLE MODE                ║
║                                                                          ║
║  Database: starcast                                                      ║
║  Tables: dev_user, dev_talent                                            ║
╚══════════════════════════════════════════════════════════════════════════╝
  `);

  const client = new Client(dbConfig);

  try {
    console.log('🔗 Connecting to database...');
    await client.connect();
    console.log('✅ Connected\n');

    // Upload messy users
    const userResults = await uploadDataSet(
      client,
      CSV_FILES.users,
      TABLES.users,
      'Messy Users'
    );

    // Upload messy talents
    const talentResults = await uploadDataSet(
      client,
      CSV_FILES.talents,
      TABLES.talents,
      'Messy Talents'
    );

    // Final summary
    console.log(`\n${'='.repeat(70)}`);
    console.log(`📋 FINAL SUMMARY`);
    console.log(`${'='.repeat(70)}`);
    console.log(`Users   - Success: ${userResults.recordsProcessed.toString().padEnd(8)} Skip: ${userResults.recordsSkipped.toString().padEnd(8)} Errors: ${userResults.errors}`);
    console.log(`Talents - Success: ${talentResults.recordsProcessed.toString().padEnd(8)} Skip: ${talentResults.recordsSkipped.toString().padEnd(8)} Errors: ${talentResults.errors}`);

    const totalSuccess = userResults.recordsProcessed + talentResults.recordsProcessed;
    const totalSkipped = userResults.recordsSkipped + talentResults.recordsSkipped;
    const totalErrors = userResults.errors + talentResults.errors;

    console.log(`\n${'─'.repeat(70)}`);
    console.log(`Total: ${totalSuccess} inserted | ${totalSkipped} skipped | ${totalErrors} errors`);
    console.log(`${'─'.repeat(70)}\n`);

    if (totalErrors === 0) {
      console.log('✨ Upload successful!\n');
    } else {
      console.log(`⚠️  Upload completed with errors. Review above.\n`);
    }
  } catch (error) {
    console.error(`\n❌ [FATAL ERROR] ${error.message}\n`);
    process.exit(1);
  } finally {
    await client.end();
    console.log('🔌 Connection closed\n');
  }
}

// Execute
main().catch((error) => {
  console.error(`Unexpected error: ${error.message}`);
  process.exit(1);
});
