#!/usr/bin/env node

/**
 * PRODUCTION UPLOAD SCRIPT
 * Safely uploads user and talent data from CSV to production tables
 * 
 * Flexible CSV Detection:
 *   - Automatically detects PROD USER/TALENT.csv or DEV_OLD_USER/TALENT.csv
 *   - Use environment variables to override:
 *     CSV_USERS, CSV_TALENTS (file paths)
 *     TABLE_USERS, TABLE_TALENTS (table names)
 *     DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
 * 
 * Usage: 
 *   node uploadToProd.js
 *   CSV_USERS=/path/to/file.csv node uploadToProd.js
 */

const { Client } = require('pg');
const fs = require('fs');
const path = require('path');
const parse = require('csv-parse/sync');

// ============================================================================
// CONFIGURATION - All configurable via environment variables
// ============================================================================

const dbConfig = {
  host: 'localhost',
  port: 5432,
  user: 'ved',
  password: 'yourpassword',
  database: 'starcast',
};

/**
 * Find first existing CSV file in search paths
 */
function findCSVFile(fileNames) {
  const searchPaths = [
    __dirname, // Current script directory
    path.dirname(path.dirname(__dirname)), // Parent directory (StarCast root)
  ];

  for (const directory of searchPaths) {
    for (const fileName of fileNames) {
      const filePath = path.join(directory, fileName);
      if (fs.existsSync(filePath)) {
        return filePath;
      }
    }
  }

  // Return first preferred name if none found
  return path.join(searchPaths[0], fileNames[0]);
}

// CSV file paths - with auto-detection
const CSV_FILES = {
  users: process.env.CSV_USERS || findCSVFile(['PROD USER.csv', 'DEV_OLD_USER.csv']),
  talents: process.env.CSV_TALENTS || findCSVFile(['PROD TALENT.csv', 'DEV_OLD_TALENT.csv']),
};

// Table names - configurable
const TABLES = {
  users: process.env.TABLE_USERS || 'prod_user',
  talents: process.env.TABLE_TALENTS || 'prod_talent',
};

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

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

  // Handle JavaScript date format: "Sun May 14 2023 08:00:37 GMT+0530 (India Standard Time)"
  if (str.match(/^[A-Za-z]{3}\s+[A-Za-z]{3}\s+\d{1,2}\s+\d{4}/)) {
    try {
      const date = new Date(str);
      if (!isNaN(date.getTime())) {
        // Convert to ISO string and remove timezone
        return date.toISOString().split('.')[0]; // "2023-05-14T02:30:37"
      }
    } catch (e) {
      // Fallback to string
    }
  }

  // Handle ISO timestamps with GMT timezone info
  if (str.includes('gmt') || str.includes('GMT')) {
    const isoPart = str.split(/\s+(gmt|GMT)/i)[0];
    if (isoPart && isoPart.match(/\d{4}-\d{2}-\d{2}/)) {
      return isoPart;
    }
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
 * Check for duplicate records in CSV and database
 */
async function checkDuplicates(client, table, records, keyField = 'email') {
  const duplicates = {
    inCsv: [],
    inDatabase: [],
  };

  // Check for duplicates within CSV
  const emailMap = {};
  records.forEach((record, index) => {
    const key = record[keyField]?.toString().toLowerCase().trim();
    if (key && key !== 'null') {
      if (!emailMap[key]) {
        emailMap[key] = [];
      }
      emailMap[key].push(index + 2); // +2 because row 1 is header + 0-indexed
    }
  });

  // Identify CSV duplicates
  for (const [key, indices] of Object.entries(emailMap)) {
    if (indices.length > 1) {
      duplicates.inCsv.push({ value: key, rows: indices, count: indices.length });
    }
  }

  // Check for duplicates in database
  if (duplicates.inCsv.length > 0) {
    const csvEmails = duplicates.inCsv.map((d) => d.value);
    const placeholders = csvEmails.map((_, i) => `$${i + 1}`).join(',');
    const existingQuery = `SELECT ${keyField}, COUNT(*) as cnt FROM ${table} 
                          WHERE LOWER(${keyField}) IN (${placeholders})
                          GROUP BY ${keyField}`;
    
    try {
      const result = await client.query(existingQuery, csvEmails);
      duplicates.inDatabase = result.rows;
    } catch (error) {
      // Table might not have data yet, skip check
    }
  }

  return duplicates;
}

/**
 * Insert records with duplicate handling
 * @param {Client} client - Database client
 * @param {string} filePath - Path to CSV file
 * @param {string} table - Target table name
 * @param {string} dataType - Data type label (Users/Talents)
 * @param {string} keyField - Field to check for duplicates (email/id)
 * @param {Object} skippedUsersData - Skipped users from previous upload (for validating parent users)
 */
async function uploadDataSet(client, filePath, table, dataType, keyField = 'email', skippedUsersData = null) {
  console.log(`\n${'='.repeat(70)}`);
  console.log(`📤 UPLOADING ${dataType.toUpperCase()}`);
  console.log(`   Table: ${table}`);
  console.log(`   File:  ${path.basename(filePath)}`);
  console.log(`   Duplicate Check: ${keyField}`);
  console.log(`${'='.repeat(70)}`);

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

  // Check for duplicates
  const duplicates = await checkDuplicates(client, table, records, keyField);

  if (duplicates.inCsv.length > 0) {
    console.log(`\n⚠️  DUPLICATES IN CSV FILE:`);
    duplicates.inCsv.forEach((dup) => {
      console.log(`   📍 "${dup.value}" - ${dup.count} times (rows: ${dup.rows.join(', ')})`);
    });
  }

  if (duplicates.inDatabase.length > 0) {
    console.log(`\n⚠️  DUPLICATES IN DATABASE:`);
    duplicates.inDatabase.forEach((dup) => {
      console.log(`   📍 "${dup[Object.keys(dup)[0]]}" - already exists`);
    });
  }

  // Get existing values to skip (by keyField)
  const existingValues = new Set();
  try {
    const result = await client.query(`SELECT "${keyField}" FROM ${table} WHERE "${keyField}" IS NOT NULL`);
    result.rows.forEach((row) => {
      const value = row[keyField];
      existingValues.add(typeof value === 'string' ? value.toLowerCase() : value);
    });
  } catch (error) {
    // Table might be empty
  }

  // Build map of skipped user IDs for talent validation (when uploading talents)
  let skippedUserIdMap = new Map();
  if (skippedUsersData && keyField === 'id') {
    // This is a talent upload, check if parent users were skipped
    try {
      const userRecords = parseCSVFile(CSV_FILES.users);
      const userEmailToIdMap = new Map();
      userRecords.forEach((record) => {
        if (record.email && record.id) {
          userEmailToIdMap.set(record.email.toLowerCase(), record.id);
        }
      });
      
      // Build map of skipped userIds and their reasons
      if (skippedUsersData.skipped && skippedUsersData.skipped.length > 0) {
        skippedUsersData.skipped.forEach((skippedRecord) => {
          const email = skippedRecord.email?.toLowerCase();
          if (email) {
            const userId = userEmailToIdMap.get(email);
            if (userId) {
              skippedUserIdMap.set(userId, skippedRecord.reason);
            }
          }
        });
      }
    } catch (error) {
      console.warn(`⚠️  Could not validate parent users: ${error.message}`);
    }
  }

  console.log(`\n📝 Processing records...`);

  // Insert records
  const errors = [];
  const skipped = [];
  let successCount = 0;
  let skippedCount = 0;

  for (let i = 0; i < records.length; i++) {
    const record = records[i];

    // Skip if parent user was skipped (for talent records)
    if (keyField === 'id' && record.userId && skippedUserIdMap.has(record.userId)) {
      const parentUserSkipReason = skippedUserIdMap.get(record.userId);
      skipped.push({
        row: i + 2,
        id: record.id,
        userId: record.userId,
        reason: `Parent user was skipped (${parentUserSkipReason})`,
      });
      skippedCount++;
      continue;
    }

    // Skip if keyField value already exists
    if (record[keyField]) {
      const keyValue = typeof record[keyField] === 'string' ? record[keyField].toLowerCase() : record[keyField];
      if (existingValues.has(keyValue)) {
        skipped.push({
          row: i + 2,
          [keyField]: record[keyField],
          reason: `${keyField === 'id' ? 'ID' : 'Email'} already exists in database`,
        });
        skippedCount++;
        continue;
      }
    }

    try {
      const columns = Object.keys(record);
      const values = columns.map((col) => convertValue(record[col]));
      const placeholders = columns.map((_, i) => `$${i + 1}`).join(',');
      const quotedColumns = columns.map((col) => `"${col}"`).join(',');

      const insertQuery = `INSERT INTO ${table} (${quotedColumns}) VALUES (${placeholders})`;
      await client.query(insertQuery, values);
      successCount++;

      // Progress indicator
      if ((successCount + skippedCount) % 50 === 0) {
        process.stdout.write('.');
      }
    } catch (error) {
      errors.push({
        row: i + 2,
        email: record.email || 'N/A',
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
      const displayValue = rec[keyField] || rec.email || rec.id || rec.userId || 'N/A';
      console.log(`   • Row ${rec.row} (${displayValue}): ${rec.reason}`);
    });
    if (skipped.length > 10) {
      console.log(`   ... and ${skipped.length - 10} more`);
    }
  }

  if (errors.length > 0) {
    console.log(`\n❌ ERRORS:`);
    errors.slice(0, 5).forEach((err) => {
      console.log(`   • Row ${err.row} (${err.email}): ${err.error}`);
    });
    if (errors.length > 5) {
      console.log(`   ... and ${errors.length - 5} more`);
    }
  }

  // Final count
  try {
    const result = await client.query(`SELECT COUNT(*) as count FROM ${table}`);
    console.log(`\n📊 Total in ${table}: ${result.rows[0].count} records`);
  } catch (error) {
    // Silent fail
  }

  return {
    success: errors.length === 0,
    recordsProcessed: successCount,
    recordsSkipped: skippedCount,
    errors: errors.length,
    skipped: skipped,
  };
}

/**
 * Main execution
 */
async function main() {
  console.log(`
╔══════════════════════════════════════════════════════════════════════════╗
║                PRODUCTION DATA UPLOAD - SAFE MODE                       ║
║                                                                          ║
║  Database: ${dbConfig.database.padEnd(55)}║
║  Host: ${(dbConfig.host + ':' + dbConfig.port).padEnd(61)}║
║  User: ${dbConfig.user.padEnd(60)}║
╚══════════════════════════════════════════════════════════════════════════╝
  `);

  const client = new Client(dbConfig);

  try {
    console.log('🔗 Connecting to database...');
    await client.connect();
    console.log('✅ Connected\n');

    // Verify tables exist
    const tableCheck = await client.query(`
      SELECT table_name FROM information_schema.tables 
      WHERE table_name IN ($1, $2)
    `, [TABLES.users, TABLES.talents]);

    if (tableCheck.rows.length === 0) {
      throw new Error(`Tables not found. Expected: ${TABLES.users}, ${TABLES.talents}`);
    }

    console.log(`✅ Tables found: ${tableCheck.rows.map(r => r.table_name).join(', ')}\n`);

    // Upload users (check email duplicates)
    const userResults = await uploadDataSet(
      client,
      CSV_FILES.users,
      TABLES.users,
      'Users',
      'email'
    );

    // Upload talents (check ID duplicates + validate parent users)
    const talentResults = await uploadDataSet(
      client,
      CSV_FILES.talents,
      TABLES.talents,
      'Talents',
      'id',
      userResults
    );

    // Final summary
    console.log(`\n${'='.repeat(70)}`);
    console.log(`📋 FINAL SUMMARY`);
    console.log(`${'='.repeat(70)}`);
    console.log(`Users  - Success: ${userResults.recordsProcessed.toString().padEnd(8)} Skip: ${userResults.recordsSkipped.toString().padEnd(8)} Errors: ${userResults.errors}`);
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
