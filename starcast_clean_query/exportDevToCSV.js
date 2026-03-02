#!/usr/bin/env node

/**
 * EXPORT DEV TABLES TO CSV
 * Exports dev_old_user and dev_old_talent tables to CSV files
 * 
 * Configuration: Use environment variables:
 *   DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
 * 
 * Usage: node exportDevToCSV.js
 */

const { Client } = require('pg');
const fs = require('fs');
const path = require('path');

// ============================================================================
// CONFIGURATION - Use environment variables for white-labeled setup
// ============================================================================

const dbConfig = {
  host: process.env.DB_HOST || 'localhost',
  port: process.env.DB_PORT || 5432,
  user: process.env.DB_USER || 'ved',
  password: process.env.DB_PASSWORD || 'yourpassword',
  database: process.env.DB_NAME || 'starcast',
};

const OUTPUT_DIR = process.env.CSV_OUTPUT_DIR || __dirname;

const TABLES = {
  users: 'dev_old_user',
  talents: 'dev_old_talent',
};

const OUTPUT_FILES = {
  users: path.join(OUTPUT_DIR, 'DEV_OLD_USER.csv'),
  talents: path.join(OUTPUT_DIR, 'DEV_OLD_TALENT.csv'),
};

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Column name mapping for case normalization
 */
const COLUMN_MAPS = {
  dev_old_user: {
    createdat: 'createdAt',
    emailverified: 'emailVerified',
    updatedat: 'updatedAt',
  },
  dev_old_talent: {
    userid: 'userId',
  },
};

/**
 * Get mapped column name
 */
function getColumnName(tableName, columnName) {
  const mapping = COLUMN_MAPS[tableName] || {};
  return mapping[columnName] || columnName;
}

/**
 * Escape CSV field values
 */
function escapeCSVField(value) {
  if (value === null || value === undefined) {
    return '';
  }

  const str = String(value);

  // If contains comma, quote, or newline, wrap in quotes and escape quotes
  if (str.includes(',') || str.includes('"') || str.includes('\n')) {
    return `"${str.replace(/"/g, '""')}"`;
  }

  return str;
}

/**
 * Export table to CSV
 */
async function exportTableToCSV(client, tableName, outputFile, dataType) {
  console.log(`\n${'='.repeat(70)}`);
  console.log(`📥 EXPORTING ${dataType.toUpperCase()} TABLE: ${tableName}`);
  console.log(`${'='.repeat(70)}`);

  try {
    // Get column names
    const columnQuery = `
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = $1 
      ORDER BY ordinal_position
    `;
    const columnResult = await client.query(columnQuery, [tableName]);
    const columns = columnResult.rows.map((row) => row.column_name);

    if (columns.length === 0) {
      console.error(`❌ Table "${tableName}" not found or has no columns`);
      return { success: false, recordsExported: 0 };
    }

    console.log(`📋 Columns found: ${columns.length}`);
    const mappedColumns = columns.map((col) => getColumnName(tableName, col));
    console.log(`   ${mappedColumns.join(', ')}`);

    // Get all data
    const dataQuery = `SELECT * FROM ${tableName}`;
    const dataResult = await client.query(dataQuery);
    const records = dataResult.rows;

    console.log(`📊 Records found: ${records.length}`);

    if (records.length === 0) {
      console.log(`⚠️  No records found in table`);
      return { success: true, recordsExported: 0 };
    }

    // Build CSV content
    console.log(`📝 Building CSV...`);

    const csvLines = [];

    // Header row with mapped names
    csvLines.push(mappedColumns.map(escapeCSVField).join(','));

    // Data rows
    records.forEach((record, index) => {
      const values = columns.map((col) => escapeCSVField(record[col]));
      csvLines.push(values.join(','));

      if ((index + 1) % 500 === 0) {
        process.stdout.write('.');
      }
    });

    const csvContent = csvLines.join('\n');

    // Write to file
    console.log('\n🔍 Writing to file...');
    fs.writeFileSync(outputFile, csvContent, 'utf8');

    const fileSize = fs.statSync(outputFile).size;
    const fileSizeMB = (fileSize / (1024 * 1024)).toFixed(2);

    console.log(`\n✅ EXPORT SUMMARY:`);
    console.log(`   ✓ Records exported: ${records.length}`);
    console.log(`   ✓ File created: ${outputFile}`);
    console.log(`   ✓ File size: ${fileSizeMB} MB`);

    return { success: true, recordsExported: records.length, fileSize: fileSizeMB };
  } catch (error) {
    console.error(`❌ Export failed: ${error.message}`);
    return { success: false, recordsExported: 0, error: error.message };
  }
}

/**
 * Main execution
 */
async function main() {
  console.log(`
╔══════════════════════════════════════════════════════════════════════════╗
║              EXPORT DEV TABLES TO CSV                                    ║
║                                                                          ║
║  Database: ${dbConfig.database ? dbConfig.database.padEnd(54) : 'Configured via ENV'}║
║  Host: ${dbConfig.host}:${String(dbConfig.port).padEnd(51)}║
║  User: ${dbConfig.user.padEnd(60)}║
║  Output Dir: ${OUTPUT_DIR.padEnd(56)}║
╚══════════════════════════════════════════════════════════════════════════╝
  `);

  const client = new Client(dbConfig);

  try {
    console.log('🔗 Connecting to database...');
    await client.connect();
    console.log('✅ Connected successfully\n');

    // Export users
    const userResults = await exportTableToCSV(
      client,
      TABLES.users,
      OUTPUT_FILES.users,
      'Users'
    );

    // Export talents
    const talentResults = await exportTableToCSV(
      client,
      TABLES.talents,
      OUTPUT_FILES.talents,
      'Talents'
    );

    // Final summary
    console.log(`\n${'='.repeat(70)}`);
    console.log(`📋 FINAL SUMMARY`);
    console.log(`${'='.repeat(70)}`);

    if (userResults.success) {
      console.log(`✅ Users  - ${userResults.recordsExported} records exported (${userResults.fileSize} MB)`);
    } else {
      console.log(`❌ Users  - Export failed: ${userResults.error}`);
    }

    if (talentResults.success) {
      console.log(`✅ Talents - ${talentResults.recordsExported} records exported (${talentResults.fileSize} MB)`);
    } else {
      console.log(`❌ Talents - Export failed: ${talentResults.error}`);
    }

    const totalRecords = userResults.recordsExported + talentResults.recordsExported;

    console.log(`\n${'─'.repeat(70)}`);
    console.log(`Total Records Exported: ${totalRecords}`);
    console.log(`${'─'.repeat(70)}\n`);

    if (userResults.success && talentResults.success) {
      console.log('✨ Export completed successfully!\n');
      console.log(`📁 CSV Files Ready:`);
      console.log(`   • ${OUTPUT_FILES.users}`);
      console.log(`   • ${OUTPUT_FILES.talents}\n`);
      console.log(`Next Step: Use uploadToProd.js to import these CSVs to production database\n`);
    } else {
      console.log(`⚠️  Export completed with errors. Review logs above.\n`);
      process.exit(1);
    }
  } catch (error) {
    console.error(`\n❌ [FATAL ERROR] ${error.message}\n`);
    process.exit(1);
  } finally {
    await client.end();
    console.log('🔌 Database connection closed\n');
  }
}

// Execute
main().catch((error) => {
  console.error(`Unexpected error: ${error.message}`);
  process.exit(1);
});
