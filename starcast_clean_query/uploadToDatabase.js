const fs = require('fs');
const { parse } = require('csv-parse');
const { Pool } = require('pg');

// Database configuration
const DATABASE_URL = process.env.DATABASE_URL || "postgresql://ved:yourpassword@127.0.0.1:5432/starcast?schema=public";

const pool = new Pool({
  connectionString: DATABASE_URL,
});

/**
 * Upload CSV records to database one by one
 * @param {string} csvFilePath - Path to the CSV file
 * @param {string} tableName - Name of the table to insert into
 * @param {object} options - Additional options
 */
async function uploadCSVToDatabase(csvFilePath, tableName, options = {}) {
  const {
    skipHeader = false,
    delimiter = ',',
    onProgress = null,
    columnMapping = null, // Optional: map CSV columns to DB columns
  } = options;

  const records = [];
  let headers = [];
  let recordCount = 0;
  let successCount = 0;
  let errorCount = 0;

  return new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(parse({
        delimiter: delimiter,
        columns: true, // Automatically use first row as headers
        skip_empty_lines: true,
        trim: true,
      }))
      .on('data', (row) => {
        records.push(row);
      })
      .on('end', async () => {
        console.log(`\n📊 Total records found in CSV: ${records.length}`);
        console.log('🚀 Starting upload to database...\n');

        if (records.length === 0) {
          console.log('⚠️  No records to upload');
          await pool.end();
          return resolve({ total: 0, success: 0, errors: 0 });
        }

        // Get column names from first record
        headers = Object.keys(records[0]);
        console.log(`📋 CSV Columns: ${headers.join(', ')}\n`);

        // Process each record one by one
        for (let i = 0; i < records.length; i++) {
          const record = records[i];
          recordCount++;

          try {
            // Apply column mapping if provided
            const mappedRecord = columnMapping 
              ? Object.keys(columnMapping).reduce((acc, csvCol) => {
                  acc[columnMapping[csvCol]] = record[csvCol];
                  return acc;
                }, {})
              : record;

            // Build INSERT query dynamically
            const columns = Object.keys(mappedRecord);
            const values = Object.values(mappedRecord);
            const placeholders = columns.map((_, idx) => `$${idx + 1}`).join(', ');
            
            const query = `
              INSERT INTO ${tableName} (${columns.join(', ')})
              VALUES (${placeholders})
            `;

            await pool.query(query, values);
            successCount++;
            
            console.log(`✅ Record ${recordCount}/${records.length} uploaded successfully`);
            
            if (onProgress) {
              onProgress(recordCount, records.length, successCount, errorCount);
            }

          } catch (error) {
            errorCount++;
            console.error(`❌ Error uploading record ${recordCount}:`, error.message);
            console.error(`   Data:`, JSON.stringify(record, null, 2));
          }
        }

        console.log('\n' + '='.repeat(50));
        console.log('📈 Upload Summary:');
        console.log(`   Total Records: ${records.length}`);
        console.log(`   ✅ Successful: ${successCount}`);
        console.log(`   ❌ Failed: ${errorCount}`);
        console.log('='.repeat(50) + '\n');

        await pool.end();
        resolve({ total: records.length, success: successCount, errors: errorCount });
      })
      .on('error', (error) => {
        console.error('Error reading CSV file:', error);
        reject(error);
      });
  });
}

/**
 * Main execution function
 */
async function main() {
  // Configuration - Update these values based on your needs
  const csvFilePath = process.argv[2] || './your-file.csv'; // CSV file path from command line or default
  const tableName = process.argv[3] || 'your_table_name'; // Table name from command line or default

  console.log('\n🗄️  Database CSV Uploader');
  console.log('='.repeat(50));
  console.log(`📁 CSV File: ${csvFilePath}`);
  console.log(`📊 Target Table: ${tableName}`);
  console.log(`🔗 Database: ${DATABASE_URL.split('@')[1]}`);
  console.log('='.repeat(50) + '\n');

  // Check if CSV file exists
  if (!fs.existsSync(csvFilePath)) {
    console.error(`❌ Error: CSV file not found at ${csvFilePath}`);
    process.exit(1);
  }

  try {
    // Test database connection
    console.log('🔍 Testing database connection...');
    const client = await pool.connect();
    console.log('✅ Database connected successfully!\n');
    client.release();

    // Upload CSV to database
    const result = await uploadCSVToDatabase(csvFilePath, tableName, {
      delimiter: ',',
      onProgress: (current, total, success, errors) => {
        // Optional: Custom progress handler
      },
      // Example column mapping (uncomment and modify if needed):
      // columnMapping: {
      //   'CSV Column Name': 'db_column_name',
      //   'Another CSV Column': 'another_db_column'
      // }
    });

    if (result.errors === 0) {
      console.log('🎉 All records uploaded successfully!');
      process.exit(0);
    } else {
      console.log('⚠️  Upload completed with some errors');
      process.exit(1);
    }

  } catch (error) {
    console.error('❌ Fatal error:', error.message);
    await pool.end();
    process.exit(1);
  }
}

// Run the script
if (require.main === module) {
  main();
}

module.exports = { uploadCSVToDatabase };
