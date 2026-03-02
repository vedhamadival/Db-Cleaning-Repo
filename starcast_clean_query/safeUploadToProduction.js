const fs = require('fs');
const { parse } = require('csv-parse');
const { Pool } = require('pg');

// Database configuration - PRODUCTION
const DATABASE_URL = process.env.DATABASE_URL || "postgresql://ved:yourpassword@127.0.0.1:5432/starcast?schema=public";

const pool = new Pool({
  connectionString: DATABASE_URL,
});

// Table name - PRODUCTION TABLE
const TABLE_NAME = 'Talent';

// Boolean columns
const BOOLEAN_COLUMNS = [
  'have_license', 'have_training', 'member_union', 'gst_compliant',
  'have_been_background_artist', 'have_theatre_experience',
  'flight_direct_flight_only', 'applyingForTalentManagement', 'terms_accepted'
];

// Numeric columns
const NUMERIC_COLUMNS = [
  'ankle', 'ankle_female', 'arm_around', 'arm_hole', 'armhole', 'back_depth',
  'bicep', 'bust', 'calf', 'calf_female', 'catalogue_lookbook_fee_per_change',
  'chest', 'collab_fee', 'collar', 'cross_back', 'cross_front', 'cuff',
  'daily_tv_show_per_day_shoot_fee', 'dart_point', 'dubbing_fee_per_minute',
  'dubbing_fee_per_minute_other_lang', 'elbow', 'feature_film_per_day_fee',
  'fee_per_minute', 'fee_per_post_view', 'front_crotch', 'full_crotch',
  'hip', 'hips', 'hosting_per_event_day_fee', 'in_seam', 'jacket_length',
  'knee', 'knee_female', 'low_waist', 'lower_chest', 'lower_waist', 'mid_waist',
  'nape_to_hips', 'nape_to_waist', 'neck_round', 'per_speaking_engagement_fee',
  'per_spoken_word_fee', 'presenting_per_shoot_day_fee', 'ramp_walk_fee_per_day',
  'ramp_walk_fee_per_show', 'reels_post_fee', 'short_film_per_day_shoot_fee',
  'shoulder', 'shoulder_to_stomach', 'shoulders', 'sleeve', 'sleeve_length',
  'static_post_fee', 'stomach', 'story_fee', 'story_re_share_fee', 'thigh',
  'thigh_round', 'trouser_length', 'upper_chest', 'upper_thigh', 'upper_waist',
  'waist', 'web_series_per_day_shoot_fee', 'wrist',
  'actor_digital_ad_per_day_shoot_five_year_usage_fee',
  'actor_digital_ad_per_day_shoot_one_year_usage_fee',
  'actor_digital_ad_per_day_shoot_perpetuity_usage_fee',
  'actor_digital_ad_per_day_shoot_six_months_usage_fee',
  'actor_digital_ad_per_day_shoot_three_months_usage_fee',
  'actor_digital_ad_per_day_shoot_three_year_usage_fee',
  'actor_digital_ad_per_day_shoot_two_year_usage_fee',
  'actor_digital_print_ad_per_day_shoot_five_year_usage_fee',
  'actor_digital_print_ad_per_day_shoot_one_year_usage_fee',
  'actor_digital_print_ad_per_day_shoot_perpetuity_usage_fee',
  'actor_digital_print_ad_per_day_shoot_six_months_usage_fee',
  'actor_digital_print_ad_per_day_shoot_three_months_usage_fee',
  'actor_digital_print_ad_per_day_shoot_three_year_usage_fee',
  'actor_digital_print_ad_per_day_shoot_two_year_usage_fee',
  'actor_print_ad_per_day_shoot_five_year_usage_fee',
  'actor_print_ad_per_day_shoot_one_year_usage_fee',
  'actor_print_ad_per_day_shoot_perpetuity_usage_fee',
  'actor_print_ad_per_day_shoot_six_months_usage_fee',
  'actor_print_ad_per_day_shoot_three_months_usage_fee',
  'actor_print_ad_per_day_shoot_three_year_usage_fee',
  'actor_print_ad_per_day_shoot_two_year_usage_fee',
  'actor_social_media_ad_per_day_shoot_five_year_usage_fee',
  'actor_social_media_ad_per_day_shoot_one_year_usage_fee',
  'actor_social_media_ad_per_day_shoot_perpetuity_usage_fee',
  'actor_social_media_ad_per_day_shoot_six_months_usage_fee',
  'actor_social_media_ad_per_day_shoot_three_months_usage_fee',
  'actor_social_media_ad_per_day_shoot_three_year_usage_fee',
  'actor_social_media_ad_per_day_shoot_two_year_usage_fee',
  'actor_tv_commercial_per_day_shoot_five_year_usage_fee',
  'actor_tv_commercial_per_day_shoot_one_year_usage_fee',
  'actor_tv_commercial_per_day_shoot_perpetuity_usage_fee',
  'actor_tv_commercial_per_day_shoot_three_year_usage_fee',
  'actor_tv_commercial_per_day_shoot_two_year_usage_fee',
  'model_digital_ad_per_day_shoot_five_year_usage_fee',
  'model_digital_ad_per_day_shoot_one_year_usage_fee',
  'model_digital_ad_per_day_shoot_perpetuity_usage_fee',
  'model_digital_ad_per_day_shoot_six_months_usage_fee',
  'model_digital_ad_per_day_shoot_three_months_usage_fee',
  'model_digital_ad_per_day_shoot_three_year_usage_fee',
  'model_digital_ad_per_day_shoot_two_year_usage_fee',
  'model_digital_print_ad_per_day_shoot_five_year_usage_fee',
  'model_digital_print_ad_per_day_shoot_one_year_usage_fee',
  'model_digital_print_ad_per_day_shoot_perpetuity_usage_fee',
  'model_digital_print_ad_per_day_shoot_six_months_usage_fee',
  'model_digital_print_ad_per_day_shoot_three_months_usage_fee',
  'model_digital_print_ad_per_day_shoot_three_year_usage_fee',
  'model_digital_print_ad_per_day_shoot_two_year_usage_fee',
  'model_print_ad_per_day_shoot_five_year_usage_fee',
  'model_print_ad_per_day_shoot_one_year_usage_fee',
  'model_print_ad_per_day_shoot_perpetuity_usage_fee',
  'model_print_ad_per_day_shoot_six_months_usage_fee',
  'model_print_ad_per_day_shoot_three_months_usage_fee',
  'model_print_ad_per_day_shoot_three_year_usage_fee',
  'model_print_ad_per_day_shoot_two_year_usage_fee',
  'model_social_media_ad_per_day_shoot_five_year_usage_fee',
  'model_social_media_ad_per_day_shoot_one_year_usage_fee',
  'model_social_media_ad_per_day_shoot_perpetuity_usage_fee',
  'model_social_media_ad_per_day_shoot_six_months_usage_fee',
  'model_social_media_ad_per_day_shoot_three_months_usage_fee',
  'model_social_media_ad_per_day_shoot_three_year_usage_fee',
  'model_social_media_ad_per_day_shoot_two_year_usage_fee',
  'model_tv_commercial_per_day_shoot_five_year_usage_fee',
  'model_tv_commercial_per_day_shoot_one_year_usage_fee',
  'model_tv_commercial_per_day_shoot_perpetuity_usage_fee',
  'model_tv_commercial_per_day_shoot_three_year_usage_fee',
  'model_tv_commercial_per_day_shoot_two_year_usage_fee',
  'radio_ad_fee',
  'vo_digital_ad_per_day_shoot_five_year_usage_fee',
  'vo_digital_ad_per_day_shoot_one_year_usage_fee',
  'vo_digital_ad_per_day_shoot_perpetuity_usage_fee',
  'vo_digital_ad_per_day_shoot_six_months_usage_fee',
  'vo_digital_ad_per_day_shoot_three_months_usage_fee',
  'vo_digital_ad_per_day_shoot_three_year_usage_fee',
  'vo_digital_ad_per_day_shoot_two_year_usage_fee',
  'vo_digital_print_ad_per_day_shoot_five_year_usage_fee',
  'vo_digital_print_ad_per_day_shoot_one_year_usage_fee',
  'vo_digital_print_ad_per_day_shoot_perpetuity_usage_fee',
  'vo_digital_print_ad_per_day_shoot_six_months_usage_fee',
  'vo_digital_print_ad_per_day_shoot_three_months_usage_fee',
  'vo_digital_print_ad_per_day_shoot_three_year_usage_fee',
  'vo_digital_print_ad_per_day_shoot_two_year_usage_fee',
  'vo_print_ad_per_day_shoot_five_year_usage_fee',
  'vo_print_ad_per_day_shoot_one_year_usage_fee',
  'vo_print_ad_per_day_shoot_perpetuity_usage_fee',
  'vo_print_ad_per_day_shoot_six_months_usage_fee',
  'vo_print_ad_per_day_shoot_three_months_usage_fee',
  'vo_print_ad_per_day_shoot_three_year_usage_fee',
  'vo_print_ad_per_day_shoot_two_year_usage_fee',
  'vo_social_media_ad_per_day_shoot_five_year_usage_fee',
  'vo_social_media_ad_per_day_shoot_one_year_usage_fee',
  'vo_social_media_ad_per_day_shoot_perpetuity_usage_fee',
  'vo_social_media_ad_per_day_shoot_six_months_usage_fee',
  'vo_social_media_ad_per_day_shoot_three_months_usage_fee',
  'vo_social_media_ad_per_day_shoot_three_year_usage_fee',
  'vo_social_media_ad_per_day_shoot_two_year_usage_fee',
  'vo_tv_commercial_per_day_shoot_five_year_usage_fee',
  'vo_tv_commercial_per_day_shoot_one_year_usage_fee',
  'vo_tv_commercial_per_day_shoot_perpetuity_usage_fee',
  'vo_tv_commercial_per_day_shoot_three_year_usage_fee',
  'vo_tv_commercial_per_day_shoot_two_year_usage_fee',
  'acting_workshops_per_day_shoot_fee',
  'actor_fee_per_appearance',
  'content_creator_fee_per_appearance',
  'model_fee_per_appearance'
];

/**
 * Convert value based on column type
 */
function convertValue(columnName, value) {
  if (!value || value.trim() === '') {
    return null;
  }
  
  if (BOOLEAN_COLUMNS.includes(columnName)) {
    const upperVal = value.toUpperCase();
    if (upperVal === 'TRUE') return true;
    if (upperVal === 'FALSE') return false;
    return null;
  }
  
  if (NUMERIC_COLUMNS.includes(columnName)) {
    const num = parseFloat(value);
    return isNaN(num) ? null : num;
  }
  
  return value;
}

/**
 * Safely upload CSV records to database - PRODUCTION SAFE
 */
async function safeUploadToProduction(csvFilePath, logFile) {
  const records = [];
  let successCount = 0;
  let duplicateCount = 0;
  let errorCount = 0;
  const duplicateRecords = [];
  const errorRecords = [];

  // Create log file
  const logStream = fs.createWriteStream(logFile, { flags: 'w' });
  
  function log(message) {
    console.log(message);
    logStream.write(message + '\n');
  }

  return new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(parse({
        delimiter: ',',
        columns: true,
        skip_empty_lines: true,
        trim: true,
      }))
      .on('data', (row) => {
        records.push(row);
      })
      .on('end', async () => {
        log(`\n${'='.repeat(70)}`);
        log(`📊 CSV Analysis`);
        log(`${'='.repeat(70)}`);
        log(`Total records in CSV: ${records.length}`);
        log(`Starting safe upload to PRODUCTION table: "${TABLE_NAME}"`);
        log(`${'='.repeat(70)}\n`);

        if (records.length === 0) {
          log('⚠️  No records to upload');
          logStream.end();
          await pool.end();
          return resolve({ total: 0, success: 0, duplicates: 0, errors: 0 });
        }

        // First, check existing record count
        try {
          const countResult = await pool.query(`SELECT COUNT(*) FROM "${TABLE_NAME}"`);
          const existingCount = parseInt(countResult.rows[0].count);
          log(`✅ Current records in database: ${existingCount}\n`);
          log(`${'='.repeat(70)}`);
          log(`🚀 Starting upload process...`);
          log(`${'='.repeat(70)}\n`);
        } catch (error) {
          log(`❌ Error checking existing records: ${error.message}`);
          logStream.end();
          await pool.end();
          return reject(error);
        }

        // Process each record one by one
        for (let i = 0; i < records.length; i++) {
          const record = records[i];
          const recordCount = i + 1;

          try {
            // Check if record already exists (by id)
            const checkQuery = `SELECT id FROM "${TABLE_NAME}" WHERE id = $1`;
            const checkResult = await pool.query(checkQuery, [record.id]);

            if (checkResult.rows.length > 0) {
              // Record already exists - SKIP
              duplicateCount++;
              const dupInfo = {
                csvRow: recordCount,
                id: record.id,
                username: record.username,
                first_name: record.first_name,
                last_name: record.last_name,
                contact_email: record.contact_email
              };
              duplicateRecords.push(dupInfo);
              log(`⏭️  SKIPPED (Duplicate) [${recordCount}/${records.length}] ID: ${record.id} - ${record.first_name} ${record.last_name}`);
              continue;
            }

            // Convert values based on column types
            const columns = Object.keys(record);
            const values = columns.map(col => convertValue(col, record[col]));
            
            // Build INSERT query with ON CONFLICT DO NOTHING for extra safety
            const columnList = columns.map(col => `"${col}"`).join(', ');
            const placeholders = columns.map((_, idx) => `$${idx + 1}`).join(', ');
            
            const query = `
              INSERT INTO "${TABLE_NAME}" (${columnList}) 
              VALUES (${placeholders})
              ON CONFLICT (id) DO NOTHING
            `;

            const result = await pool.query(query, values);
            
            if (result.rowCount > 0) {
              successCount++;
              log(`✅ INSERTED [${recordCount}/${records.length}] ID: ${record.id} - ${record.first_name} ${record.last_name}`);
            } else {
              // Conflict occurred (shouldn't happen as we checked above, but just in case)
              duplicateCount++;
              log(`⏭️  SKIPPED (Conflict) [${recordCount}/${records.length}] ID: ${record.id}`);
            }

          } catch (error) {
            errorCount++;
            const errInfo = {
              csvRow: recordCount,
              id: record.id,
              username: record.username,
              first_name: record.first_name,
              last_name: record.last_name,
              error: error.message
            };
            errorRecords.push(errInfo);
            log(`❌ ERROR [${recordCount}/${records.length}] ID: ${record.id} - ${error.message}`);
          }
        }

        // Final summary
        log(`\n${'='.repeat(70)}`);
        log(`📈 UPLOAD SUMMARY`);
        log(`${'='.repeat(70)}`);
        log(`Total CSV Records:        ${records.length}`);
        log(`✅ Successfully Inserted: ${successCount}`);
        log(`⏭️  Skipped (Duplicates):  ${duplicateCount}`);
        log(`❌ Failed (Errors):       ${errorCount}`);
        log(`${'='.repeat(70)}\n`);

        // Verify final count
        try {
          const finalCountResult = await pool.query(`SELECT COUNT(*) FROM "${TABLE_NAME}"`);
          const finalCount = parseInt(finalCountResult.rows[0].count);
          log(`📊 Final database record count: ${finalCount}\n`);
        } catch (error) {
          log(`⚠️  Could not verify final count: ${error.message}\n`);
        }

        // Log duplicate details
        if (duplicateRecords.length > 0) {
          log(`\n${'='.repeat(70)}`);
          log(`📋 DUPLICATE RECORDS (${duplicateRecords.length})`);
          log(`${'='.repeat(70)}`);
          duplicateRecords.forEach((dup, idx) => {
            log(`${idx + 1}. CSV Row ${dup.csvRow}: ID=${dup.id}, Name="${dup.first_name} ${dup.last_name}", Email=${dup.contact_email || 'N/A'}`);
          });
        }

        // Log error details
        if (errorRecords.length > 0) {
          log(`\n${'='.repeat(70)}`);
          log(`❌ ERROR RECORDS (${errorRecords.length})`);
          log(`${'='.repeat(70)}`);
          errorRecords.forEach((err, idx) => {
            log(`${idx + 1}. CSV Row ${err.csvRow}: ID=${err.id}, Name="${err.first_name} ${err.last_name}"`);
            log(`   Error: ${err.error}`);
          });
        }

        log(`\n${'='.repeat(70)}`);
        log(`✅ Upload process completed safely!`);
        log(`Log file saved to: ${logFile}`);
        log(`${'='.repeat(70)}\n`);

        logStream.end();
        await pool.end();
        resolve({ 
          total: records.length, 
          success: successCount, 
          duplicates: duplicateCount, 
          errors: errorCount,
          duplicateRecords,
          errorRecords
        });
      })
      .on('error', (error) => {
        log(`❌ Error reading CSV file: ${error.message}`);
        logStream.end();
        reject(error);
      });
  });
}

/**
 * Main execution function
 */
async function main() {
  const csvFilePath = process.argv[2] || './Old Starcast Talents - Remaining.csv';
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const logFile = `upload-log-${timestamp}.txt`;

  console.log('\n🔐 SAFE PRODUCTION DATABASE UPLOAD');
  console.log('='.repeat(70));
  console.log(`📁 CSV File: ${csvFilePath}`);
  console.log(`📊 Target Table: ${TABLE_NAME} (PRODUCTION)`);
  console.log(`🔗 Database: ${DATABASE_URL.split('@')[1]}`);
  console.log(`📝 Log File: ${logFile}`);
  console.log('='.repeat(70));
  console.log('⚠️  PRODUCTION MODE: Existing data will NOT be modified');
  console.log('⚠️  Duplicates will be skipped based on ID');
  console.log('='.repeat(70) + '\n');

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

    // Upload data safely
    const result = await safeUploadToProduction(csvFilePath, logFile);

    console.log(`\n📊 Final Results:`);
    console.log(`   New records added: ${result.success}`);
    console.log(`   Duplicates skipped: ${result.duplicates}`);
    console.log(`   Errors: ${result.errors}`);
    console.log(`\n📄 Detailed log saved to: ${logFile}\n`);

    if (result.errors === 0) {
      console.log('✅ Upload completed successfully!');
      process.exit(0);
    } else {
      console.log('⚠️  Upload completed with some errors (check log file)');
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

module.exports = { safeUploadToProduction };
