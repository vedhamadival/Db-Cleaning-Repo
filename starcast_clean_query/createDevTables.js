const fs = require("fs");
const path = require("path");
const { Client } = require("pg");
const { parse } = require("csv-parse/sync");

// ============================================
// DATABASE CONNECTION - STARCAST
// ============================================
const client = new Client({
  user: "ved",
  host: "localhost",
  database: "starcast",
  password: "yourpassword",
  port: 5432,
});

// ============================================
// CSV FILES TO IMPORT
// ============================================
const CSV_FILES = [
  {
    path: "../../User (3).csv",
    tableName: "dev_user",
  },
  {
    path: "../../Talent (4).csv",
    tableName: "dev_talent",
  },
];

// ============================================
// CREATE TABLES
// ============================================
async function createTables() {
  try {
    console.log("🧱 Creating DEV tables...\n");

    // Drop existing tables
    await client.query('DROP TABLE IF EXISTS dev_talent CASCADE');
    await client.query('DROP TABLE IF EXISTS dev_user CASCADE');
    await client.query('DROP TABLE IF EXISTS dev_old_talent CASCADE');
    await client.query('DROP TABLE IF EXISTS dev_old_user CASCADE');
    console.log("   ✅ Dropped existing tables (if any)");

    // Create dev_user table (matching Prisma User model)
    await client.query(`
      CREATE TABLE dev_user (
        "id" VARCHAR(50) PRIMARY KEY,
        "name" VARCHAR(255),
        "email" VARCHAR(255) UNIQUE,
        "createdAt" TIMESTAMP,
        "emailVerified" TIMESTAMP,
        "image" TEXT,
        "updatedAt" TIMESTAMP,
        "password" VARCHAR(255),
        "role" VARCHAR(50) DEFAULT 'Talent'
      );
    `);
    console.log("   ✅ Created dev_user table");

    // Create dev_talent table (matching Prisma Talent model - all TEXT columns)
    await client.query(`
      CREATE TABLE dev_talent (
        "id" VARCHAR(50) PRIMARY KEY,
        "userId" TEXT,
        "first_name" TEXT,
        "last_name" TEXT,
        "passport_name" TEXT,
        "gender" TEXT,
        "date_of_birth" TEXT,
        "nationality" TEXT,
        "hometown" TEXT,
        "current_city" TEXT,
        "bio" TEXT,
        "contact_number" TEXT,
        "whatsapp_contact_number" TEXT,
        "contact_email" TEXT,
        "contact_address" TEXT,
        "joined_date_starcast" TEXT,
        "talent_agent_name" TEXT,
        "talent_agent_type" TEXT,
        "talent_agency_company_name" TEXT,
        "talent_agent_number" TEXT,
        "talent_agent_email" TEXT,
        "talent_agent_designation" TEXT,
        "ethnicity" TEXT,
        "build" TEXT,
        "complexion" TEXT,
        "eye_colour" TEXT,
        "hair_colour" TEXT,
        "hair_length" TEXT,
        "have_license" TEXT,
        "have_training" TEXT,
        "training" TEXT,
        "member_union" TEXT,
        "union_name" TEXT,
        "headshot" TEXT,
        "midshot" TEXT,
        "longshot" TEXT,
        "side_profile" TEXT,
        "intro_link" TEXT,
        "work_links" TEXT,
        "showreel" TEXT,
        "monologue" TEXT,
        "auditions" TEXT,
        "starcast_profile_link" TEXT,
        "imdb_link" TEXT,
        "youtube_channel" TEXT,
        "instagram_profile" TEXT,
        "facebook_page" TEXT,
        "x_handle" TEXT,
        "website_link" TEXT,
        "status" TEXT,
        "created_at" TEXT,
        "updated_at" TEXT,
        "ankle" TEXT,
        "ankle_female" TEXT,
        "arm_around" TEXT,
        "arm_hole" TEXT,
        "armhole" TEXT,
        "back_depth" TEXT,
        "bank_account_number" TEXT,
        "bank_name" TEXT,
        "bicep" TEXT,
        "branch_address" TEXT,
        "bust" TEXT,
        "ca_company_name" TEXT,
        "ca_contact_number" TEXT,
        "ca_designation" TEXT,
        "ca_email_id" TEXT,
        "ca_name" TEXT,
        "calf" TEXT,
        "calf_female" TEXT,
        "catalogue_lookbook_fee_per_change" TEXT,
        "chest" TEXT,
        "collab_fee" TEXT,
        "collar" TEXT,
        "cross_back" TEXT,
        "cross_front" TEXT,
        "cuff" TEXT,
        "daily_tv_show_per_day_shoot_fee" TEXT,
        "dart_point" TEXT,
        "dubbing_fee_per_minute" TEXT,
        "dubbing_fee_per_minute_other_lang" TEXT,
        "elbow" TEXT,
        "feature_film_per_day_fee" TEXT,
        "fee_per_minute" TEXT,
        "fee_per_post_view" TEXT,
        "front_crotch" TEXT,
        "full_crotch" TEXT,
        "gst_compliant" TEXT,
        "gstin_number" TEXT,
        "have_been_background_artist" TEXT,
        "have_theatre_experience" TEXT,
        "height" TEXT,
        "hip" TEXT,
        "hips" TEXT,
        "hosting_per_event_day_fee" TEXT,
        "ifsc_code" TEXT,
        "in_seam" TEXT,
        "jacket_length" TEXT,
        "knee" TEXT,
        "knee_female" TEXT,
        "legal_address" TEXT,
        "legal_name" TEXT,
        "low_waist" TEXT,
        "lower_chest" TEXT,
        "lower_waist" TEXT,
        "mid_waist" TEXT,
        "nape_to_hips" TEXT,
        "nape_to_waist" TEXT,
        "neck_round" TEXT,
        "pan_number" TEXT,
        "payment_transfer_notes" TEXT,
        "per_speaking_engagement_fee" TEXT,
        "per_spoken_word_fee" TEXT,
        "presenting_per_shoot_day_fee" TEXT,
        "ramp_walk_fee_per_day" TEXT,
        "ramp_walk_fee_per_show" TEXT,
        "reels_post_fee" TEXT,
        "shoe_size" TEXT,
        "short_film_per_day_shoot_fee" TEXT,
        "shoulder" TEXT,
        "shoulder_to_stomach" TEXT,
        "shoulders" TEXT,
        "sleeve" TEXT,
        "sleeve_length" TEXT,
        "static_post_fee" TEXT,
        "stomach" TEXT,
        "story_fee" TEXT,
        "story_re_share_fee" TEXT,
        "theatre_experience_notes" TEXT,
        "thigh" TEXT,
        "thigh_round" TEXT,
        "trouser_length" TEXT,
        "upper_chest" TEXT,
        "upper_thigh" TEXT,
        "upper_waist" TEXT,
        "voice_age_range" TEXT,
        "waist" TEXT,
        "web_series_per_day_shoot_fee" TEXT,
        "wrist" TEXT,
        "actor_digital_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "actor_digital_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "actor_digital_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "actor_digital_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "actor_digital_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "actor_digital_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "actor_digital_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "actor_digital_print_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "actor_digital_print_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "actor_digital_print_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "actor_digital_print_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "actor_digital_print_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "actor_digital_print_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "actor_digital_print_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "actor_print_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "actor_print_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "actor_print_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "actor_print_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "actor_print_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "actor_print_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "actor_print_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "actor_social_media_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "actor_social_media_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "actor_social_media_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "actor_social_media_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "actor_social_media_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "actor_social_media_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "actor_social_media_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "actor_tv_commercial_per_day_shoot_five_year_usage_fee" TEXT,
        "actor_tv_commercial_per_day_shoot_one_year_usage_fee" TEXT,
        "actor_tv_commercial_per_day_shoot_perpetuity_usage_fee" TEXT,
        "actor_tv_commercial_per_day_shoot_three_year_usage_fee" TEXT,
        "actor_tv_commercial_per_day_shoot_two_year_usage_fee" TEXT,
        "age_range" TEXT,
        "model_digital_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "model_digital_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "model_digital_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "model_digital_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "model_digital_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "model_digital_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "model_digital_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "model_digital_print_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "model_digital_print_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "model_digital_print_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "model_digital_print_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "model_digital_print_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "model_digital_print_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "model_digital_print_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "model_print_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "model_print_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "model_print_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "model_print_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "model_print_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "model_print_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "model_print_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "model_social_media_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "model_social_media_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "model_social_media_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "model_social_media_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "model_social_media_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "model_social_media_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "model_social_media_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "model_tv_commercial_per_day_shoot_five_year_usage_fee" TEXT,
        "model_tv_commercial_per_day_shoot_one_year_usage_fee" TEXT,
        "model_tv_commercial_per_day_shoot_perpetuity_usage_fee" TEXT,
        "model_tv_commercial_per_day_shoot_three_year_usage_fee" TEXT,
        "model_tv_commercial_per_day_shoot_two_year_usage_fee" TEXT,
        "radio_ad_fee" TEXT,
        "vo_digital_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "vo_digital_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "vo_digital_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "vo_digital_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "vo_digital_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "vo_digital_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "vo_digital_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "vo_digital_print_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "vo_digital_print_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "vo_digital_print_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "vo_digital_print_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "vo_digital_print_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "vo_digital_print_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "vo_digital_print_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "vo_print_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "vo_print_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "vo_print_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "vo_print_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "vo_print_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "vo_print_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "vo_print_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "vo_social_media_ad_per_day_shoot_five_year_usage_fee" TEXT,
        "vo_social_media_ad_per_day_shoot_one_year_usage_fee" TEXT,
        "vo_social_media_ad_per_day_shoot_perpetuity_usage_fee" TEXT,
        "vo_social_media_ad_per_day_shoot_six_months_usage_fee" TEXT,
        "vo_social_media_ad_per_day_shoot_three_months_usage_fee" TEXT,
        "vo_social_media_ad_per_day_shoot_three_year_usage_fee" TEXT,
        "vo_social_media_ad_per_day_shoot_two_year_usage_fee" TEXT,
        "vo_tv_commercial_per_day_shoot_five_year_usage_fee" TEXT,
        "vo_tv_commercial_per_day_shoot_one_year_usage_fee" TEXT,
        "vo_tv_commercial_per_day_shoot_perpetuity_usage_fee" TEXT,
        "vo_tv_commercial_per_day_shoot_three_year_usage_fee" TEXT,
        "vo_tv_commercial_per_day_shoot_two_year_usage_fee" TEXT,
        "absolute_no_nos" TEXT,
        "awards_achievements" TEXT,
        "differences_current_vs_desired" TEXT,
        "expectations_from_starcast" TEXT,
        "if_no_limits_what_would_you_do" TEXT,
        "inspiring_role_models" TEXT,
        "past_experience_and_projects" TEXT,
        "people_already_met" TEXT,
        "projects_under_consideration" TEXT,
        "steps_willing_to_work_on" TEXT,
        "talent_type" TEXT,
        "tell_us_about_yourself" TEXT,
        "what_could_you_do_right_now" TEXT,
        "when_will_this_take_place" TEXT,
        "wishlist_collaborations" TEXT,
        "acting_workshops_per_day_shoot_fee" TEXT,
        "talent_starcast_agent_name" TEXT,
        "talent_agency_status" TEXT,
        "unique_physical_characteristics" TEXT,
        "license_type" TEXT,
        "hair_type" TEXT,
        "accommodation_sharing_preference" TEXT,
        "accommodation_special_preferences" TEXT,
        "accommodation_type" TEXT,
        "flight_class" TEXT,
        "flight_direct_flight_only" TEXT,
        "flight_frequent_flyer_program" TEXT,
        "flight_preference" TEXT,
        "flight_preferred_airlines" TEXT,
        "flight_seat_type" TEXT,
        "meal_dietary_preference" TEXT,
        "meal_health_nutrition_preferences" TEXT,
        "meal_special_food_preference" TEXT,
        "vanity_request_type" TEXT,
        "vehicle_additional_requirements" TEXT,
        "vehicle_car_type" TEXT,
        "vehicle_engine_or_fuel_type" TEXT,
        "vehicle_seating_type" TEXT,
        "vehicle_transmission_type" TEXT,
        "facial_hair" TEXT,
        "languages" TEXT,
        "skills" TEXT,
        "working_well_current" TEXT,
        "actor_fee_per_appearance" TEXT,
        "content_creator_fee_per_appearance" TEXT,
        "model_fee_per_appearance" TEXT,
        "username" TEXT,
        "applyingForTalentManagement" TEXT,
        "terms_accepted" TEXT,
        "middle_name" TEXT,
        "influencer_category" TEXT,
        "presenter_category" TEXT,
        "public_figure_type" TEXT,
        "current_state" TEXT
      );
    `);
    console.log("   ✅ Created dev_talent table");

    // Create dev_old_user table (copy of clean_users structure)
    await client.query(`
      CREATE TABLE dev_old_user AS SELECT * FROM clean_users;
    `);
    console.log("   ✅ Created dev_old_user table (from clean_users)");

    // Create dev_old_talent table (copy of clean_talents structure)
    await client.query(`
      CREATE TABLE dev_old_talent AS SELECT * FROM clean_talents;
    `);
    console.log("   ✅ Created dev_old_talent table (from clean_talents)\n");

  } catch (error) {
    console.error("   ❌ Error creating tables:", error.message);
    throw error;
  }
}

// ============================================
// PARSE CSV FILE
// ============================================
function parseCSVFile(filePath) {
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
// CONVERT VALUE TO PROPER TYPE
// ============================================
function convertValue(value) {
  if (
    value === null ||
    value === undefined ||
    value === "" ||
    value === "null" ||
    value === "NULL"
  ) {
    return null;
  }

  let strValue = String(value).trim();

  // Remove " AD" suffix from timestamps
  if (strValue.includes(" AD")) {
    strValue = strValue.replace(" AD", "").trim();
  }

  if (!strValue || strValue === "null" || strValue === "NULL") {
    return null;
  }

  // Try to parse as boolean
  if (strValue === "true" || strValue === "True" || strValue === "TRUE") {
    return true;
  }
  if (strValue === "false" || strValue === "False" || strValue === "FALSE") {
    return false;
  }

  // Try to parse as ISO timestamp
  if (strValue.includes("T") && (strValue.includes("Z") || strValue.includes("+") || strValue.includes("-"))) {
    try {
      const date = new Date(strValue);
      if (!isNaN(date.getTime())) {
        return date.toISOString();
      }
    } catch (e) {
      // Not a valid timestamp, continue to next checks
    }
  }

  // Try to parse as number
  const num = parseFloat(strValue);
  if (!isNaN(num) && strValue !== "") {
    // Check if it's an integer
    if (Number.isInteger(num)) {
      return num;
    }
    // It's a float, but still return as is
    return num;
  }

  return strValue;
}

// ============================================
// INSERT SINGLE RECORD
// ============================================
async function insertSingleRecord(tableName, record, allColumns) {
  const values = [];
  const columns = [];

  for (const col of allColumns) {
    if (record.hasOwnProperty(col)) {
      const value = record[col];
      const convertedValue = convertValue(value);
      columns.push(`"${col}"`);
      values.push(convertedValue);
    }
  }

  if (columns.length === 0) {
    throw new Error("No valid columns to insert");
  }

  const columnList = columns.join(",");
  const valuesList = columns.map((_, idx) => `$${idx + 1}`).join(",");
  const insertSQL = `INSERT INTO "${tableName}" (${columnList}) VALUES (${valuesList})`;

  await client.query(insertSQL, values);
}

// ============================================
// INSERT DATA - Row by Row
// ============================================
async function insertDataRowByRow(tableName, records) {
  try {
    const allColumns = Object.keys(records[0]);
    let insertedCount = 0;
    const failedRecords = [];

    console.log(`   📋 Total columns: ${allColumns.length}`);
    console.log(`   📊 Processing ${records.length} records...\n`);

    for (let i = 0; i < records.length; i++) {
      const record = records[i];

      try {
        await insertSingleRecord(tableName, record, allColumns);
        insertedCount++;

        if ((i + 1) % 100 === 0 || i === 0 || i === records.length - 1) {
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
    } else {
      console.log(`   🎉 NO ERRORS - All records inserted!`);
    }

    return insertedCount;
  } catch (error) {
    console.log(`   ❌ Fatal error during insertion: ${error.message}`);
    return 0;
  }
}

// ============================================
// IMPORT CSV TO DATABASE
// ============================================
async function importCSVToDatabase(csvConfig) {
  const csvFile = csvConfig.path;
  const tableName = csvConfig.tableName;

  console.log(`\n📥 Processing: ${path.basename(csvFile)}`);
  console.log("─".repeat(60));

  try {
    console.log("   📖 Parsing CSV...");
    const records = parseCSVFile(csvFile);
    if (!records) return false;
    console.log(`   ✅ Found ${records.length} records`);

    console.log("   📤 Inserting data...");
    const inserted = await insertDataRowByRow(tableName, records);
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
  console.log("📊 CREATE DEV TABLES (CSV DATA + CLEAN DATA)");
  console.log("=".repeat(60));
  console.log();

  try {
    await client.connect();
    console.log("✅ Connected to starcast database\n");

    // Create all tables
    await createTables();

    let successCount = 0;
    let failureCount = 0;

    // Process CSV files
    for (const csvConfig of CSV_FILES) {
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
    console.log("\n" + "─".repeat(60));
    console.log("🔍 Final Verification:");
    console.log("─".repeat(60));

    const devUserCount = await client.query('SELECT COUNT(*) FROM dev_user');
    const devTalentCount = await client.query('SELECT COUNT(*) FROM dev_talent');
    const devOldUserCount = await client.query('SELECT COUNT(*) FROM dev_old_user');
    const devOldTalentCount = await client.query('SELECT COUNT(*) FROM dev_old_talent');

    console.log(`   📊 dev_user records: ${devUserCount.rows[0].count}`);
    console.log(`   📊 dev_talent records: ${devTalentCount.rows[0].count}`);
    console.log(`   📊 dev_old_user records: ${devOldUserCount.rows[0].count}`);
    console.log(`   📊 dev_old_talent records: ${devOldTalentCount.rows[0].count}`);

    const totalCount = parseInt(devUserCount.rows[0].count) + 
                      parseInt(devTalentCount.rows[0].count) + 
                      parseInt(devOldUserCount.rows[0].count) + 
                      parseInt(devOldTalentCount.rows[0].count);

    console.log(`\n   🔢 TOTAL RECORDS ACROSS ALL DEV TABLES: ${totalCount}\n`);

    // Summary
    console.log("=".repeat(60));
    console.log("📊 IMPORT SUMMARY");
    console.log("=".repeat(60));
    console.log(`✅ CSV imports successful: ${successCount}`);
    console.log(`❌ CSV imports failed: ${failureCount}`);
    console.log(`📋 CSV files processed: ${CSV_FILES.length}`);
    console.log(`\nℹ️  Old data (from clean_users/clean_talents):`);
    console.log(`   • dev_old_user: ${devOldUserCount.rows[0].count}`);
    console.log(`   • dev_old_talent: ${devOldTalentCount.rows[0].count}\n`);

    if (successCount === CSV_FILES.length) {
      console.log("🎉 ALL DEV TABLES CREATED AND POPULATED SUCCESSFULLY!\n");
    }
  } catch (error) {
    console.error("❌ Fatal error:", error.message);
  } finally {
    await client.end();
    console.log("🔌 Database connection closed.\n");
  }
}

if (require.main === module) {
  main();
}

module.exports = {};
