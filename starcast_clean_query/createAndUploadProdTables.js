const fs = require("fs");
const path = require("path");
const { Client } = require("pg");
const { parse } = require("csv-parse/sync");

// ============================================
// DATABASE CONNECTION - STARCAST PRODUCTION
// ============================================
const client = new Client({
  user: "ved",
  host: "localhost",
  database: "starcast",
  password: "yourpassword",
  port: 5432,
});

// ============================================
// CSV FILES TO IMPORT (Order matters - Users first, then Talents)
// ============================================
const CSV_FILES = [
  {
    path: "prod_user.csv",
    tableName: "prod_user",
  },
  {
    path: "prod_talent.csv",
    tableName: "prod_talent",
  },
];

// ============================================
// CREATE TABLES
// ============================================
async function createTables() {
  try {
    console.log("🧱 Creating prod_User and prod_Talent tables...\n");

    // Drop existing tables if they exist
    await client.query('DROP TABLE IF EXISTS prod_talent CASCADE');
    await client.query('DROP TABLE IF EXISTS prod_user CASCADE');
    console.log("   ✅ Dropped existing tables (if any)");

    // Create prod_user table (matching Prisma User model)
    await client.query(`
      CREATE TABLE prod_user (
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
    console.log("   ✅ Created prod_user table");

    // Create prod_talent table with proper data types matching Prisma schema
    await client.query(`
      CREATE TABLE prod_talent (
  "id" VARCHAR(25) PRIMARY KEY,
  "userId" VARCHAR(255) NOT NULL UNIQUE,
  "username" VARCHAR(50) NOT NULL UNIQUE,
  "first_name" VARCHAR(100),
  "middle_name" VARCHAR(100),
  "last_name" VARCHAR(100),
  "passport_name" VARCHAR(150),
  "gender" VARCHAR(50),
  "date_of_birth" DATE,
  "nationality" VARCHAR(100),
  "hometown" VARCHAR(150),
  "current_state" VARCHAR(150),
  "current_city" VARCHAR(150),
  "bio" TEXT,
  "contact_number" VARCHAR(20),
  "whatsapp_contact_number" VARCHAR(20),
  "contact_email" VARCHAR(255),
  "contact_address" TEXT,
  "applyingForTalentManagement" BOOLEAN DEFAULT false,
  "terms_accepted" BOOLEAN DEFAULT false,
  "isMigratedTalent" BOOLEAN DEFAULT true,
  "migrationMailStatus" VARCHAR(50) DEFAULT 'pending',
  "joined_date_starcast" DATE,
  "talent_agent_name" VARCHAR(150),
  "talent_agent_type" VARCHAR(100),
  "talent_agency_company_name" VARCHAR(150),
  "talent_agent_number" VARCHAR(20),
  "talent_agent_email" VARCHAR(150),
  "talent_agent_designation" VARCHAR(100),
  "ethnicity" VARCHAR(100),
  "build" VARCHAR(100),
  "complexion" VARCHAR(100),
  "unique_physical_characteristics" TEXT DEFAULT '[]',
  "eye_colour" VARCHAR(50),
  "hair_type" TEXT DEFAULT '[]',
  "hair_colour" VARCHAR(50),
  "hair_length" VARCHAR(50),
  "facial_hair" TEXT DEFAULT '[]',
  "languages" TEXT DEFAULT '[]',
  "skills" TEXT DEFAULT '[]',
  "have_license" BOOLEAN DEFAULT false,
  "license_type" TEXT DEFAULT '[]',
  "have_training" BOOLEAN DEFAULT false,
  "training" TEXT,
  "member_union" BOOLEAN DEFAULT false,
  "union_name" VARCHAR(150),
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
  "status" VARCHAR(50) DEFAULT 'pending',
  "created_at" TIMESTAMPTZ(6) DEFAULT now(),
  "updated_at" TIMESTAMPTZ(6) DEFAULT now(),
  "ankle" VARCHAR(50),
  "ankle_female" VARCHAR(50),
  "arm_around" VARCHAR(50),
  "arm_hole" VARCHAR(50),
  "armhole" VARCHAR(50),
  "back_depth" VARCHAR(50),
  "bank_account_number" VARCHAR(30),
  "bank_name" VARCHAR(100),
  "bicep" VARCHAR(50),
  "branch_address" TEXT,
  "bust" VARCHAR(50),
  "ca_company_name" VARCHAR(150),
  "ca_contact_number" VARCHAR(20),
  "ca_designation" VARCHAR(100),
  "ca_email_id" VARCHAR(150),
  "ca_name" VARCHAR(150),
  "calf" VARCHAR(50),
  "calf_female" VARCHAR(50),
  "catalogue_lookbook_fee_per_change" INTEGER,
  "chest" VARCHAR(50),
  "collab_fee" INTEGER,
  "collar" VARCHAR(50),
  "cross_back" VARCHAR(50),
  "cross_front" VARCHAR(50),
  "cuff" VARCHAR(50),
  "daily_tv_show_per_day_shoot_fee" INTEGER,
  "dart_point" VARCHAR(50),
  "dubbing_fee_per_minute" INTEGER,
  "dubbing_fee_per_minute_other_lang" INTEGER,
  "elbow" VARCHAR(50),
  "feature_film_per_day_fee" INTEGER,
  "actor_fee_per_appearance" INTEGER,
  "model_fee_per_appearance" INTEGER,
  "content_creator_fee_per_appearance" INTEGER,
  "fee_per_minute" INTEGER,
  "fee_per_post_view" INTEGER,
  "front_crotch" VARCHAR(50),
  "full_crotch" VARCHAR(50),
  "gst_compliant" BOOLEAN DEFAULT false,
  "gstin_number" VARCHAR(20),
  "have_been_background_artist" BOOLEAN DEFAULT false,
  "have_theatre_experience" BOOLEAN DEFAULT false,
  "height" VARCHAR(50),
  "hip" VARCHAR(50),
  "hips" VARCHAR(50),
  "hosting_per_event_day_fee" INTEGER,
  "ifsc_code" VARCHAR(20),
  "in_seam" VARCHAR(50),
  "influencer_category" TEXT DEFAULT '[]',
  "jacket_length" VARCHAR(50),
  "knee" VARCHAR(50),
  "knee_female" VARCHAR(50),
  "legal_address" TEXT,
  "legal_name" VARCHAR(150),
  "low_waist" VARCHAR(50),
  "lower_chest" VARCHAR(50),
  "lower_waist" VARCHAR(50),
  "mid_waist" VARCHAR(50),
  "nape_to_hips" VARCHAR(50),
  "nape_to_waist" VARCHAR(50),
  "neck_round" VARCHAR(50),
  "pan_number" VARCHAR(20),
  "payment_transfer_notes" TEXT,
  "per_speaking_engagement_fee" INTEGER,
  "per_spoken_word_fee" INTEGER,
  "presenter_category" TEXT DEFAULT '[]',
  "presenting_per_shoot_day_fee" INTEGER,
  "public_figure_type" TEXT DEFAULT '[]',
  "ramp_walk_fee_per_day" INTEGER,
  "ramp_walk_fee_per_show" INTEGER,
  "reels_post_fee" INTEGER,
  "shoe_size" VARCHAR(50),
  "short_film_per_day_shoot_fee" INTEGER,
  "shoulder" VARCHAR(50),
  "shoulder_to_stomach" VARCHAR(50),
  "shoulders" VARCHAR(50),
  "sleeve" VARCHAR(50),
  "sleeve_length" VARCHAR(50),
  "static_post_fee" INTEGER,
  "stomach" VARCHAR(50),
  "story_fee" INTEGER,
  "story_re_share_fee" INTEGER,
  "theatre_experience_notes" TEXT,
  "thigh" VARCHAR(50),
  "thigh_round" VARCHAR(50),
  "trouser_length" VARCHAR(50),
  "upper_chest" VARCHAR(50),
  "upper_thigh" VARCHAR(50),
  "upper_waist" VARCHAR(50),
  "voice_age_range" VARCHAR(50),
  "waist" VARCHAR(50),
  "web_series_per_day_shoot_fee" INTEGER,
  "wrist" VARCHAR(50),
  "actor_digital_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "actor_digital_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "actor_digital_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "actor_digital_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "actor_digital_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "actor_digital_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "actor_digital_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "actor_digital_print_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "actor_digital_print_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "actor_digital_print_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "actor_digital_print_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "actor_digital_print_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "actor_digital_print_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "actor_digital_print_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "actor_print_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "actor_print_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "actor_print_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "actor_print_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "actor_print_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "actor_print_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "actor_print_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "actor_social_media_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "actor_social_media_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "actor_social_media_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "actor_social_media_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "actor_social_media_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "actor_social_media_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "actor_social_media_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "actor_tv_commercial_per_day_shoot_five_year_usage_fee" INTEGER,
  "actor_tv_commercial_per_day_shoot_one_year_usage_fee" INTEGER,
  "actor_tv_commercial_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "actor_tv_commercial_per_day_shoot_three_year_usage_fee" INTEGER,
  "actor_tv_commercial_per_day_shoot_two_year_usage_fee" INTEGER,
  "age_range" VARCHAR(50),
  "model_digital_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "model_digital_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "model_digital_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "model_digital_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "model_digital_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "model_digital_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "model_digital_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "model_digital_print_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "model_digital_print_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "model_digital_print_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "model_digital_print_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "model_digital_print_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "model_digital_print_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "model_digital_print_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "model_print_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "model_print_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "model_print_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "model_print_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "model_print_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "model_print_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "model_print_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "model_social_media_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "model_social_media_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "model_social_media_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "model_social_media_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "model_social_media_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "model_social_media_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "model_social_media_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "model_tv_commercial_per_day_shoot_five_year_usage_fee" INTEGER,
  "model_tv_commercial_per_day_shoot_one_year_usage_fee" INTEGER,
  "model_tv_commercial_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "model_tv_commercial_per_day_shoot_three_year_usage_fee" INTEGER,
  "model_tv_commercial_per_day_shoot_two_year_usage_fee" INTEGER,
  "radio_ad_fee" INTEGER,
  "vo_digital_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "vo_digital_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "vo_digital_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "vo_digital_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "vo_digital_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "vo_digital_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "vo_digital_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "vo_digital_print_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "vo_digital_print_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "vo_digital_print_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "vo_digital_print_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "vo_digital_print_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "vo_digital_print_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "vo_digital_print_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "vo_print_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "vo_print_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "vo_print_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "vo_print_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "vo_print_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "vo_print_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "vo_print_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "vo_social_media_ad_per_day_shoot_five_year_usage_fee" INTEGER,
  "vo_social_media_ad_per_day_shoot_one_year_usage_fee" INTEGER,
  "vo_social_media_ad_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "vo_social_media_ad_per_day_shoot_six_months_usage_fee" INTEGER,
  "vo_social_media_ad_per_day_shoot_three_months_usage_fee" INTEGER,
  "vo_social_media_ad_per_day_shoot_three_year_usage_fee" INTEGER,
  "vo_social_media_ad_per_day_shoot_two_year_usage_fee" INTEGER,
  "vo_tv_commercial_per_day_shoot_five_year_usage_fee" INTEGER,
  "vo_tv_commercial_per_day_shoot_one_year_usage_fee" INTEGER,
  "vo_tv_commercial_per_day_shoot_perpetuity_usage_fee" INTEGER,
  "vo_tv_commercial_per_day_shoot_three_year_usage_fee" INTEGER,
  "vo_tv_commercial_per_day_shoot_two_year_usage_fee" INTEGER,
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
  "talent_type" TEXT DEFAULT '[]',
  "tell_us_about_yourself" TEXT,
  "what_could_you_do_right_now" TEXT,
  "when_will_this_take_place" TEXT,
  "working_well_current" TEXT,
  "wishlist_collaborations" TEXT,
  "acting_workshops_per_day_shoot_fee" INTEGER,
  "talent_starcast_agent_name" VARCHAR(150),
  "talent_agency_status" VARCHAR(50),
  "vanity_request_type" TEXT DEFAULT '[]',
  "accommodation_sharing_preference" TEXT DEFAULT '[]',
  "accommodation_special_preferences" TEXT DEFAULT '[]',
  "accommodation_type" TEXT DEFAULT '[]',
  "flight_class" TEXT DEFAULT '[]',
  "flight_direct_flight_only" BOOLEAN,
  "flight_frequent_flyer_program" TEXT,
  "flight_preference" TEXT,
  "flight_preferred_airlines" TEXT,
  "flight_seat_type" TEXT DEFAULT '[]',
  "meal_dietary_preference" TEXT DEFAULT '[]',
  "meal_health_nutrition_preferences" TEXT DEFAULT '[]',
  "meal_special_food_preference" TEXT DEFAULT '[]',
  "vehicle_additional_requirements" TEXT DEFAULT '[]',
  "vehicle_car_type" TEXT DEFAULT '[]',
  "vehicle_engine_or_fuel_type" TEXT DEFAULT '[]',
  "vehicle_seating_type" TEXT DEFAULT '[]',
  "vehicle_transmission_type" TEXT DEFAULT '[]'
      );
    `);
    console.log("   ✅ Created prod_talent table\n");

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
function convertValue(value, columnName = '') {
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

  // DATE fields - extract just the date part (YYYY-MM-DD)
  const dateFields = ['date_of_birth', 'joined_date_starcast'];
  if (dateFields.includes(columnName)) {
    try {
      const date = new Date(strValue);
      if (!isNaN(date.getTime())) {
        // Return YYYY-MM-DD format
        const year = date.getFullYear();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        return `${year}-${month}-${day}`;
      }
    } catch (e) {
      // Fallback to null
      return null;
    }
  }

  // Try to parse as ISO timestamp for created_at/updated_at (snake_case) or createdAt/updatedAt (camelCase)
  if ((columnName === 'created_at' || columnName === 'updated_at' || columnName === 'createdAt' || columnName === 'updatedAt') && strValue.includes("T")) {
    try {
      const date = new Date(strValue);
      if (!isNaN(date.getTime())) {
        return date.toISOString();
      }
    } catch (e) {
      // Not a valid timestamp, continue
    }
  }

  // DON'T parse ID or email columns as numbers!
  // ID and email fields should ALWAYS be returned as strings
  if (columnName === 'id' || columnName === 'userId' || columnName === 'email' || columnName === 'contact_email' || strValue.includes('@')) {
    return strValue;
  }

  // INTEGER fields - all fee fields and numeric IDs
  const integerFields = [
    'catalogue_lookbook_fee_per_change', 'collab_fee', 'daily_tv_show_per_day_shoot_fee',
    'dubbing_fee_per_minute', 'dubbing_fee_per_minute_other_lang', 'feature_film_per_day_fee',
    'actor_fee_per_appearance', 'model_fee_per_appearance', 'content_creator_fee_per_appearance',
    'fee_per_minute', 'fee_per_post_view', 'hosting_per_event_day_fee', 'per_speaking_engagement_fee',
    'per_spoken_word_fee', 'presenting_per_shoot_day_fee', 'ramp_walk_fee_per_day', 'ramp_walk_fee_per_show',
    'reels_post_fee', 'short_film_per_day_shoot_fee', 'static_post_fee', 'story_fee', 'story_re_share_fee',
    'web_series_per_day_shoot_fee', 'acting_workshops_per_day_shoot_fee', 'radio_ad_fee'
  ];
  
  // Check if column contains fee-related keywords
  const isFeeField = integerFields.includes(columnName) || columnName.includes('_fee');
  
  if (isFeeField) {
    const num = parseInt(strValue, 10);
    if (!isNaN(num)) {
      return num;
    }
    return null;  // Return null if can't parse as integer
  }

  // Try to parse as number (generic) - but only if the entire string is a valid number
  // This prevents "29sharadbishnoi@gmail.com" from being parsed as 29
  if (!isNaN(strValue) && !isNaN(parseFloat(strValue))) {
    // Only parse as number if string contains ONLY numeric/decimal characters
    if (/^-?\d+\.?\d*$/.test(strValue) || /^-?\d*\.?\d+$/.test(strValue)) {
      const num = parseFloat(strValue);
      if (!isNaN(num)) {
        // Check if it's an integer
        if (Number.isInteger(num)) {
          return num;
        }
        // It's a float, return as is
        return num;
      }
    }
  }

  // Return as string
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
      const convertedValue = convertValue(value, col);  // Pass column name for type-specific conversion
      columns.push(`"${col}"`);  // Quote column names for case sensitivity
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
// INSERT DATA - Row by Row with Error Tracking
// ============================================
async function insertDataRowByRow(tableName, records) {
  try {
    // Get all columns from first record
    const allColumns = Object.keys(records[0]);
    let insertedCount = 0;
    const failedRecords = [];

    console.log(`   📋 Total columns in table: ${allColumns.length}`);
    console.log(`   📊 Processing ${records.length} records...\n`);

    for (let i = 0; i < records.length; i++) {
      const record = records[i];

      // Log specific emails for investigation
      if (record.email && (record.email.includes('29sharadbishnoi') || record.email.includes('11.urzan'))) {
          console.log(`\n   🔍 DEBUG: Inserting problematic email '${record.email}' at index ${i} (Row ${i+2})`);
      }

      try {
        await insertSingleRecord(tableName, record, allColumns);
        insertedCount++;
        
        if (record.email && (record.email.includes('29sharadbishnoi') || record.email.includes('11.urzan'))) {
            console.log(`   ✅ DEBUG: Successfully inserted '${record.email}'`);
        }

        // Progress indicator every 5 records or at start/end
        if ((i + 1) % 5 === 0 || i === 0 || i === records.length - 1) {
          process.stdout.write(
            `\r   📊 Inserting... ${insertedCount}/${records.length}`
          );
        }
      } catch (err) {
        // Check for duplicate email constraint violations - handle them differently
        if (err.message && err.message.includes('duplicate key') && err.message.includes('email')) {
          console.log(`\n   ⚠️  Email conflict for '${record.email}' at row ${i + 2}`);
          console.log(`       Checking if this email already exists in database...`);
          
          try {
            const checkResult = await client.query(
              'SELECT id, email FROM prod_user WHERE LOWER(email) = LOWER($1) LIMIT 1',
              [record.email]
            );
            
            if (checkResult.rows.length > 0) {
              const existing = checkResult.rows[0];
              console.log(`       Found existing record: ID=${existing.id}, Email=${existing.email}`);
              console.log(`       Current record: ID=${record.id}, Email=${record.email}`);
              console.log(`       Skipping duplicate...`);
            } else {
              console.log(`       No existing record found! This is unexpected.`);
              console.log(`       Full error: ${err.message}`);
            }
          } catch (checkErr) {
            console.log(`       Error checking database: ${checkErr.message}`);
          }
        }
        
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
      console.log("\n   📋 Failed Record Details (first 5):");

      failedRecords.slice(0, 5).forEach((failed) => {
        console.log(`      Row ${failed.rowNumber} (ID: ${failed.recordId})`);
        console.log(`      Error: ${failed.error}`);
      });

      if (failedRecords.length > 5) {
        console.log(`      ... and ${failedRecords.length - 5} more failed records`);
      }
    } else {
      console.log(`   🎉 NO ERRORS - All records inserted successfully!`);
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
    const records = parseCSVFile(csvFile);
    if (!records) return false;
    console.log(`   ✅ Found ${records.length} records`);

    // 2. Insert data row by row
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
  console.log("📊 CREATE & UPLOAD PROD TABLES TO STARCAST DATABASE");
  console.log("=".repeat(60));
  console.log();

  try {
    await client.connect();
    console.log("✅ Connected to starcast database\n");

    // Create tables
    await createTables();

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
    console.log("\n" + "─".repeat(60));
    console.log("🔍 Final Verification:");
    console.log("─".repeat(60));

    const userCount = await client.query('SELECT COUNT(*) FROM prod_user');
    const talentCount = await client.query('SELECT COUNT(*) FROM prod_talent');

    console.log(`   📊 prod_user records: ${userCount.rows[0].count}`);
    console.log(`   📊 prod_talent records: ${talentCount.rows[0].count}\n`);

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

// Run if called directly
if (require.main === module) {
  main();
}

module.exports = {};
