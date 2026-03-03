const { Client } = require('pg');
const fs = require('fs');
const { createWriteStream } = require('fs');

// Database connection - DEV
const devClient = new Client({
  user: 'ved',
  host: 'localhost',
  database: 'starcast',
  password: 'yourpassword',
  port: 5432,
});

// All columns for messy_users table
const USERS_COLUMNS = [
  'id', 'name', 'email', 'createdAt', 'emailVerified', 'image', 'updatedAt', 'password', 'role'
];

// All 294 columns in EXACT order from example CSV (Old Starcast Talents - Remaining.csv)
const TALENT_COLUMNS = [
  'id', 'userId', 'username', 'first_name', 'last_name', 'contact_email', 'contact_number', 'whatsapp_contact_number',
  'date_of_birth', 'gender', 'nationality', 'hometown', 'current_city', 'bio', 'imdb_link', 'instagram_profile',
  'status', 'created_at', 'updated_at', 'passport_name', 'contact_address', 'joined_date_starcast', 'talent_agent_name',
  'talent_agent_type', 'talent_agency_company_name', 'talent_agent_number', 'talent_agent_email', 'talent_agent_designation',
  'ethnicity', 'build', 'complexion', 'eye_colour', 'hair_colour', 'hair_length', 'have_license', 'have_training',
  'training', 'member_union', 'union_name', 'headshot', 'midshot', 'longshot', 'side_profile', 'intro_link',
  'work_links', 'showreel', 'monologue', 'auditions', 'starcast_profile_link', 'youtube_channel', 'facebook_page', 'x_handle',
  'website_link', 'ankle', 'ankle_female', 'arm_around', 'arm_hole', 'armhole', 'back_depth', 'bank_account_number',
  'bank_name', 'bicep', 'branch_address', 'bust', 'ca_company_name', 'ca_contact_number', 'ca_designation', 'ca_email_id',
  'ca_name', 'calf', 'calf_female', 'catalogue_lookbook_fee_per_change', 'chest', 'collab_fee', 'collar', 'cross_back',
  'cross_front', 'cuff', 'daily_tv_show_per_day_shoot_fee', 'dart_point', 'dubbing_fee_per_minute', 'dubbing_fee_per_minute_other_lang',
  'elbow', 'feature_film_per_day_fee', 'fee_per_minute', 'fee_per_post_view', 'front_crotch', 'full_crotch', 'gst_compliant',
  'gstin_number', 'have_been_background_artist', 'have_theatre_experience', 'height', 'hip', 'hips', 'hosting_per_event_day_fee',
  'ifsc_code', 'in_seam', 'jacket_length', 'knee', 'knee_female', 'legal_address', 'legal_name', 'low_waist',
  'lower_chest', 'lower_waist', 'mid_waist', 'nape_to_hips', 'nape_to_waist', 'neck_round', 'pan_number', 'payment_transfer_notes',
  'per_speaking_engagement_fee', 'per_spoken_word_fee', 'presenting_per_shoot_day_fee', 'ramp_walk_fee_per_day', 'ramp_walk_fee_per_show',
  'reels_post_fee', 'shoe_size', 'short_film_per_day_shoot_fee', 'shoulder', 'shoulder_to_stomach', 'shoulders', 'sleeve', 'sleeve_length',
  'static_post_fee', 'stomach', 'story_fee', 'story_re_share_fee', 'theatre_experience_notes', 'thigh', 'thigh_round', 'trouser_length',
  'upper_chest', 'upper_thigh', 'upper_waist', 'voice_age_range', 'waist', 'web_series_per_day_shoot_fee', 'wrist',
  'actor_digital_ad_per_day_shoot_five_year_usage_fee', 'actor_digital_ad_per_day_shoot_one_year_usage_fee', 'actor_digital_ad_per_day_shoot_perpetuity_usage_fee',
  'actor_digital_ad_per_day_shoot_six_months_usage_fee', 'actor_digital_ad_per_day_shoot_three_months_usage_fee', 'actor_digital_ad_per_day_shoot_three_year_usage_fee',
  'actor_digital_ad_per_day_shoot_two_year_usage_fee', 'actor_digital_print_ad_per_day_shoot_five_year_usage_fee', 'actor_digital_print_ad_per_day_shoot_one_year_usage_fee',
  'actor_digital_print_ad_per_day_shoot_perpetuity_usage_fee', 'actor_digital_print_ad_per_day_shoot_six_months_usage_fee', 'actor_digital_print_ad_per_day_shoot_three_months_usage_fee',
  'actor_digital_print_ad_per_day_shoot_three_year_usage_fee', 'actor_digital_print_ad_per_day_shoot_two_year_usage_fee', 'actor_print_ad_per_day_shoot_five_year_usage_fee',
  'actor_print_ad_per_day_shoot_one_year_usage_fee', 'actor_print_ad_per_day_shoot_perpetuity_usage_fee', 'actor_print_ad_per_day_shoot_six_months_usage_fee',
  'actor_print_ad_per_day_shoot_three_months_usage_fee', 'actor_print_ad_per_day_shoot_three_year_usage_fee', 'actor_print_ad_per_day_shoot_two_year_usage_fee',
  'actor_social_media_ad_per_day_shoot_five_year_usage_fee', 'actor_social_media_ad_per_day_shoot_one_year_usage_fee', 'actor_social_media_ad_per_day_shoot_perpetuity_usage_fee',
  'actor_social_media_ad_per_day_shoot_six_months_usage_fee', 'actor_social_media_ad_per_day_shoot_three_months_usage_fee', 'actor_social_media_ad_per_day_shoot_three_year_usage_fee',
  'actor_social_media_ad_per_day_shoot_two_year_usage_fee', 'actor_tv_commercial_per_day_shoot_five_year_usage_fee', 'actor_tv_commercial_per_day_shoot_one_year_usage_fee',
  'actor_tv_commercial_per_day_shoot_perpetuity_usage_fee', 'actor_tv_commercial_per_day_shoot_three_year_usage_fee', 'actor_tv_commercial_per_day_shoot_two_year_usage_fee',
  'age_range', 'model_digital_ad_per_day_shoot_five_year_usage_fee', 'model_digital_ad_per_day_shoot_one_year_usage_fee', 'model_digital_ad_per_day_shoot_perpetuity_usage_fee',
  'model_digital_ad_per_day_shoot_six_months_usage_fee', 'model_digital_ad_per_day_shoot_three_months_usage_fee', 'model_digital_ad_per_day_shoot_three_year_usage_fee',
  'model_digital_ad_per_day_shoot_two_year_usage_fee', 'model_digital_print_ad_per_day_shoot_five_year_usage_fee', 'model_digital_print_ad_per_day_shoot_one_year_usage_fee',
  'model_digital_print_ad_per_day_shoot_perpetuity_usage_fee', 'model_digital_print_ad_per_day_shoot_six_months_usage_fee', 'model_digital_print_ad_per_day_shoot_three_months_usage_fee',
  'model_digital_print_ad_per_day_shoot_three_year_usage_fee', 'model_digital_print_ad_per_day_shoot_two_year_usage_fee', 'model_print_ad_per_day_shoot_five_year_usage_fee',
  'model_print_ad_per_day_shoot_one_year_usage_fee', 'model_print_ad_per_day_shoot_perpetuity_usage_fee', 'model_print_ad_per_day_shoot_six_months_usage_fee',
  'model_print_ad_per_day_shoot_three_months_usage_fee', 'model_print_ad_per_day_shoot_three_year_usage_fee', 'model_print_ad_per_day_shoot_two_year_usage_fee',
  'model_social_media_ad_per_day_shoot_five_year_usage_fee', 'model_social_media_ad_per_day_shoot_one_year_usage_fee', 'model_social_media_ad_per_day_shoot_perpetuity_usage_fee',
  'model_social_media_ad_per_day_shoot_six_months_usage_fee', 'model_social_media_ad_per_day_shoot_three_months_usage_fee', 'model_social_media_ad_per_day_shoot_three_year_usage_fee',
  'model_social_media_ad_per_day_shoot_two_year_usage_fee', 'model_tv_commercial_per_day_shoot_five_year_usage_fee', 'model_tv_commercial_per_day_shoot_one_year_usage_fee',
  'model_tv_commercial_per_day_shoot_perpetuity_usage_fee', 'model_tv_commercial_per_day_shoot_three_year_usage_fee', 'model_tv_commercial_per_day_shoot_two_year_usage_fee',
  'radio_ad_fee', 'vo_digital_ad_per_day_shoot_five_year_usage_fee', 'vo_digital_ad_per_day_shoot_one_year_usage_fee', 'vo_digital_ad_per_day_shoot_perpetuity_usage_fee',
  'vo_digital_ad_per_day_shoot_six_months_usage_fee', 'vo_digital_ad_per_day_shoot_three_months_usage_fee', 'vo_digital_ad_per_day_shoot_three_year_usage_fee',
  'vo_digital_ad_per_day_shoot_two_year_usage_fee', 'vo_digital_print_ad_per_day_shoot_five_year_usage_fee', 'vo_digital_print_ad_per_day_shoot_one_year_usage_fee',
  'vo_digital_print_ad_per_day_shoot_perpetuity_usage_fee', 'vo_digital_print_ad_per_day_shoot_six_months_usage_fee', 'vo_digital_print_ad_per_day_shoot_three_months_usage_fee',
  'vo_digital_print_ad_per_day_shoot_three_year_usage_fee', 'vo_digital_print_ad_per_day_shoot_two_year_usage_fee', 'vo_print_ad_per_day_shoot_five_year_usage_fee',
  'vo_print_ad_per_day_shoot_one_year_usage_fee', 'vo_print_ad_per_day_shoot_perpetuity_usage_fee', 'vo_print_ad_per_day_shoot_six_months_usage_fee',
  'vo_print_ad_per_day_shoot_three_months_usage_fee', 'vo_print_ad_per_day_shoot_three_year_usage_fee', 'vo_print_ad_per_day_shoot_two_year_usage_fee',
  'vo_social_media_ad_per_day_shoot_five_year_usage_fee', 'vo_social_media_ad_per_day_shoot_one_year_usage_fee', 'vo_social_media_ad_per_day_shoot_perpetuity_usage_fee',
  'vo_social_media_ad_per_day_shoot_six_months_usage_fee', 'vo_social_media_ad_per_day_shoot_three_months_usage_fee', 'vo_social_media_ad_per_day_shoot_three_year_usage_fee',
  'vo_social_media_ad_per_day_shoot_two_year_usage_fee', 'vo_tv_commercial_per_day_shoot_five_year_usage_fee', 'vo_tv_commercial_per_day_shoot_one_year_usage_fee',
  'vo_tv_commercial_per_day_shoot_perpetuity_usage_fee', 'vo_tv_commercial_per_day_shoot_three_year_usage_fee', 'vo_tv_commercial_per_day_shoot_two_year_usage_fee',
  'absolute_no_nos', 'awards_achievements', 'differences_current_vs_desired', 'expectations_from_starcast', 'if_no_limits_what_would_you_do',
  'inspiring_role_models', 'past_experience_and_projects', 'people_already_met', 'projects_under_consideration', 'steps_willing_to_work_on',
  'talent_type', 'tell_us_about_yourself', 'what_could_you_do_right_now', 'when_will_this_take_place', 'wishlist_collaborations', 'acting_workshops_per_day_shoot_fee',
  'talent_starcast_agent_name', 'talent_agency_status', 'unique_physical_characteristics', 'license_type', 'hair_type', 'accommodation_sharing_preference',
  'accommodation_special_preferences', 'accommodation_type', 'flight_class', 'flight_direct_flight_only', 'flight_frequent_flyer_program', 'flight_preference',
  'flight_preferred_airlines', 'flight_seat_type', 'meal_dietary_preference', 'meal_health_nutrition_preferences', 'meal_special_food_preference', 'vanity_request_type',
  'vehicle_additional_requirements', 'vehicle_car_type', 'vehicle_engine_or_fuel_type', 'vehicle_seating_type', 'vehicle_transmission_type', 'facial_hair',
  'languages', 'skills', 'working_well_current', 'actor_fee_per_appearance', 'content_creator_fee_per_appearance', 'model_fee_per_appearance',
  'applyingForTalentManagement', 'terms_accepted', 'middle_name', 'influencer_category', 'presenter_category', 'public_figure_type', 'current_state',
  'isMigratedTalent', 'migrationMailStatus'
];

// Map camelCase display names to actual lowercase database column names
const COLUMN_NAME_MAP = {
  'userId': 'userid',
  'isMigratedTalent': 'ismigratedtalent',
  'migrationMailStatus': 'migrationmailstatus'
};

/**
 * Escape CSV value - handle commas, quotes, and newlines
 */
function escapeCSVValue(value) {
  if (value === null || value === undefined) {
    return '';
  }
  
  const strValue = String(value);
  
  // If contains comma, quote, or newline - wrap in quotes and escape quotes
  if (strValue.includes(',') || strValue.includes('"') || strValue.includes('\n')) {
    return '"' + strValue.replace(/"/g, '""') + '"';
  }
  
  return strValue;
}

/**
 * Convert talent record to CSV row string
 */
function talentToCSVRow(talent) {
  return TALENT_COLUMNS.map(col => {
    // Use mapped lowercase name if it exists, otherwise use the column name as-is
    const dbColName = COLUMN_NAME_MAP[col] || col;
    let value = talent[dbColName];
    
    // Convert undefined/null to empty string
    if (value === null || value === undefined) {
      return '';
    }
    
    // Convert arrays/objects to JSON string
    if (typeof value === 'object') {
      value = JSON.stringify(value);
    }
    
    return escapeCSVValue(value);
  }).join(',');
}

/**
 * Convert user record to CSV row string
 */
function userToCSVRow(user) {
  return USERS_COLUMNS.map(col => {
    let value = user[col];
    
    // Convert undefined/null to empty string
    if (value === null || value === undefined) {
      return '';
    }
    
    // Convert arrays/objects to JSON string
    if (typeof value === 'object') {
      value = JSON.stringify(value);
    }
    
    return escapeCSVValue(value);
  }).join(',');
}

/**
 * Main function to export messy talents
 */
async function exportMessyTalents() {
  try {
    await devClient.connect();
    console.log('✅ Connected to DEV database');
    
    // Query messy_talents table
    const result = await devClient.query('SELECT * FROM messy_talents');
    const talents = result.rows;
    
    console.log(`✅ Found ${talents.length} messy talent records`);
    
    // Create header row
    const header = TALENT_COLUMNS.map(col => `"${col}"`).join(',');
    
    // Create write stream
    const stream = createWriteStream('./messy_talents.csv');
    
    // Write header
    stream.write(header + '\n');
    
    // Write data rows
    for (const talent of talents) {
      stream.write(talentToCSVRow(talent) + '\n');
    }
    
    stream.end();
    
    await new Promise((resolve, reject) => {
      stream.on('finish', resolve);
      stream.on('error', reject);
    });
    
    console.log('✨ Export complete! File: ./messy_talents.csv');
    console.log(`📊 Total records exported: ${talents.length}`);
    console.log(`📋 Total columns: ${TALENT_COLUMNS.length}`);
    
  } catch (error) {
    console.error('❌ Error during export:', error);
    process.exit(1);
  } finally {
    await devClient.end();
  }
}

/**
 * Main function to export messy users
 */
async function exportMessyUsers() {
  try {
    const userClient = new Client({
      user: 'ved',
      host: 'localhost',
      database: 'starcast',
      password: 'yourpassword',
      port: 5432,
    });
    
    await userClient.connect();
    console.log('✅ Connected to DEV database');
    
    // Query messy_users table
    const result = await userClient.query('SELECT * FROM messy_users');
    const users = result.rows;
    
    console.log(`✅ Found ${users.length} messy user records`);
    
    // Create header row
    const header = USERS_COLUMNS.map(col => `"${col}"`).join(',');
    
    // Create write stream
    const stream = createWriteStream('./messy_users.csv');
    
    // Write header
    stream.write(header + '\n');
    
    // Write data rows
    for (const user of users) {
      stream.write(userToCSVRow(user) + '\n');
    }
    
    stream.end();
    
    await new Promise((resolve, reject) => {
      stream.on('finish', resolve);
      stream.on('error', reject);
    });
    
    console.log('✨ Export complete! File: ./messy_users.csv');
    console.log(`📊 Total records exported: ${users.length}`);
    console.log(`📋 Total columns: ${USERS_COLUMNS.length}`);
    
    await userClient.end();
    
  } catch (error) {
    console.error('❌ Error during users export:', error);
    process.exit(1);
  }
}

/**
 * Run both exports sequentially
 */
async function runExports() {
  try {
    console.log('🚀 Starting messy data export...\n');
    
    console.log('--- EXPORTING MESSY TALENTS ---');
    await exportMessyTalents();
    
    console.log('\n--- EXPORTING MESSY USERS ---');
    await exportMessyUsers();
    
    console.log('\n✅ All exports completed successfully!');
  } catch (error) {
    console.error('❌ Export failed:', error);
    process.exit(1);
  }
}

// Run exports
runExports();

