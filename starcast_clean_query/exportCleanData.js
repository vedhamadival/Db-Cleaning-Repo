const { Client } = require("pg");
const fs = require("fs");
const path = require("path");

// 🧱 Connect to PostgreSQL
const client = new Client({
  user: "ved",
  host: "localhost",
  database: "starcast",
  password: "yourpassword",
  port: 5432,
});

// ============================================
// HELPER: Convert array of objects to CSV
// ============================================
const convertToCSV = (data, headers) => {
  if (!data || data.length === 0) {
    return headers.join(",");
  }

  // Create header row
  let csv = headers.join(",") + "\n";

  // Create data rows
  data.forEach((row) => {
    const values = headers.map((header) => {
      let value = row[header];

      // Handle null/undefined
      if (value === null || value === undefined) {
        return "";
      }

      // Convert to string
      value = String(value);

      // Escape quotes and wrap in quotes if contains comma, newline, or quote
      if (value.includes(",") || value.includes("\n") || value.includes('"')) {
        value = '"' + value.replace(/"/g, '""') + '"';
      }

      return value;
    });

    csv += values.join(",") + "\n";
  });

  return csv;
};

// ============================================
// EXPORT CLEAN_USERS
// ============================================
async function exportCleanUsers() {
  console.log("📊 Exporting clean_users table...");

  try {
    const result = await client.query("SELECT * FROM clean_users ORDER BY id");
    const records = result.rows;

    if (records.length === 0) {
      console.log("   ⚠️  No records found in clean_users table");
      return 0;
    }

    // Define headers in desired order
    const headers = [
      "id",
      "name",
      "email",
      "createdAt",
      "emailVerified",
      "image",
      "updatedAt",
      "password",
      "role",
    ];

    // Convert to CSV
    const csv = convertToCSV(records, headers);

    // Write to file
    const filePath = path.join(__dirname, "clean_users.csv");
    fs.writeFileSync(filePath, csv, "utf-8");

    console.log(`   ✅ Exported ${records.length} records to clean_users.csv`);
    return records.length;
  } catch (err) {
    console.error("   ❌ Error exporting clean_users:", err);
    return 0;
  }
}

// ============================================
// EXPORT CLEAN_ACTORDETAILS
// ============================================
async function exportCleanActorDetails() {
  console.log("📊 Exporting clean_actordetails table...");

  try {
    const result = await client.query(
      "SELECT * FROM clean_actordetails ORDER BY actordetailid"
    );
    const records = result.rows;

    if (records.length === 0) {
      console.log("   ⚠️  No records found in clean_actordetails table");
      return 0;
    }

    // Define headers in desired order
    const headers = [
      "actordetailid",
      "actoruid",
      "firstname",
      "lastname",
      "gender",
      "contact_email",
      "contact_number",
      "whatsapp_number",
      "date_of_birth",
      "training",
      "theatre_experience",
      "member_union",
      "union_name",
      "imdb_link",
      "website_link",
      "instagram_link",
      "age_from",
      "age_to",
      "height_feet",
      "height_inches",
      "feature_film_per_day_fee",
      "daily_tv_show_per_day_shoot_fee",
      "created_at",
      "updated_at",
    ];

    // Convert to CSV
    const csv = convertToCSV(records, headers);

    // Write to file
    const filePath = path.join(__dirname, "clean_actordetails.csv");
    fs.writeFileSync(filePath, csv, "utf-8");

    console.log(
      `   ✅ Exported ${records.length} records to clean_actordetails.csv`
    );
    return records.length;
  } catch (err) {
    console.error("   ❌ Error exporting clean_actordetails:", err);
    return 0;
  }
}

// ============================================
// EXPORT CLEAN_TALENTS
// ============================================
async function exportCleanTalents() {
  console.log("📊 Exporting clean_talents table...");

  try {
    const result = await client.query(
      "SELECT * FROM clean_talents ORDER BY id"
    );
    const records = result.rows;

    if (records.length === 0) {
      console.log("   ⚠️  No records found in clean_talents table");
      return 0;
    }

    // Define headers in desired order
    const headers = [
      "id",
      "userId",
      "username",
      "first_name",
      "last_name",
      "gender",
      "contact_email",
      "contact_number",
      "whatsapp_contact_number",
      "date_of_birth",
      "age_range",
      "height",
      "imdb_link",
      "instagram_profile",
      "website_link",
      "have_training",
      "have_theatre_experience",
      "member_union",
      "union_name",
      "feature_film_per_day_fee",
      "daily_tv_show_per_day_shoot_fee",
      "status",
      "created_at",
      "updated_at",
    ];

    // Convert to CSV
    const csv = convertToCSV(records, headers);

    // Write to file
    const filePath = path.join(__dirname, "clean_talents.csv");
    fs.writeFileSync(filePath, csv, "utf-8");

    console.log(
      `   ✅ Exported ${records.length} records to clean_talents.csv`
    );
    return records.length;
  } catch (err) {
    console.error("   ❌ Error exporting clean_talents:", err);
    return 0;
  }
}

// ============================================
// MAIN EXECUTION
// ============================================
async function exportCleanData() {
  console.log("\n" + "=".repeat(60));
  console.log("📤 EXPORTING CLEAN DATA TO CSV");
  console.log("=".repeat(60) + "\n");

  try {
    await client.connect();
    console.log("✅ Connected to PostgreSQL\n");

    // Export all three tables
    const usersCount = await exportCleanUsers();
    const actorCount = await exportCleanActorDetails();
    const talentCount = await exportCleanTalents();

    // Summary
    console.log("\n" + "=".repeat(60));
    console.log("📊 EXPORT SUMMARY");
    console.log("=".repeat(60));
    console.log(
      `\n📋 clean_users.csv: ${usersCount} records exported`
    );
    console.log(
      `📋 clean_actordetails.csv: ${actorCount} records exported`
    );
    console.log(
      `📋 clean_talents.csv: ${talentCount} records exported`
    );
    console.log(`\n📁 Files saved in: ${path.resolve(__dirname)}\n`);
    console.log("✅ Export complete!\n");
  } catch (err) {
    console.error("❌ Error during export:", err);
  } finally {
    await client.end();
    console.log("🔌 Database connection closed.\n");
  }
}

// Run the function
exportCleanData();

module.exports = exportCleanData;
