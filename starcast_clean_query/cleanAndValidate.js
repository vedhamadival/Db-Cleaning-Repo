const { Client } = require("pg");
const cuid = require("cuid");
const {
  normalizePhone,
  parseBirthdate,
  cleanEmail,
  cleanName,
  cleanGender,
  cleanDesignation,
} = require("./helperFunctions");

// 🧱 Connect to PostgreSQL
const client = new Client({
  user: "ved",
  host: "localhost",
  database: "starcast",
  password: "yourpassword",
  port: 5432,
});

// ============================================
// HELPER: Format Age Range
// ============================================
function formatAgeRange(age_from, age_to) {
  if (!age_from && !age_to) return null;
  
  const from = age_from ? Math.abs(parseInt(age_from)) : null;
  const to = age_to ? Math.abs(parseInt(age_to)) : null;
  
  if (from && !to) return `${from}-${from}`;
  if (to && !from) return `${to}-${to}`;
  if (from && to) return `${from}-${to}`;
  
  return null;
}

// ============================================
// HELPER: Format Height
// ============================================
function formatHeight(height_feet, height_inches) {
  if (!height_feet && !height_inches) return null;
  
  const feet = height_feet ? Math.abs(parseInt(height_feet)) : 0;
  const inches = height_inches ? Math.abs(parseInt(height_inches)) : 0;
  
  if (feet < 1 || feet > 8) return null;
  if (inches < 0 || inches > 11) return null;
  
  if (feet > 0 && !height_inches) return `${feet}'0"`;
  if (feet > 0 && inches >= 0) return `${feet}'${inches}"`;
  
  return null;
}

// ============================================
// HELPER: Parse Fee
// ============================================
function parseFee(feeString) {
  if (!feeString) return null;
  
  const cleaned = String(feeString).trim();
  if (cleaned === '' || cleaned === '0') return null;
  
  const parsed = parseInt(cleaned, 10);
  if (Number.isNaN(parsed)) return null;
  
  // Cap at 50,000,000 (8-digit maximum)
  const MAX_FEE = 50000000;
  return parsed > MAX_FEE ? MAX_FEE : parsed;
}

// ============================================
// HELPER: Generate Random Slug
// ============================================
function generateRandomSlug() {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let slug = 'talent-';
  for (let i = 0; i < 16; i++) {
    slug += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return slug;
}

// ============================================
// HELPER: Validate URL
// ============================================
function validateURL(url) {
  if (!url) return null;
  
  const urlString = String(url).trim();
  if (urlString === '') return null;
  
  try {
    const urlObj = new URL(urlString.startsWith('http') ? urlString : `https://${urlString}`);
    return urlString;
  } catch (e) {
    return null;
  }
}

// ============================================
// HELPER: Validate Instagram Link
// ============================================
function validateInstagramLink(url) {
  if (!url) return null;
  
  const urlString = String(url).trim();
  if (!urlString.toLowerCase().includes('instagram')) return null;
  if (!urlString.startsWith('http') && !urlString.startsWith('www')) return null;
  
  try {
    new URL(urlString.startsWith('http') ? urlString : `https://${urlString}`);
    return urlString;
  } catch (e) {
    return null;
  }
}

// ============================================
// HELPER: Find User ID by Email
// ============================================
async function findUserIdByEmail(email) {
  const result = await client.query(
    'SELECT id FROM clean_users WHERE email = $1',
    [email]
  );
  return result.rows.length > 0 ? result.rows[0].id : null;
}

// ============================================
// HELPER: Create User from Actor Detail
// ============================================
async function createUserFromActorDetail(actorDetail) {
  try {
    const id = cuid();
    const first = cleanName(actorDetail.firstname);
    const last = cleanName(actorDetail.lastname);
    const name = last ? `${first} ${last}` : first;
    const email = cleanEmail(actorDetail.contact_email);
    const createdAt = actorDetail.createdat || new Date();
    const updatedAt = actorDetail.updatedat || new Date();

    await client.query(
      `INSERT INTO clean_users 
       (id, name, email, createdAt, emailVerified, image, updatedAt, password, role)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
      [
        id,
        name,
        email,
        createdAt,
        null,
        null,
        updatedAt,
        null,
        'Talent'
      ]
    );
    return id;
  } catch (err) {
    console.error(`   ⚠️  Failed to create user for ${actorDetail.contact_email}: ${err.message}`);
    return null;
  }
}

// ============================================
// HELPER: Find duplicate emails in records
// ============================================
const findDuplicateEmails = (records, emailField) => {
  const emailMap = {};

  records.forEach((r) => {
    const rawEmail = r[emailField];
    const email = cleanEmail(rawEmail); // Sanitize first

    if (email) {
      // Only count non-empty emails
      if (!emailMap[email]) emailMap[email] = [];
      emailMap[email].push(r);
    }
  });

  // Return Set of emails that appear more than once
  return new Set(
    Object.keys(emailMap).filter((email) => emailMap[email].length > 1)
  );
};

// ============================================
// CREATE CLEAN TABLES
// ============================================
async function ensureTables() {
  console.log("🧱 Creating clean tables...\n");

  // Clean Users Table - Prisma User Schema
  await client.query(`
    DROP TABLE IF EXISTS clean_users;
    CREATE TABLE clean_users (
      id           VARCHAR(25) PRIMARY KEY,
      name         VARCHAR(255) NOT NULL,
      email        VARCHAR(255) NOT NULL UNIQUE,
      createdAt    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      emailVerified TIMESTAMP,
      image        VARCHAR(255),
      updatedAt    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      password     VARCHAR(255),
      role         VARCHAR(50) DEFAULT 'Talent'
    );
  `);

  // Clean Actor Details Table
  await client.query(`
    DROP TABLE IF EXISTS clean_actordetails;
    CREATE TABLE clean_actordetails (
      actordetailid INTEGER PRIMARY KEY,
      actoruid VARCHAR(255),
      firstname VARCHAR(255) NOT NULL,
      lastname VARCHAR(255),
      gender VARCHAR(50),
      contact_email VARCHAR(255) NOT NULL UNIQUE,
      contact_number VARCHAR(20),
      whatsapp_number VARCHAR(20),
      date_of_birth DATE,
      training BOOLEAN DEFAULT FALSE,
      theatre_experience BOOLEAN DEFAULT FALSE,
      member_union BOOLEAN DEFAULT FALSE,
      union_name TEXT,
      imdb_link TEXT,
      website_link TEXT,
      instagram_link TEXT,
      age_from INTEGER,
      age_to INTEGER,
      height_feet INTEGER,
      height_inches INTEGER,
      feature_film_per_day_fee VARCHAR(20),
      daily_tv_show_per_day_shoot_fee VARCHAR(20),
      createdat TIMESTAMP,
      updatedat TIMESTAMP
    );
  `);

  // Clean Talents Table - Prisma Talent Schema
  await client.query(`
    DROP TABLE IF EXISTS clean_talents;
    CREATE TABLE clean_talents (
      id                                  VARCHAR(25) PRIMARY KEY,
      userId                              VARCHAR(25) NOT NULL UNIQUE,
      username                            VARCHAR(50) NOT NULL UNIQUE,
      first_name                          VARCHAR(100),
      last_name                           VARCHAR(100),
      gender                              VARCHAR(50),
      contact_email                       VARCHAR(255),
      contact_number                      VARCHAR(20),
      whatsapp_contact_number             VARCHAR(20),
      date_of_birth                       DATE,
      age_range                           VARCHAR(50),
      height                              VARCHAR(50),
      imdb_link                           TEXT,
      instagram_profile                   TEXT,
      website_link                        TEXT,
      have_training                       BOOLEAN DEFAULT FALSE,
      have_theatre_experience             BOOLEAN DEFAULT FALSE,
      member_union                        BOOLEAN DEFAULT FALSE,
      union_name                          VARCHAR(150),
      feature_film_per_day_fee            INTEGER,
      daily_tv_show_per_day_shoot_fee     INTEGER,
      status                              VARCHAR(50) DEFAULT 'pending',
      created_at                          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at                          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  console.log("✅ Clean tables created.\n");
}

// ============================================
// PROCESS USERS TABLE
// ============================================
async function processUsers() {
  console.log("📋 Processing USERS table...");

  const usersRes = await client.query("SELECT * FROM users");
  const allUsers = usersRes.rows;
  console.log(`📊 Total records found: ${allUsers.length}\n`);

  // Step 1: Find duplicate emails
  console.log("🔍 Identifying duplicate emails...");
  const duplicateEmails = findDuplicateEmails(allUsers, "emailaddress");
  console.log(
    `   Found ${duplicateEmails.size} email(s) with duplicates\n`
  );

  let accepted = 0;
  let rejectedNoEmail = 0;
  let rejectedDuplicate = 0;
  let rejectedNoFirstName = 0;
  let rejectedInsertError = 0;

  // Step 2: Validate and insert
  for (const u of allUsers) {
    // Sanitize
    const email = cleanEmail(u.emailaddress);
    const phone = normalizePhone(u.mobilenumber);
    const first = cleanName(u.firstname);
    const last = cleanName(u.lastname);
    const gender = cleanGender(u.gender);
    const designation = cleanDesignation(u.designation);

    // VALIDATION CHECK 1: Email required
    if (!email) {
      rejectedNoEmail++;
      continue;
    }

    // VALIDATION CHECK 2: No duplicates
    if (duplicateEmails.has(email)) {
      rejectedDuplicate++;
      continue;
    }

    // VALIDATION CHECK 3: FirstName required
    if (!first) {
      rejectedNoFirstName++;
      continue;
    }

    // ✅ All checks passed - INSERT
    try {
      const id = cuid();
      const name = last ? `${first} ${last}` : first; // Combine firstname and lastname
      const createdAt = u.createdat || new Date();
      const updatedAt = u.updatedat || new Date();
      
      await client.query(
        `INSERT INTO clean_users 
         (id, name, email, createdAt, emailVerified, image, updatedAt, password, role)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [
          id,                 // Generated CUID
          name,              // Combined firstname + lastname
          email,             // contact_email
          createdAt,         // createdat
          null,              // emailVerified (default null)
          null,              // image (default null)
          updatedAt,         // updatedat
          null,              // password (default null)
          'Talent'           // role (default 'Talent')
        ]
      );
      accepted++;
    } catch (err) {
      rejectedInsertError++;
    }
  }

  const totalRejected = rejectedNoEmail + rejectedDuplicate + rejectedNoFirstName + rejectedInsertError;
  console.log(
    `\n✅ USERS Summary:\n   Accepted: ${accepted}\n   ❌ Total Rejected: ${totalRejected}\n   (no email: ${rejectedNoEmail}, duplicates: ${rejectedDuplicate}, no first name: ${rejectedNoFirstName}, insert errors: ${rejectedInsertError})\n`
  );

  return { accepted, rejectedNoEmail, rejectedDuplicate, rejectedNoFirstName, rejectedInsertError, totalRejected };
}

// ============================================
// PROCESS ACTORDETAILS TABLE
// ============================================
async function processActorDetails() {
  console.log("📋 Processing ACTORDETAILS table...");

  const actorsRes = await client.query("SELECT * FROM actordetails");
  const allActors = actorsRes.rows;
  console.log(`📊 Total records found: ${allActors.length}\n`);

  // Step 1: Find duplicate emails
  console.log("🔍 Identifying duplicate emails...");
  const duplicateEmails = findDuplicateEmails(allActors, "email");
  console.log(
    `   Found ${duplicateEmails.size} email(s) with duplicates\n`
  );

  let accepted = 0;
  let rejectedNoEmail = 0;
  let rejectedDuplicate = 0;
  let rejectedNoFirstName = 0;
  let rejectedInsertError = 0;

  // Step 2: Validate and insert
  for (const a of allActors) {
    // Sanitize
    const email = cleanEmail(a.email);
    const phone = normalizePhone(a.mobileno);
    const whatsapp = normalizePhone(a.whatsappno);
    const first = cleanName(a.firstname);
    const last = cleanName(a.lastname);
    const gender = cleanGender(a.gender);
    const date_of_birth = parseBirthdate(a.birthdate);

    // VALIDATION CHECK 1: Email required
    if (!email) {
      rejectedNoEmail++;
      continue;
    }

    // VALIDATION CHECK 2: No duplicates
    if (duplicateEmails.has(email)) {
      rejectedDuplicate++;
      continue;
    }

    // VALIDATION CHECK 3: FirstName required
    if (!first) {
      rejectedNoFirstName++;
      continue;
    }

    // ✅ All checks passed - INSERT
    try {
      await client.query(
        `INSERT INTO clean_actordetails
         (actordetailid, actoruid, firstname, lastname, gender, contact_email, contact_number, whatsapp_number,
          date_of_birth, training, theatre_experience, member_union, union_name,
          imdb_link, website_link, instagram_link, age_from, age_to, height_feet, height_inches, feature_film_per_day_fee, daily_tv_show_per_day_shoot_fee, createdat, updatedat)
         VALUES
         ($1, $2, $3, $4, $5, $6, $7, $8,
          $9, $10, $11, $12, $13,
          $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)`,
        [
          a.actordetailid,
          a.actoruid,
          first,
          last,
          gender,
          email,
          phone ?? whatsapp,
          whatsapp,
          date_of_birth,
          a.training === true || a.training === "true" ? true : false,
          a.theatretraining === true || a.theatretraining === "true" ? true : false,
          a.ismemberofunionorguild === true ||
          a.ismemberofunionorguild === "true"
            ? true
            : false,
          a.unionorguildname && a.unionorguildname.trim() !== ""
            ? a.unionorguildname
            : null,
          validateURL(a.imdbpagelink),
          validateURL(a.websitelink),
          validateInstagramLink(a.instagramlink),
          a.agefrom ? parseInt(a.agefrom) : null,
          a.ageto ? parseInt(a.ageto) : null,
          a.heightfeet ? parseInt(a.heightfeet) : null,
          a.heightinches ? parseInt(a.heightinches) : null,
          a.featurefilmperdayrate ? String(a.featurefilmperdayrate) : null,
          a.dailytvshowperdayrate ? String(a.dailytvshowperdayrate) : null,
          a.createdat,
          a.updatedat,
        ]
      );
      accepted++;
    } catch (err) {
      rejectedInsertError++;
    }
  }

  const totalRejected = rejectedNoEmail + rejectedDuplicate + rejectedNoFirstName + rejectedInsertError;
  console.log(
    `\n✅ ACTORDETAILS Summary:\n   Accepted: ${accepted}\n   ❌ Total Rejected: ${totalRejected}\n   (no email: ${rejectedNoEmail}, duplicates: ${rejectedDuplicate}, no first name: ${rejectedNoFirstName}, insert errors: ${rejectedInsertError})\n`
  );

  return { accepted, rejectedNoEmail, rejectedDuplicate, rejectedNoFirstName, rejectedInsertError, totalRejected };
}

// ============================================
// PROCESS ACTORDETAILS TO TALENTS
// ============================================
async function processActorDetailsToTalent() {
  console.log("📋 Processing ACTORDETAILS to TALENTS table...");
  const actorsRes = await client.query("SELECT * FROM clean_actordetails");
  const allActors = actorsRes.rows;
  console.log(`📊 Total records: ${allActors.length}`);

  let accepted = 0, rejectedInsertError = 0, usersAutoCreated = 0;
  let warningInstagramInvalid = 0, warningHeightInvalid = 0, warningFeeInvalid = 0;

  for (const a of allActors) {
    const email = a.contact_email;
    const first = a.firstname;
    const last = a.lastname;
    
    let userId = await findUserIdByEmail(email);
    
    // If user doesn't exist, create one from actor detail
    if (!userId) {
      userId = await createUserFromActorDetail(a);
      if (!userId) { rejectedInsertError++; continue; }
      usersAutoCreated++;
    }
    
    const id = cuid();
    const username = generateRandomSlug();
    if (username.length > 50) { rejectedUsernameTooLong++; continue; }
    
    const gender = a.gender;
    const age_range = formatAgeRange(a.age_from, a.age_to);
    const height = formatHeight(a.height_feet, a.height_inches);
    const instagram_profile = a.instagram_link;
    const feature_film_fee = parseFee(a.feature_film_per_day_fee);
    const daily_tv_fee = parseFee(a.daily_tv_show_per_day_shoot_fee);
    
    if (a.instagram_link && !a.instagram_link.toLowerCase().includes('instagram')) warningInstagramInvalid++;
    if (a.height_feet && !height) warningHeightInvalid++;
    if ((a.feature_film_per_day_fee || a.daily_tv_show_per_day_shoot_fee) && (!feature_film_fee && !daily_tv_fee)) warningFeeInvalid++;
    
    try {
      await client.query(
        `INSERT INTO clean_talents (id, userId, username, first_name, last_name, gender, contact_email, contact_number, whatsapp_contact_number, date_of_birth, age_range, height, imdb_link, instagram_profile, website_link, have_training, have_theatre_experience, member_union, union_name, feature_film_per_day_fee, daily_tv_show_per_day_shoot_fee, status, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)`,
        [id, userId, username, first, last, gender, email, a.contact_number, a.whatsapp_number, a.date_of_birth, age_range, height, a.imdb_link, instagram_profile, a.website_link, a.training, a.theatre_experience, a.member_union, a.union_name, feature_film_fee, daily_tv_fee, 'pending', a.createdat, a.updatedat]
      );
      accepted++;
    } catch (err) {
      console.error(`   ⚠️  Insert error for ${email}: ${err.message}`);
      rejectedInsertError++;
    }
  }
  
  const totalRejected = rejectedInsertError;
  const warnings = warningInstagramInvalid + warningHeightInvalid + warningFeeInvalid;
  console.log(`\n✅ TALENTS Summary:\n   Accepted: ${accepted}\n   ❌ Total Rejected: ${totalRejected}\n   (insert errors: ${rejectedInsertError})\n   👤 Users auto-created: ${usersAutoCreated}`);
  return { accepted, rejectedInsertError, totalRejected, usersAutoCreated, warningInstagramInvalid, warningHeightInvalid, warningFeeInvalid };
}

// ============================================
// CLEANUP: Remove orphaned users without talents
// ============================================
async function cleanupOrphanedUsers() {
  console.log("🧹 Cleaning up orphaned users (no talent records)...");
  
  try {
    // Find users that don't have a corresponding talent
    const result = await client.query(`
      SELECT id FROM clean_users 
      WHERE id NOT IN (SELECT DISTINCT userId FROM clean_talents)
    `);
    
    const orphanedUsers = result.rows.length;
    
    if (orphanedUsers > 0) {
      // Delete orphaned users
      await client.query(`
        DELETE FROM clean_users 
        WHERE id NOT IN (SELECT DISTINCT userId FROM clean_talents)
      `);
      console.log(`   🗑️  Removed ${orphanedUsers} orphaned users\n`);
    } else {
      console.log(`   ✅ No orphaned users to remove\n`);
    }
    
    return orphanedUsers;
  } catch (err) {
    console.error("   ❌ Error cleaning up orphaned users:", err);
    return 0;
  }
}

// ============================================
// MAIN EXECUTION
// ============================================
async function cleanAndValidate() {
  console.log("\n" + "=".repeat(60));
  console.log("🎬 STARCAST DATA CLEANING & VALIDATION");
  console.log("=".repeat(60) + "\n");

  try {
    await client.connect();
    console.log("✅ Connected to PostgreSQL\n");

    // Create clean tables
    await ensureTables();

    // Process both tables
    const usersStats = await processUsers();
    const actorStats = await processActorDetails();
    const talentStats = await processActorDetailsToTalent();

    // Cleanup orphaned users
    const orphanedCount = await cleanupOrphanedUsers();

    // Final summary
    printFinalSummary(usersStats, actorStats, talentStats, orphanedCount);
  } catch (err) {
    console.error("❌ Error during execution:", err);
  } finally {
    await client.end();
    console.log("🔌 Database connection closed.\n");
  }
}

// ============================================
// PRINT FINAL SUMMARY
// ============================================

function printFinalSummary(usersStats, actorStats, talentStats, orphanedCount) {
  console.log("=".repeat(60));
  console.log("📊 FINAL SUMMARY");
  console.log("=".repeat(60));
  
  const totalUsersCreated = usersStats.accepted + talentStats.usersAutoCreated;
  const finalUsersCount = totalUsersCreated - orphanedCount;
  console.log(`\n👥 TOTAL USERS CREATED: ${totalUsersCreated}`);
  console.log(`   • From USERS table: ${usersStats.accepted}`);
  console.log(`   • Auto-created from ACTORDETAILS: ${talentStats.usersAutoCreated}`);
  console.log(`   • Removed (orphaned, no talents): ${orphanedCount}`);
  console.log(`   • Final USERS count: ${finalUsersCount}`);
  
  console.log(`\n📋 CLEAN_USERS TABLE (after cleanup):`);
  console.log(`   📥 Initial count: ${totalUsersCreated}`);
  console.log(`   🗑️  Orphaned removed: ${orphanedCount}`);
  console.log(`   ✅ Final count: ${finalUsersCount}`);
  
  const usersTotal = usersStats.accepted + usersStats.totalRejected;
  console.log(`\n📋 CLEAN_USERS VALIDATION (from USERS table input):`);
  console.log(`   📥 Total Input: ${usersTotal}`);
  console.log(`   ✅ Accepted: ${usersStats.accepted}`);
  console.log(`   ❌ Total Rejected: ${usersStats.totalRejected}`);
  console.log(`      • No email: ${usersStats.rejectedNoEmail}`);
  console.log(`      • Duplicates: ${usersStats.rejectedDuplicate}`);
  console.log(`      • No first name: ${usersStats.rejectedNoFirstName}`);
  console.log(`      • Insert errors: ${usersStats.rejectedInsertError}`);
  console.log(`   ✔️  Math: ${usersStats.accepted} + ${usersStats.totalRejected} = ${usersTotal}`);

  console.log(`\n📋 CLEAN_ACTORDETAILS TABLE:`);
  const actorTotal = actorStats.accepted + actorStats.totalRejected;
  console.log(`   📥 Total Input: ${actorTotal}`);
  console.log(`   ✅ Accepted: ${actorStats.accepted}`);
  console.log(`   ❌ Total Rejected: ${actorStats.totalRejected}`);
  console.log(`      • No email: ${actorStats.rejectedNoEmail}`);
  console.log(`      • Duplicates: ${actorStats.rejectedDuplicate}`);
  console.log(`      • No first name: ${actorStats.rejectedNoFirstName}`);
  console.log(`      • Insert errors: ${actorStats.rejectedInsertError}`);
  console.log(`   ✔️  Math: ${actorStats.accepted} + ${actorStats.totalRejected} = ${actorTotal}`);

  console.log(`\n📋 CLEAN_TALENTS TABLE (User-Linked/Auto-Created):`);
  console.log(`   📥 Total Input (from clean_actordetails): ${actorStats.accepted}`);
  console.log(`   ✅ Accepted (with user): ${talentStats.accepted}`);
  console.log(`   ❌ Total Rejected: ${talentStats.totalRejected}`);
  console.log(`      • Insert errors: ${talentStats.rejectedInsertError}`);
  console.log(`   👤 Users auto-created: ${talentStats.usersAutoCreated}`);
  console.log(`   ⚠️  Warnings (data quality issues): ${talentStats.warningInstagramInvalid + talentStats.warningHeightInvalid + talentStats.warningFeeInvalid}`);
  console.log(`      • Invalid instagram: ${talentStats.warningInstagramInvalid}`);
  console.log(`      • Invalid height: ${talentStats.warningHeightInvalid}`);
  console.log(`      • Invalid fees: ${talentStats.warningFeeInvalid}`);
  console.log(`   ✔️  Math: ${talentStats.accepted} + ${talentStats.totalRejected} = ${talentStats.accepted + talentStats.totalRejected} (from ${actorStats.accepted} clean actors)`);

  console.log(`\n🎉 Validation & cleaning complete!\n`);
}

cleanAndValidate();

module.exports = cleanAndValidate;
