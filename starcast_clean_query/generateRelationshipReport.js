const { Client } = require("pg");

const client = new Client({
  user: "ved",
  host: "localhost",
  database: "starcast",
  password: "yourpassword",
  port: 5432,
});

async function generateRelationshipReport() {
  try {
    await client.connect();

    console.log("\n" + "=".repeat(90));
    console.log("📊 STARCAST DATABASE - RELATIONSHIPS & SCHEMA MAPPING");
    console.log("=".repeat(90) + "\n");

    // Relationship Overview
    console.log("🔗 RELATIONSHIP DIAGRAM");
    console.log("-".repeat(90));
    console.log(`
users
  ├─ userid (Primary Key)
  ├─ actordetailid → actordetails.actordetailid
  └─ countrycode → countries.phonecode (or shortname)
  
  
actordetails
  ├─ actordetailid (Primary Key)
  ├─ parentactordetailid → actordetails.actordetailid (Self-reference for duplicates)
  ├─ currentcityid → cities.id
  ├─ currentcountry → countries.name
  ├─ currentstate → states.name
  ├─ nationality → nationality.nationality
  └─ ONE-TO-MANY: actormediadetails.actordetailid


actormediadetails
  ├─ actormediadetailid (Primary Key)
  ├─ actordetailid → actordetails.actordetailid (Foreign Key)
  └─ ONE-TO-MANY: mediatags.actormediadetailid


mediatags
  ├─ mediatagid (Primary Key)
  └─ actormediadetailid → actormediadetails.actormediadetailid (Foreign Key)


cities
  ├─ id (Primary Key)
  ├─ state_id → states.id
  └─ Used by: actordetails.currentcityid


states
  ├─ id (Primary Key)
  ├─ country_id → countries.id
  └─ Used by: cities.state_id, actordetails.currentstate


nationality (Lookup Table)
  ├─ num_code (Primary Key)
  ├─ nationality (Lookup field)
  └─ Used by: actordetails.nationality


countries (Lookup Table)
  ├─ id (Primary Key)
  ├─ name, shortname, nationality (Lookup fields)
  └─ Used by: states.country_id, actordetails.currentcountry
    `);

    console.log("\n" + "=".repeat(90));
    console.log("📋 TABLE DETAILS");
    console.log("=".repeat(90) + "\n");

    // users table
    console.log("1️⃣ USERS");
    console.log("-".repeat(90));
    console.log("Purpose: User accounts (actors, casting directors, etc.)");
    console.log("Primary Key: userid");
    console.log("Key Relationships:");
    console.log("  • actordetailid → Links to ONE actordetails record");
    console.log("  • countrycode → Links to countries (phone code)");
    console.log("\nSample Query:");
    console.log(`
  SELECT u.userid, u.firstname, u.lastname, u.emailaddress, 
         ad.email, ad.gender, ad.birthdate
  FROM users u
  LEFT JOIN actordetails ad ON u.actordetailid = ad.actordetailid
    `);

    // actordetails table
    console.log("\n2️⃣ ACTORDETAILS");
    console.log("-".repeat(90));
    console.log("Purpose: Actor profile information (talent database)");
    console.log("Primary Key: actordetailid");
    console.log("Key Relationships:");
    console.log("  • ONE actor → MANY media details (photos, videos via actormediadetails)");
    console.log("  • parentactordetailid → Self-reference (marks duplicates/merged records)");
    console.log("  • currentcityid → Cities lookup");
    console.log("  • currentcountry → Countries lookup");
    console.log("  • currentstate → States lookup");
    console.log("  • nationality → Nationality lookup");
    console.log(
      "\nSample Hierarchy: One Actor → Many Media → Many Media Tags"
    );
    console.log(`
  SELECT 
    ad.actordetailid, ad.firstname, ad.lastname,
    COUNT(DISTINCT amd.actormediadetailid) as photo_count,
    COUNT(DISTINCT mt.mediatagid) as tags_assigned
  FROM actordetails ad
  LEFT JOIN actormediadetails amd ON ad.actordetailid = amd.actordetailid
  LEFT JOIN mediatags mt ON amd.actormediadetailid = mt.actormediadetailid
  GROUP BY ad.actordetailid
    `);

    // actormediadetails table
    console.log("\n3️⃣ ACTORMEDIADETAILS");
    console.log("-".repeat(90));
    console.log("Purpose: Media files (photos, videos) for actors");
    console.log("Primary Key: actormediadetailid");
    console.log("Foreign Key: actordetailid → actordetails");
    console.log("Key Relationships:");
    console.log(
      "  • Many media records per actor (ONE actor → MANY media files)"
    );
    console.log("  • ONE media → MANY mediatags (tagging system)");
    console.log("Columns:");
    console.log("  • url: File path/URL");
    console.log("  • type: Media type (image, video, etc.)");
    console.log("  • filename: Original filename");
    console.log("  • mediasize: File size");

    // mediatags table
    console.log("\n4️⃣ MEDIATAGS");
    console.log("-".repeat(90));
    console.log("Purpose: Tags/annotations on media (field data for photos/videos)");
    console.log("Primary Key: mediatagid");
    console.log("Foreign Key: actormediadetailid → actormediadetails");
    console.log("Key Relationships:");
    console.log("  • fielddataid: Reference to tag/field definitions");
    console.log("  • Links media to media details");

    // Location hierarchy
    console.log("\n5️⃣ LOCATION HIERARCHY");
    console.log("-".repeat(90));
    console.log("countries.id");
    console.log("  ↓ (country_id)");
    console.log("states.id");
    console.log("  ↓ (state_id)");
    console.log("cities.id");
    console.log("\nUsed By: actordetails");
    console.log("  • currentcountry (string) → countries.name");
    console.log("  • currentstate (string) → states.name");
    console.log("  • currentcityid (integer) → cities.id");

    // Lookup tables
    console.log("\n6️⃣ LOOKUP TABLES");
    console.log("-".repeat(90));
    console.log("nationality:");
    console.log("  • num_code (PK), alpha_2_code, alpha_3_code, nationality");
    console.log("  • Used by: actordetails.nationality");
    console.log("\ncountries:");
    console.log("  • id (PK), name, shortname, phonecode, nationality");
    console.log("  • Used by: states.country_id, actordetails.currentcountry");

    console.log("\n" + "=".repeat(90));
    console.log("📊 DATA STATISTICS");
    console.log("=".repeat(90) + "\n");

    // Get some stats
    const stats = {};

    // Users count
    const usersRes = await client.query("SELECT COUNT(*) FROM users");
    stats.users = usersRes.rows[0].count;

    // Actordetails count
    const actorsRes = await client.query("SELECT COUNT(*) FROM actordetails");
    stats.actors = actorsRes.rows[0].count;

    // Media per actor
    const mediaRes = await client.query(`
      SELECT 
        COUNT(*) as total_media,
        COUNT(DISTINCT actordetailid) as actors_with_media,
        ROUND(COUNT(*)::numeric / COUNT(DISTINCT actordetailid), 2) as avg_media_per_actor
      FROM actormediadetails
    `);
    stats.media = mediaRes.rows[0];

    // Tags per media
    const tagsRes = await client.query(`
      SELECT 
        COUNT(*) as total_tags,
        COUNT(DISTINCT actormediadetailid) as media_with_tags,
        ROUND(COUNT(*)::numeric / COUNT(DISTINCT actormediadetailid), 2) as avg_tags_per_media
      FROM mediatags
    `);
    stats.tags = tagsRes.rows[0];

    // Cities
    const citiesRes = await client.query("SELECT COUNT(*) FROM cities");
    stats.cities = citiesRes.rows[0].count;

    // States
    const statesRes = await client.query("SELECT COUNT(*) FROM states");
    stats.states = statesRes.rows[0].count;

    // Countries
    const countriesRes = await client.query("SELECT COUNT(*) FROM countries");
    stats.countries = countriesRes.rows[0].count;

    console.log(`👥 Users: ${stats.users.toLocaleString()}`);
    console.log(`🎭 Actors: ${stats.actors.toLocaleString()}`);
    console.log(
      `📸 Media Files: ${stats.media.total_media.toLocaleString()} (avg ${stats.media.avg_media_per_actor} per actor)`
    );
    console.log(
      `🏷️  Media Tags: ${stats.tags.total_tags.toLocaleString()} (avg ${stats.tags.avg_tags_per_media} per media)`
    );
    console.log(`🏙️  Cities: ${stats.cities.toLocaleString()}`);
    console.log(`🗺️  States: ${stats.states.toLocaleString()}`);
    console.log(`🇮🇳 Countries: ${stats.countries.toLocaleString()}`);

    console.log("\n" + "=".repeat(90));
    console.log("🔍 LINKING LOGIC");
    console.log("=".repeat(90) + "\n");

    console.log(`
How to find an actor's complete profile:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. Start with USERS
   Find user by: users.userid or users.emailaddress

2. Get actor details from ACTORDETAILS
   Link via: users.actordetailid → actordetails.actordetailid

3. Get location info
   • Query cities WHERE id = actordetails.currentcityid
   • Query states WHERE id = cities.state_id
   • Query countries WHERE id = states.country_id

4. Get media/photos
   Query ACTORMEDIADETAILS WHERE actordetailid = [actor's id]

5. Get media tags
   Query MEDIATAGS WHERE actormediadetailid IN (list from step 4)

6. Check for duplicate records
   Query ACTORDETAILS WHERE parentactordetailid = [actor's id]


Example Complete Query:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SELECT 
  u.userid, u.firstname, u.lastname, u.emailaddress,
  ad.actordetailid, ad.gender, ad.birthdate,
  c.name as city, s.name as state, co.name as country,
  COUNT(DISTINCT amd.actormediadetailid) as total_media,
  COUNT(DISTINCT mt.mediatagid) as total_tags
FROM users u
LEFT JOIN actordetails ad ON u.actordetailid = ad.actordetailid
LEFT JOIN cities c ON ad.currentcityid = c.id
LEFT JOIN states s ON c.state_id = s.id
LEFT JOIN countries co ON s.country_id = co.id
LEFT JOIN actormediadetails amd ON ad.actordetailid = amd.actordetailid
LEFT JOIN mediatags mt ON amd.actormediadetailid = mt.actormediadetailid
GROUP BY u.userid, ad.actordetailid, c.id, s.id, co.id
    `);

    await client.end();
  } catch (error) {
    console.error("Error:", error.message);
    await client.end();
  }
}

generateRelationshipReport();
