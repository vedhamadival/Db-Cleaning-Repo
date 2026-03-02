const { Client } = require("pg");

const client = new Client({
  user: "ved",
  host: "localhost",
  database: "starcast",
  password: "yourpassword",
  port: 5432,
});

async function checkSchema() {
  try {
    await client.connect();
    console.log("\n=== ANALYZING ACTORDETAILS TABLE SCHEMA ===\n");

    // Get column names and types
    const columnsResult = await client.query(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_name = 'actordetails'
      ORDER BY ordinal_position
    `);

    console.log("📋 COLUMNS IN ACTORDETAILS:");
    columnsResult.rows.forEach(col => {
      console.log(`   ${col.column_name} | ${col.data_type} | Nullable: ${col.is_nullable}`);
    });

    // Check for specific columns we need
    console.log("\n🔍 CHECKING FOR CRITICAL COLUMNS:\n");
    
    const criticalCols = ['nationality', 'currentcity', 'currentstate', 'currentcityid', 'is_influencer', 'currentstateid'];
    for (const col of criticalCols) {
      const result = await client.query(`
        SELECT EXISTS(
          SELECT 1 FROM information_schema.columns 
          WHERE table_name = 'actordetails' 
          AND column_name = $1
        )
      `, [col]);
      console.log(`   ${col}: ${result.rows[0].exists ? '✅ EXISTS' : '❌ NOT FOUND'}`);
    }

    // Get sample data
    console.log("\n📊 SAMPLE DATA (3 records with key fields):\n");
    const sampleResult = await client.query(`
      SELECT 
        actordetailid,
        firstname,
        lastname,
        nationality,
        currentcity,
        currentstate,
        currentcityid,
        is_influencer
      FROM actordetails
      LIMIT 3
    `);

    console.log("Sample Records:");
    sampleResult.rows.forEach((row, idx) => {
      console.log(`\n  Record ${idx + 1}:`);
      console.log(`    ID: ${row.actordetailid}`);
      console.log(`    Name: ${row.firstname} ${row.lastname}`);
      console.log(`    Nationality: ${row.nationality}`);
      console.log(`    Current City (text): ${row.currentcity}`);
      console.log(`    Current State (text): ${row.currentstate}`);
      console.log(`    Current City ID (FK): ${row.currentcityid}`);
      console.log(`    Is Influencer: ${row.is_influencer}`);
    });

    // Check null patterns
    console.log("\n\n📈 DATA QUALITY ANALYSIS:\n");
    const qualityResult = await client.query(`
      SELECT
        COUNT(*) as total_records,
        COUNT(NULLIF(nationality, '')) as nationality_filled,
        COUNT(NULLIF(currentcity, '')) as currentcity_filled,
        COUNT(NULLIF(currentstate, '')) as currentstate_filled,
        COUNT(NULLIF(currentcityid::text, '')) as currentcityid_filled,
        COUNT(CASE WHEN is_influencer = true THEN 1 END) as influencer_count
      FROM actordetails
    `);

    const q = qualityResult.rows[0];
    console.log(`   Total Records: ${q.total_records}`);
    console.log(`   Nationality filled: ${q.nationality_filled} (${((q.nationality_filled/q.total_records)*100).toFixed(1)}%)`);
    console.log(`   Current City (text) filled: ${q.currentcity_filled} (${((q.currentcity_filled/q.total_records)*100).toFixed(1)}%)`);
    console.log(`   Current State (text) filled: ${q.currentstate_filled} (${((q.currentstate_filled/q.total_records)*100).toFixed(1)}%)`);
    console.log(`   Current City ID (FK) filled: ${q.currentcityid_filled} (${((q.currentcityid_filled/q.total_records)*100).toFixed(1)}%)`);
    console.log(`   Is Influencer = true: ${q.influencer_count} (${((q.influencer_count/q.total_records)*100).toFixed(1)}%)`);

    // Check unique nationalities
    console.log("\n\n🌍 UNIQUE NATIONALITY VALUES:\n");
    const nationalityResult = await client.query(`
      SELECT DISTINCT nationality, COUNT(*) as count
      FROM actordetails
      WHERE nationality IS NOT NULL AND nationality != ''
      GROUP BY nationality
      ORDER BY count DESC
      LIMIT 10
    `);

    nationalityResult.rows.forEach(row => {
      console.log(`   "${row.nationality}": ${row.count} records`);
    });

  } catch (err) {
    console.error("❌ Error:", err.message);
  } finally {
    await client.end();
  }
}

checkSchema();
