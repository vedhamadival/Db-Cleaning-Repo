const { Client } = require("pg");

async function checkDatabase() {
  const client = new Client({
    user: "ved",
    host: "localhost",
    database: "v2starcast_new",
    password: "yourpassword",
    port: 5432,
  });

  try {
    await client.connect();
    console.log("✅ Connected to v2starcast_new\n");

    // List all tables
    const tablesResult = await client.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public'
      ORDER BY table_name
    `);

    console.log("📋 Existing tables:");
    if (tablesResult.rows.length === 0) {
      console.log("   (None - database is empty)\n");
    } else {
      tablesResult.rows.forEach((row) => {
        console.log(`   • ${row.table_name}`);
      });
      console.log();
    }

    // Check for "user" and "talent" tables specifically
    const userTableCheck = await client.query(`
      SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_name='user')
    `);

    const talentTableCheck = await client.query(`
      SELECT EXISTS(SELECT FROM information_schema.tables WHERE table_name='talent')
    `);

    console.log("📊 Target tables status:");
    console.log(`   user table exists: ${userTableCheck.rows[0].exists}`);
    console.log(`   talent table exists: ${talentTableCheck.rows[0].exists}\n`);

  } catch (error) {
    console.error("❌ Error:", error.message);
  } finally {
    await client.end();
  }
}

checkDatabase();
