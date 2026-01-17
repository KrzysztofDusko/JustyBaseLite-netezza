/**
 * Test for improved table reference extraction
 * 
 * This demonstrates the fix for parsing DB.SCHEMA.TABLE format
 */

// Test SQL from user:
const testSQL = `
SELECT * FROM JUST_DATA.ADMIN.DIMACCOUNT A 
WHERE A.ACCOUNTCODEALTERNATEKEY = '40000'
`;

// Expected parsing:
// Database: JUST_DATA
// Schema: ADMIN
// Table: DIMACCOUNT

// Regex patterns now handle 3 parts:
const patterns = [
    /FROM\s+(?:(\w+)\.)?(?:(\w+)\.)?(\w+)/gi,
];

const pattern = patterns[0];
let match;
const results = [];

while ((match = pattern.exec(testSQL)) !== null) {
    console.log('Match groups:');
    console.log('  [1] (first part):', match[1]);  // JUST_DATA
    console.log('  [2] (second part):', match[2]); // ADMIN
    console.log('  [3] (third part):', match[3]);  // DIMACCOUNT
    
    let database;
    let schema;
    let tableName;

    if (match[3]) {
        // All 3 parts: DB.SCHEMA.TABLE
        database = match[1];
        schema = match[2];
        tableName = match[3];
    } else if (match[2]) {
        // 2 parts: SCHEMA.TABLE
        schema = match[1];
        tableName = match[2];
    } else if (match[1]) {
        // 1 part: TABLE
        tableName = match[1];
    }

    results.push({
        database,
        schema,
        tableName,
        fullName: `${database ? database + '.' : ''}${schema ? schema + '.' : ''}${tableName}`
    });
}

console.log('\n=== Parsed Table References ===');
results.forEach((r, i) => {
    console.log(`\nResult ${i + 1}:`);
    console.log(`  Database: ${r.database || '(none)'}`);
    console.log(`  Schema: ${r.schema || '(none)'}`);
    console.log(`  Table: ${r.tableName}`);
    console.log(`  Full name: ${r.fullName}`);
});

// Expected output:
// Database: JUST_DATA
// Schema: ADMIN
// Table: DIMACCOUNT
// Full name: JUST_DATA.ADMIN.DIMACCOUNT
