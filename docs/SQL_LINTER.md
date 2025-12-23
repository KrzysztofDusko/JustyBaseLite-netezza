# SQL Linter

JustyBaseLite includes a built-in SQL linter that provides real-time feedback on common SQL anti-patterns and potential issues specific to Netezza.

## Configuration

Enable/disable the linter and customize rule severity in VS Code settings:

```json
{
  "netezza.linter.enabled": true,
  "netezza.linter.rules": {
    "NZ001": "warning",
    "NZ002": "error",
    "NZ007": "off"
  }
}
```

**Severity levels:** `error`, `warning`, `information`, `hint`, `off`

## Available Rules

| Rule | Default | Description |
|------|---------|-------------|
| **NZ001** | Warning | `SELECT *` usage - recommend explicit column names |
| **NZ002** | Error | `DELETE` without `WHERE` clause |
| **NZ003** | Error | `UPDATE` without `WHERE` clause |
| **NZ004** | Warning | `CROSS JOIN` detected - Cartesian product warning |
| **NZ005** | Hint | Leading wildcard `LIKE '%...'` prevents index usage |
| **NZ006** | Info | `ORDER BY` without `LIMIT` on large result sets |
| **NZ007** | Information | Inconsistent keyword casing (mixed UPPER/lower) |
| **NZ008** | Warning | `TRUNCATE` statement - data loss warning |
| **NZ009** | Hint | Multiple `OR` conditions - consider `UNION` |
| **NZ010** | Info | Missing table alias in `JOIN` |
| **NZ011** | Warning | `CREATE TABLE AS SELECT` missing `DISTRIBUTE ON` |

## Examples

### NZ002 - DELETE without WHERE
```sql
-- ❌ Error: Will delete all rows
DELETE FROM customers;

-- ✅ OK: Has WHERE clause
DELETE FROM customers WHERE status = 'inactive';
```

### NZ005 - Leading Wildcard LIKE
```sql
-- ⚠️ Hint: Cannot use index
SELECT * FROM products WHERE name LIKE '%widget';

-- ✅ Better: Index can be used
SELECT * FROM products WHERE name LIKE 'widget%';
```

### NZ007 - Inconsistent Casing
```sql
-- ⚠️ Warning: Mixed case keywords
SELECT col1 from table1 WHERE id = 1;

-- ✅ Consistent: All uppercase
SELECT col1 FROM table1 WHERE id = 1;
```

### NZ011 - CTAS Missing Distribution
```sql
-- ⚠️ Warning: Missing distribution
CREATE TABLE copy_t AS SELECT * FROM original;

-- ✅ OK: Explicit distribution
CREATE TABLE copy_t AS SELECT * FROM original DISTRIBUTE ON RANDOM;
```

## Smart Detection

The linter correctly ignores patterns inside:
- String literals (`'SELECT * FROM ...'`)
- Line comments (`-- SELECT * is bad`)
- Block comments (`/* DELETE FROM table */`)
