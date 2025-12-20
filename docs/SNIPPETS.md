# SQL Snippets Reference

JustyBaseLite includes **55+ SQL code snippets** for rapid SQL development with Netezza.

## How to Use

1. Open any `.sql` file
2. Type a snippet prefix (e.g., `nzselect`)
3. Press `Tab` or accept from IntelliSense dropdown
4. Use `Tab` to jump between placeholders, `Shift+Tab` to go back

> **Tip**: All snippets start with `nz` prefix for easy discovery.

---

## Available Snippets

### Basic SQL Operations

| Prefix | Description |
|--------|-------------|
| `nzselect` | SELECT with columns and WHERE clause |
| `nzselectall` | SELECT * with LIMIT |
| `nzselecttop` | SELECT TOP with wildcard |
| `nzinsert` | INSERT INTO statement |
| `nzinsertmulti` | INSERT multiple rows |
| `nzupdate` | UPDATE with SET and WHERE |
| `nzdelete` | DELETE with WHERE clause |
| `nzmerge` | MERGE statement template |

### DDL Operations

| Prefix | Description |
|--------|-------------|
| `nzcreatetable` | CREATE TABLE with distribution |
| `nzcreatetableas` | CREATE TABLE AS SELECT (CTAS) |
| `nzcreateview` | CREATE VIEW template |
| `nzaltertableadd` | ALTER TABLE ADD COLUMN |
| `nzaltertabledrop` | ALTER TABLE DROP COLUMN |
| `nzdroptable` | DROP TABLE IF EXISTS |
| `nzdropview` | DROP VIEW IF EXISTS |
| `nztruncate` | TRUNCATE TABLE |

### Netezza-Specific Operations

| Prefix | Description |
|--------|-------------|
| `nzgroom` | GROOM TABLE VERSIONS |
| `nzgroomall` | GROOM TABLE RECORDS ALL |
| `nzstats` | GENERATE STATISTICS for table |
| `nzstatscols` | GENERATE STATISTICS for columns |
| `nzexternaltable` | CREATE EXTERNAL TABLE |
| `nzdistribute` | DISTRIBUTE ON clause |
| `nzdistributehash` | DISTRIBUTE ON HASH |
| `nzdistributerandom` | DISTRIBUTE ON RANDOM |
| `nzorganize` | ORGANIZE ON clause |

### NZPLSQL Procedures & Functions

| Prefix | Description |
|--------|-------------|
| `nzprocedure` | Complete stored procedure skeleton |
| `nzfunction` | Scalar function template |
| `nzifelse` | IF/ELSIF/ELSE block |
| `nzforloop` | FOR loop with range |
| `nzwhileloop` | WHILE loop |
| `nzcursor` | Cursor declaration and loop |
| `nzexception` | Exception handling block |
| `nzraise` | RAISE NOTICE for debugging |
| `nzraiseexception` | RAISE EXCEPTION |
| `nzvar` | Variable declaration |
| `nzexecute` | Execute dynamic SQL |

### Query Patterns

| Prefix | Description |
|--------|-------------|
| `nzjoin` | INNER JOIN template |
| `nzleftjoin` | LEFT JOIN template |
| `nzgroupby` | GROUP BY with aggregates |
| `nzcte` | Common Table Expression (WITH) |
| `nzctesmulti` | Multiple CTEs |
| `nzcase` | CASE WHEN expression |
| `nzcasesimple` | Simple CASE expression |
| `nzunion` | UNION ALL query |
| `nzexists` | EXISTS subquery |
| `nznotexists` | NOT EXISTS subquery |

### Window Functions

| Prefix | Description |
|--------|-------------|
| `nzrownumber` | ROW_NUMBER() OVER |
| `nzrank` | RANK() OVER |

### Utility Functions

| Prefix | Description |
|--------|-------------|
| `nzcoalesce` | COALESCE function |
| `nznvl` | NVL function |
| `nzcast` | CAST expression |
| `nzdate` | Common date functions |
| `nzstring` | Common string functions |
| `nzgrantselect` | GRANT SELECT |
| `nzgrantall` | GRANT ALL |

---

## Custom User Snippets

You can create your own SQL snippets in VS Code:

1. Open Command Palette (`Ctrl+Shift+P`)
2. Select **"Preferences: Configure User Snippets"**
3. Choose **"sql.json"**
4. Add your custom snippets in the same JSON format

Example:
```json
{
  "My Custom Query": {
    "prefix": "myquery",
    "body": [
      "SELECT * FROM my_schema.my_table",
      "WHERE date_column >= CURRENT_DATE - ${1:7};"
    ],
    "description": "My frequently used query"
  }
}
```
