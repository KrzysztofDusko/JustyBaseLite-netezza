# Schema/Object Comparison (Diff)

Compare table structures and procedure definitions between different objects in your Netezza database.

## How to Use

1. **Right-click** on a TABLE, VIEW, or PROCEDURE in the Schema Explorer
2. Select **"Compare With..."** from the context menu
3. **Pick** another object of the same type from the Quick Pick list
4. View the comparison results in a dedicated panel

## Table Comparison

When comparing tables, the following properties are analyzed:

| Property | Description |
|----------|-------------|
| **Columns** | Name, data type, NOT NULL constraint, default value |
| **Keys** | Primary keys, foreign keys, unique constraints |
| **Distribution** | DISTRIBUTE ON columns |
| **Organization** | ORGANIZE ON columns |

### Status Indicators

| Icon | Meaning |
|------|---------|
| 🟢 | Added (exists only in target) |
| 🔴 | Removed (exists only in source) |
| 🟡 | Modified (exists in both with differences) |
| ⚪ | Unchanged (identical in both) |

## Procedure Comparison

When comparing procedures, the following properties are analyzed:

- **Arguments**: Input/output parameters
- **Returns**: Return type
- **Execute As**: OWNER or CALLER
- **Source Code**: Full procedure body with line-by-line diff

## Example Use Cases

- Compare identical tables in different schemas (DEV vs PROD)
- Verify table structure after migration
- Review procedure changes before deployment
- Identify schema drift between environments
