# JustyBaseLite

A powerful VS Code extension for working with IBM Netezza / PureData System for Analytics databases via ODBC.

## Features

- **Query Execution**: Run SQL queries with `Ctrl+Enter` or `F5`
- **Schema Browser**: Browse databases, schemas, tables, and views in sidebar
- **Query History**: Track and re-run past queries
- **Object Search**: Quickly find database objects
- **Results Panel**: View query results in a full-featured data grid with filtering, grouping, and aggregation
- **Export**: Export results to Excel (XLSX) or CSV
- **Data Import**: Import data from CSV/TSV files to tables
- **DDL Generation**: Generate CREATE statements for tables, views, procedures, and functions
- **Table Management**: Grant permissions, groom tables, generate statistics, truncate, add primary keys

## Requirements

- **Node.js**: Required for building the extension
- **Netezza ODBC Driver**: Must be installed and configured on your system

## Setup

1.  **Install Dependencies**:
    Run `npm install` in the terminal to install the necessary Node.js packages.

2.  **Build Extension**:
    Run `npm run compile` to compile TypeScript sources.

3.  **Configuration**:
    - Open VS Code Settings (`Ctrl+,`).
    - Search for `netezza`.
    - Configure your Netezza connection using the **Connect** button in the Schema view, or set a default connection string.

## Usage

### Running Queries

1. Open a `.sql` file or change language mode to SQL.
2. Type a Netezza SQL query, e.g., `SELECT * FROM _v_system_info LIMIT 10;`
3. Select the query text (optional - will run current statement if no selection).
4. Press `Ctrl+Enter` or `F5` to execute.
5. Results appear in the **Query Results** panel.

### Schema Browser

- Click the **Netezza** icon in the Activity Bar to open the Schema Browser.
- Click **Connect** to connect to a database.
- Expand databases, schemas, and object types to explore.
- Right-click objects for actions like Copy Name, Drop, Grant Permissions, etc.

### Exporting Results

- Use toolbar buttons or context menu to export to:
  - **Excel (XLSX)**: Full spreadsheet with formatting
  - **CSV**: Plain text comma-separated values

### Importing Data

- Right-click in editor and select **Import Data to Table** to import from CSV/TSV files.
- Supports automatic delimiter detection and data type inference.

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+Enter` / `F5` | Run Query |
| `Ctrl+F5` | Run Query Batch (Legacy) |
| `Ctrl+Shift+V` | Smart Paste (Auto-detect Excel XML) |

## Development

1. Press `F5` to start debugging in Extension Development Host.
2. Make changes to TypeScript files in `src/`.
3. Run `npm run watch` for automatic recompilation.
