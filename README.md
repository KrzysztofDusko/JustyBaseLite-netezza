<div align="center">
  <img src="netezza_icon64.png" />
</div>

# JustyBaseLite - Netezza / PureData System for Analytics

A powerful, **Zero Config** VS Code extension for working with IBM Netezza / PureData System for Analytics databases.
Distinct from other extensions, JustyBaseLite includes a community based **Node.js-based Netezza driver** ([node-netezza](https://github.com/markddrake/node-netezza)), eliminating the need to install or configure IBM ODBC drivers. Just install and connect!

## Features

### 🚀 Query Execution
- **Zero Configuration**: Connect immediately using host, user, and password. No ODBC setup required.
- **Progressive Results**: Results appear immediately as queries finish, even when running multiple statements.
- **Sequential Execution**: Run complex scripts with multiple statements safely.
- **Run Selection**: Execute selected text or the current statement (`Ctrl+Enter` / `F5`).
- **Cancel Query**: Stop long-running queries instantly.
- **Explain Plan**: Visualize query execution plan (`Ctrl+L`).
- **SQL Formatter**: Auto-format SQL code (`Shift+Alt+F`).
- 📖 **[Query Execution & Analysis Guide](docs/QUERY_EXECUTION.md)**


### 🔎 Schema Browser
- **Object Explorer**: Browse Databases, Schemas, Tables, Views, Procedures, Sequences, and Synonyms.
- **Search**: Quickly find objects across the entire system.
- **Rich Metadata**: View column types, primary keys, and specialized object properties.

### 📊 Results & Export
- **Data Grid**: Full-featured grid with filtering, sorting, and cell selection.
- **Multi-Grid Export**:
    - **Excel (XLSB)**: Export all result sets to a single Excel file with multiple sheets.
    - **CSV, JSON, XML, SQL INSERT, Markdown**: Multiple export format options.
    - **Open Immediately**: Option to open Excel files automatically after export.
- 📖 **[Full Export/Import Reference](docs/EXPORT_IMPORT.md)**

### 📥 Data Import & Smart Paste
- **Import Wizard**: Import CSV/TSV/Excel files directly into new or existing tables.
- **Locale-Aware**: Correctly handles numbers with comma decimals based on content.
- **Smart Paste (`Ctrl+Shift+V`)**: Paste data directly from Excel or other sources; the extension auto-detects structure (Excel XML, CSV, etc.) and generates an `INSERT` statement.


### 🛠️ Table & Object Management
Right-click on objects in the Schema Browser for powerful context actions:
- **Maintenance**:
    - **Groom Table**: Reclaim space and organize records.
    - **Generate Statistics**: Update optimizer statistics.
    - **Truncate Table**: Quickly empty tables.
    - **Recreate Table**: Generate a maintenance script to recreate a table (useful for skew fixing).
- **Modification**:
    - **Rename Table**: Safely rename tables.
    - **Change Owner**: Transfer object ownership.
    - **Add Primary Key**: GUI for adding PK constraints.
    - **Add/Edit Comments**: Manage object comments.
- **Analysis**:
    - **Compare With...**: Compare table structures or procedure definitions with another object.
    - **Check Data Skew**: Analyze distribution of data across slices.
    - **View/Edit Data**: Edit table rows directly (with limit safeguards).
- 📖 **[Schema Comparison Guide](docs/SCHEMA_COMPARE.md)**

### ⚡ Professional Development
- **DDL Generation**: Generate production-ready DDL for Tables, Views, and Procedures (including arguments and returns).
- **Batch DDL Export**: Export DDL for an entire database or all objects of a type (Tables, Views, Procedures) at once.
- **Procedure Support**:
    - **Create Procedure**: Template for new NZPLSQL procedures.
    - **Notice Handling**: Captures and prints `RAISE NOTICE` output to the "Netezza Logs" channel.
    - **Signature Support**: Correctly parses and displays full procedure signatures.

### 📈 Query Monitoring Dashboard
- **Session Monitor**: Real-time view of active sessions, running queries, and system resources.
- **Running Queries**: View currently executing queries with estimated cost, elapsed time, and ability to kill sessions.
- **Resources**: Monitor CPU, Memory, Disk, and Fabric utilization across SPUs with system utilization summary.
- **Storage Statistics**: Analyze table storage, used bytes, and data skew (weighted average) per schema and database.
- **Access**: Right-click on a database in the Schema Browser → **Open Monitor Dashboard**.

### 🗺️ Entity Relationship Diagram (ERD)
- **Visual Schema Exploration**: Generate interactive diagrams showing tables and their relationships.
- **Foreign Key Visualization**: Display Primary Key (PK) and Foreign Key (FK) relationships between tables.
- **Column Details**: View column names, data types, and key indicators directly in the diagram.
- **Access**: Right-click on a schema in the Schema Browser → **Generate ERD**.

### ✂️ SQL Snippets
- **55+ Code Snippets**: Type `nz` followed by a keyword to quickly insert SQL templates.
- **Categories**: Basic SQL, DDL, Netezza-specific (GROOM, GENERATE STATISTICS), NZPLSQL procedures, query patterns.
- **Usage**: Type prefix (e.g., `nzselect`, `nzprocedure`, `nzgroom`) → Press `Tab`.
- 📖 **[Full Snippets Reference](docs/SNIPPETS.md)**


## Requirements

- **VS Code**: v1.80.0 or higher.
- **No external drivers required**: The extension bundles its own pure JavaScript/TypeScript driver for Netezza.

## Setup

1.  **Install**: Search for "JustyBaseLite" in the VS Code Marketplace and install.
2.  **Connect**:
    - Click the **Netezza** icon in the Activity Bar.
    - Click **Connect** (or edit User Settings).
    - Enter `Host`, `User`, `Password`, and `Database`.

## Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| `Ctrl+Enter` / `F5` | Run Current Statement / Selection |
| `Ctrl+F5` | Run Query Batch |
| `Ctrl+Shift+V` | Smart Paste (Auto-detect Excel XML/CSV) |

## Development

1.  Clone the repository.
2.  Run `npm install`.
3.  Press `F5` to launch a debugging instance of VS Code.
4.  Run `npm run watch` to automatically compile TS changes.
