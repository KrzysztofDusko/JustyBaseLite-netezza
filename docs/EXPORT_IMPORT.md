# Data Export & Import Reference

JustyBaseLite provides comprehensive data export and import capabilities for seamless data transfer.

---

## Export Formats

### Excel (XLSB) - Recommended

**Binary Excel format** - compact, fast, supports large datasets.

| Method | Description |
|--------|-------------|
| **Export to Excel** | Save query results to `.xlsb` file |
| **Export & Open** | Export and immediately open in Excel |
| **Copy as Excel** | Copy results to clipboard (paste directly into Excel) |

**Features:**
- Multiple result sets → multiple sheets
- Preserves numeric types
- Includes SQL queries in separate "_SQL" sheet
- File size ~60% smaller than `.xlsx`

### CSV

Standard comma-separated format, compatible with any application.

### JSON

Export results as JSON array for use in applications and APIs.

### XML

Export results as XML document.

### SQL INSERT

Generate `INSERT INTO` statements for recreating data in another database.

### Markdown

Export results as Markdown table for documentation.

---

## How to Export

### From Results Panel

1. Run your query (`Ctrl+Enter` or `F5`)
2. In the Results panel, use the **Export** split button:
   - Click main button → Export to Excel (default)
   - Click arrow → Select format (CSV, JSON, XML, SQL INSERT, Markdown)

### From Editor Toolbar

Icons available when editing `.sql` files:

| Icon | Command |
|------|---------|
| 📋 Copy | **Excel Copy** - Copy results as Excel to clipboard |
| 📂 File | **Export to Excel and Open** |
| ⬇️ Export | **Export to Excel** (save file) |
| 📄 Text | **Export to CSV** |


### From Context Menu

Right-click on selected SQL → Select export option

### Keyboard Shortcuts

No default shortcuts, but you can assign them via **Keyboard Shortcuts** settings.

---

## Data Import

### Import from Files

JustyBaseLite can import data from:
- **CSV / TSV** files (auto-detects delimiter)
- **Excel files** (`.xlsx`, `.xlsb`)

**How to use:**

1. Right-click on a table in Schema Browser → **Import Data**
2. Or use Command Palette → `Netezza: Import Data`
3. Select source file
4. Review detected column types
5. Confirm import

**Features:**
- Automatic data type detection (INTEGER, BIGINT, NUMERIC, VARCHAR, DATE, TIMESTAMP)
- Locale-aware number parsing (handles both `.` and `,` decimals)
- UTF-8 BOM handling
- Progress reporting for large files

### Import from Clipboard

Paste data directly from Excel or other sources.

**How to use:**

1. Copy data in Excel (or other source)
2. Right-click on table in Schema Browser → **Import Clipboard Data**
3. Or use toolbar button ![clippy]($(clippy))
4. Data is automatically parsed and inserted

**Supported clipboard formats:**
- Excel XML Spreadsheet (when copying from Excel)
- Tab-separated text
- Comma-separated text

### Smart Paste (`Ctrl+Shift+V`)

Automatically detect and format pasted data as SQL.

**How to use:**

1. Copy data from Excel or other source
2. Position cursor in SQL editor
3. Press `Ctrl+Shift+V`
4. Extension auto-detects format and generates `INSERT` statement

**Detects:**
- Excel XML format
- CSV format
- Tab-separated format

---

## Tips

### Large Exports

- XLSB format handles millions of rows efficiently
- Progress indicator shows export status
- Consider using `LIMIT` for initial testing

### Import Performance

- For very large files, import may take several minutes
- Extension creates temporary files during import
- Import uses batch operations for speed

### Clipboard Limitations

- Clipboard imports are best for smaller datasets
- For large data, use file import instead

---

## Commands Reference

| Command | Description |
|---------|-------------|
| `Netezza: Export Query to Excel` | Export to XLSB file |
| `Netezza: Export Query to CSV` | Export to CSV file |
| `Netezza: Export Query to Excel and Open` | Export and open immediately |
| `Netezza: Copy Query Results as Excel to Clipboard` | Copy for pasting into Excel |
| `Netezza: Import Data to Table` | Import from file |
| `Netezza: Import Clipboard Data to Table` | Import from clipboard |
| `Netezza: Smart Paste` | Auto-detect and paste data |
