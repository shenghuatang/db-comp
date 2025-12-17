# Database Compare Tool

A powerful Python tool for comparing data from two database sources with support for multiple database types, tolerance-based numeric comparison, and comprehensive reporting.

## Features

- **Multi-Database Support**: MySQL, PostgreSQL, Oracle, SQL Server, SQLite
- **Composite Keys**: Support for single or multiple join columns
- **Tolerance-Based Comparison**: Configure numeric tolerances per column
- **Column Selection**: Compare specific columns or all columns
- **Duplicate Detection**: Identify duplicate rows based on join keys
- **Multiple Output Formats**:
  - Full comparison CSV with side-by-side data
  - Differences-only CSV
  - Summary text report
- **YAML Configuration**: Define multiple comparison jobs in YAML
- **Memory Monitoring**: Track memory usage during comparison
- **Comprehensive Logging**: Detailed logs with rotation

## Installation

### Required Packages

```bash
pip install pandas numpy sqlalchemy pyyaml
```

### Database-Specific Drivers

Install drivers for your database types:

```bash
# MySQL
pip install pymysql

# PostgreSQL
pip install psycopg2-binary

# Oracle
pip install cx_oracle

# SQL Server
pip install pyodbc

# SQLite (included in Python standard library)
```

## Quick Start

### Method 1: Programmatic Usage

```python
from db_compare import DBCompare, DataSource

# Configure data sources
source1 = DataSource(
    name="production",
    db_type="mysql",
    host="localhost",
    port=3306,
    database="prod_db",
    username="user",
    password="pass",
    sql_query="SELECT id, name, value FROM table1"
)

source2 = DataSource(
    name="staging",
    db_type="mysql",
    host="localhost",
    port=3306,
    database="stage_db",
    username="user",
    password="pass",
    sql_query="SELECT id, name, value FROM table1"
)

# Create comparator
comparator = DBCompare(
    data_source1=source1,
    data_source2=source2,
    join_columns=["id"],
    comparing_columns=["name", "value"]
)

# Run comparison
results = comparator.run_comparison()
print(results)
```

### Method 2: YAML Configuration

Create `db_compare.yaml`:

```yaml
comparisons:
  my_comparison:
    data_source1:
      name: "production"
      db_type: "mysql"
      host: "localhost"
      port: 3306
      database: "prod_db"
      username: "user"
      password: "pass"
      sql_query: "SELECT id, name, value FROM table1"
    
    data_source2:
      name: "staging"
      db_type: "mysql"
      host: "localhost"
      port: 3306
      database: "stage_db"
      username: "user"
      password: "pass"
      sql_query: "SELECT id, name, value FROM table1"
    
    join_columns: ["id"]
    comparing_columns: ["name", "value"]
    output_dir: "output/my_comparison"
```

Run:

```bash
python db_compare_runner.py -y db_compare.yaml
```

## Usage Examples

### Example 1: Basic Comparison

```python
from db_compare import DBCompare, DataSource

source1 = DataSource(
    name="db1",
    db_type="mysql",
    host="localhost",
    port=3306,
    database="database1",
    username="root",
    password="password",
    sql_query="SELECT customer_id, name, balance FROM customers"
)

source2 = DataSource(
    name="db2",
    db_type="mysql",
    host="localhost",
    port=3306,
    database="database2",
    username="root",
    password="password",
    sql_query="SELECT customer_id, name, balance FROM customers"
)

comparator = DBCompare(
    data_source1=source1,
    data_source2=source2,
    join_columns="customer_id"
)

results = comparator.run_comparison()
```

### Example 2: Composite Key

```python
comparator = DBCompare(
    data_source1=source1,
    data_source2=source2,
    join_columns=["store_id", "product_id", "date"],  # Multiple columns
    comparing_columns=["quantity", "revenue"]
)

results = comparator.run_comparison()
```

### Example 3: Numeric Tolerance

```python
comparator = DBCompare(
    data_source1=source1,
    data_source2=source2,
    join_columns="account_id",
    tolerance={
        "balance": 0.01,        # Allow $0.01 difference
        "interest": 0.001       # Allow $0.001 difference
    }
)

results = comparator.run_comparison()
```

### Example 4: Cross-Database Comparison

```python
# MySQL source
mysql_source = DataSource(
    name="mysql_db",
    db_type="mysql",
    host="mysql-server",
    port=3306,
    database="mydb",
    username="user",
    password="pass",
    sql_query="SELECT id, name FROM table1"
)

# PostgreSQL source
pg_source = DataSource(
    name="postgres_db",
    db_type="postgresql",
    host="pg-server",
    port=5432,
    database="mydb",
    username="user",
    password="pass",
    sql_query="SELECT id, name FROM table1"
)

comparator = DBCompare(
    data_source1=mysql_source,
    data_source2=pg_source,
    join_columns="id"
)

results = comparator.run_comparison()
```

### Example 5: Selective Reports

```python
comparator = DBCompare(
    data_source1=source1,
    data_source2=source2,
    join_columns="id"
)

# Only generate differences and summary
results = comparator.run_comparison(
    generate_full_csv=False,
    generate_diff_csv=True,
    generate_summary=True,
    validate_dups=True
)
```

## Configuration Reference

### DataSource Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| name | str | Yes | Name of the data source |
| db_type | str | Yes | Database type: mysql, postgresql, oracle, mssql, sqlite |
| host | str | Yes* | Database host (*not for SQLite) |
| port | int | Yes* | Database port (*not for SQLite) |
| database | str | Yes | Database name (or file path for SQLite) |
| username | str | Yes* | Database username (*not for SQLite) |
| password | str | Yes* | Database password (*not for SQLite) |
| sql_query | str | Yes | SQL query to fetch data |

### DBCompare Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| data_source1 | DataSource | Yes | First data source |
| data_source2 | DataSource | Yes | Second data source |
| join_columns | str or list | Yes | Column(s) to join on |
| comparing_columns | list | No | Columns to compare (None = all) |
| tolerance | dict | No | Column-specific numeric tolerances |
| abs_tol | float | No | Absolute tolerance for all numerics |
| rel_tol | float | No | Relative tolerance for all numerics |
| output_dir | str | No | Output directory (default: "output") |
| log_file | str | No | Log file path |

## Output Files

### 1. comparison_report.csv

Full comparison report with:
- All rows from both sources
- Side-by-side columns (with suffixes `_source1` and `_source2`)
- `_merge` column indicating row presence
- `is_equal` column indicating overall match
- Individual `{column}_match` columns

### 2. differences_only.csv

Contains only rows where `is_equal = False`

### 3. summary_report.txt

Text summary including:
- Data source information
- Row counts
- Comparison results
- Match percentage
- Row distribution

### 4. duplicates_{source_name}.csv

Lists duplicate rows (if found) based on join columns

## Command Line Interface

### db_compare_runner.py

Run YAML-based comparisons:

```bash
# Run all jobs
python db_compare_runner.py

# Custom YAML file
python db_compare_runner.py -y config.yaml

# Run specific job
python db_compare_runner.py -j job_name

# Custom log file
python db_compare_runner.py -o mylog.log

# Combined
python db_compare_runner.py -y config.yaml -j job1 -o job1.log
```

**Arguments:**
- `-y, --yaml`: YAML configuration file (default: db_compare.yaml)
- `-o, --output`: Log file path (default: db_compare_runner.log)
- `-j, --job`: Specific job name to run (default: run all)

## Supported Databases

### MySQL

```python
DataSource(
    db_type="mysql",
    host="localhost",
    port=3306,
    # ... other params
)
```

Requires: `pymysql`

### PostgreSQL

```python
DataSource(
    db_type="postgresql",  # or "postgres"
    host="localhost",
    port=5432,
    # ... other params
)
```

Requires: `psycopg2-binary`

### Oracle

```python
DataSource(
    db_type="oracle",
    host="localhost",
    port=1521,
    # ... other params
)
```

Requires: `cx_oracle`

### SQL Server

```python
DataSource(
    db_type="mssql",  # or "sqlserver"
    host="localhost",
    port=1433,
    # ... other params
    connection_params={
        "driver": "ODBC Driver 17 for SQL Server"
    }
)
```

Requires: `pyodbc`

### SQLite

```python
DataSource(
    db_type="sqlite",
    database="path/to/database.db",  # File path
    host="",  # Not used
    port=0,   # Not used
    username="",  # Not used
    password="",  # Not used
    sql_query="SELECT * FROM table1"
)
```

No additional driver needed (built-in)

## Best Practices

1. **Use Read-Only Accounts**: Configure database users with read-only permissions
2. **Optimize Queries**: Use WHERE clauses to limit data retrieval
3. **Index Join Columns**: Ensure join columns are indexed in databases
4. **Set Appropriate Tolerances**: For financial data, use appropriate decimal precision
5. **Validate Data**: Enable duplicate validation for data quality
6. **Monitor Memory**: Check logs for memory usage on large datasets
7. **Secure Passwords**: Use environment variables or secure vaults for passwords

## Troubleshooting

### Connection Errors

**Problem**: Cannot connect to database

**Solutions**:
- Verify host, port, username, password
- Check firewall rules
- Ensure database driver is installed
- Test connection using database client first

### Memory Issues

**Problem**: Out of memory errors

**Solutions**:
- Limit rows with WHERE clause in SQL
- Use `comparing_columns` to load fewer columns
- Process in batches using multiple jobs

### Performance Issues

**Problem**: Slow comparison

**Solutions**:
- Add indexes on join columns
- Optimize SQL queries
- Use `comparing_columns` to reduce data
- Run during off-peak hours

### Type Mismatches

**Problem**: Cannot compare different data types

**Solutions**:
- Cast columns to same type in SQL query
- Use `CAST()` or `CONVERT()` in queries
- Ensure join columns have matching types

## Examples

See `db_compare_examples.py` for comprehensive examples including:
- Basic MySQL comparison
- PostgreSQL with tolerance
- Composite key comparison
- Cross-database comparison
- SQLite comparison
- Selective report generation

## Version

Current version: 1.0.0

## License

See project repository for license information.

