# Basic Scripts - Learning Scripts

This directory contains basic scripts for learning and practice.

## Purpose

The scripts in this directory are designed to:
- Learn how to connect SQL Server with Spark
- Understand how to read data from database
- Practice basic operations with Spark DataFrame
- Get familiar with PySpark syntax

## Scripts

### 1. `extract_data.py`
**Purpose**: Learn how to extract data from SQL Server

**Functions**:
- Create SparkSession
- Connect to SQL Server via JDBC
- Read multiple tables from database
- Display row counts

**How to run**:
```bash
python scripts/basic/extract_data.py
```

### 2. `print_output.py`
**Purpose**: Test connection and display simple data

**Functions**:
- Connect to SQL Server
- Read one table
- Display sample data and schema

**How to run**:
```bash
python scripts/basic/print_output.py
```

## Notes

- These scripts do **NOT** have comprehensive error handling
- These scripts do **NOT** use config file
- Hardcoded connection strings (for learning only)
- No logging
- No data validation

## Production Scripts

To see production-ready scripts with full features, see:
- `scripts/main.py` - Main script that runs entire pipeline
- `scripts/etl_pipeline.py` - Complete ETL pipeline
- `scripts/generate_reports.py` - Generate reports

## Learning

Use these scripts to:
1. Understand how Spark connects to database
2. Learn how to read data from JDBC
3. Practice with Spark DataFrame API
4. Get familiar with PySpark

After understanding well, switch to using production scripts in the `scripts/` directory.
