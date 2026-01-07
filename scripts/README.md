# Scripts - Production Ready

This directory contains production-ready scripts for the project.

## Structure

```
scripts/
├── main.py                   # Main script - runs entire pipeline
├── etl_pipeline.py           # Complete ETL pipeline
├── generate_reports.py       # Generate reports (CSV, JSON, HTML)
└── utils.py                  # Utility functions
```

## Production Scripts

### 1. `main.py` - Main Script
**Function**: Runs the entire pipeline from start to finish

**Includes**:
- Extract data from SQL Server
- Transform and join data
- Load into data lake
- Generate reports

**How to run**:
```bash
python scripts/main.py
```

### 2. `etl_pipeline.py` - ETL Pipeline
**Function**: Extract, Transform, Load data

**Features**:
- Extract from SQL Server via JDBC (Java Database Connectivity)
- Transform: Join tables, calculate metrics
- Load into data lake (Parquet format)
- Data quality validation
- Comprehensive logging
- Error handling

**How to run**:
```bash
python scripts/etl_pipeline.py
```

### 3. `generate_reports.py` - Generate Reports
**Function**: Generate reports from analytics data

**Features**:
- Export CSV reports
- Export JSON reports
- Generate beautiful HTML report
- Summary metrics

**How to run**:
```bash
python scripts/generate_reports.py
```

**Note**: Need to run `etl_pipeline.py` first to have data

### 4. `utils.py` - Utilities
**Function**: Utility functions

**Includes**:
- `load_config()` - Load config from YAML
- `setup_logging()` - Setup logging
- `create_spark_session()` - Create SparkSession with config
- `get_jdbc_properties()` - Get JDBC properties
- `get_hdfs_path()` - Get HDFS paths


## Workflow
### Run Complete Pipeline
```bash
python scripts/main.py
```

### Run Step by Step
```bash
# Step 1: ETL
python scripts/etl_pipeline.py

# Step 2: Generate Reports
python scripts/generate_reports.py
```

## Configuration

All configuration in `config/config.yaml`:
- Database connection
- Spark settings
- HDFS paths
- Logging config

## Output

After running, you will have:
- **Data**: `data_lake/adw/` (raw + analytics)
- **Reports**: `reports/` (CSV, JSON, HTML)
- **Logs**: `logs/etl_pipeline.log`

## Troubleshooting

See `QUICK_START.md` or `README.md` in the root directory for more details.
