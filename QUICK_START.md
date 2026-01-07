# Quick Start Guide

## Quick Installation

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Database configuration
Create configuration file from template:
```bash
# Copy template file
cp config/config.yaml.example config/config.yaml
```

Then open `config/config.yaml` and edit your database connection information:
```yaml
database:
  server: "localhost"
  port: 1433
  database_name: "AdventureWorks2022"
  username: "your_username"
  password: "your_password"
```

### 3. Check JDBC Driver
Ensure the JDBC driver path in `config/config.yaml` is correct:
```yaml
jdbc_driver:
  path: "file:///path/to/your/mssql-jdbc-12.10.1.jre8.jar"
```

### 4. Run complete pipeline
```bash
python scripts/main.py
```

Or run step by step:

**ETL only:**
```bash
python scripts/etl_pipeline.py
```

**Generate reports only (after running ETL):**
```bash
python scripts/generate_reports.py
```

## Results

After running, you will have:

1. **Data in HDFS** (or local filesystem):
   - Raw data: `/adw/raw/`
   - Analytics data: `/adw/analytics/`

2. **Reports in `reports/` directory:**
   - `sales_by_year_YYYYMMDD_HHMMSS.csv` - Revenue by year
   - `top_products_YYYYMMDD_HHMMSS.csv` - Top products
   - `sales_by_category_YYYYMMDD_HHMMSS.csv` - Revenue by category
   - `sales_report_YYYYMMDD_HHMMSS.html` - Beautiful HTML report

3. **Logs in `logs/` directory:**
   - `etl_pipeline.log` - Detailed execution process

## View Results

Open the HTML report file in your browser to view beautiful results:
```bash
# Windows
start reports/sales_report_*.html

# Linux/Mac
open reports/sales_report_*.html
```

## Project Structure

```
SalesCategoryAnalytics/
├── config/
│   └── config.yaml          # Database and Spark configuration
├── scripts/
│   ├── main.py              # Main script - runs everything
│   ├── etl_pipeline.py      # ETL pipeline
│   ├── generate_reports.py  # Generate reports
│   └── utils.py             # Utilities
├── reports/                 # Generated reports are stored here
├── logs/                    # Log files
├── data_lake/              # Data storage
└── requirements.txt        # Dependencies
```

## Troubleshooting

### Database connection error
- Check if SQL Server is running
- Verify username/password in config.yaml
- Check if port 1433 is open

### JDBC driver error
- Check driver path in config.yaml
- Ensure the .jar file exists

### HDFS error
- If you don't have HDFS, you can modify config to use local filesystem:
  ```yaml
  hdfs:
    namenode: "file:///path/to/your/project/data_lake"
  ```

## Support

See the `README.md` file for more details about the project.
