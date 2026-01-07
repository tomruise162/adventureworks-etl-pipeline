# Sales Category Analytics - Complete Data Engineering Project

A complete project to load data from SQL Server, process with Apache Spark, and generate reports.

## Features

- **Extract**: Load data from SQL Server (AdventureWorks2022)
- **Transform**: Process and join data with Spark
- **Load**: Save data to data lake (Parquet format)
- **Reports**: Generate CSV, JSON, and beautiful HTML reports

## Quick Installation

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configuration
Create configuration file from template:
```bash
# Copy template file
cp config/config.yaml.example config/config.yaml
```

Then edit `config/config.yaml` with your database information:
```yaml
database:
  username: "your_username"
  password: "your_password"
```

### 3. Run pipeline
```bash
python scripts/main.py
```

## Project Structure

```
SalesCategoryAnalytics/
├── config/
│   └── config.yaml              # Database and Spark configuration
├── scripts/
│   ├── basic/                   # Learning scripts (see scripts/basic/README.md)
│   │   ├── extract_data.py     # Basic script to learn data extraction
│   │   └── print_output.py     # Basic script to test connection
│   ├── main.py                  # Main script - runs entire pipeline
│   ├── etl_pipeline.py          # ETL pipeline (Extract, Transform, Load)
│   ├── generate_reports.py     # Generate reports from analytics
│   └── utils.py                # Utility functions
├── reports/                     # Generated reports are stored here
│   ├── sales_by_year_*.csv
│   ├── top_products_*.csv
│   ├── sales_by_category_*.csv
│   └── sales_report_*.html     # Beautiful HTML report
├── logs/                        # Log files
│   └── etl_pipeline.log
├── data_lake/                   # Data storage
│   └── adw/
│       ├── raw/                # Raw data
│       └── analytics/         # Processed analytics
├── requirements.txt            # Python dependencies
├── QUICK_START.md             # Quick start guide
└── README.md                   # This file
```

## Usage

### Production Scripts (Recommended)

**Run complete pipeline:**
```bash
python scripts/main.py
```

This script will:
1. Extract data from SQL Server
2. Transform and join tables
3. Load into data lake
4. Generate reports (CSV, JSON, HTML)

**Run individual steps:**

Run ETL only:
```bash
python scripts/etl_pipeline.py
```

Generate reports only (after running ETL):
```bash
python scripts/generate_reports.py
```

### Learning Scripts (Basic)

If you want to learn step by step, you can run the scripts in `scripts/basic/`:

```bash
# Test connection and display simple data
python scripts/basic/print_output.py

# Learn how to extract data from multiple tables
python scripts/basic/extract_data.py
```

**Note**: Basic scripts don't have error handling and config file, they are for learning purposes only. See `scripts/basic/README.md` for more details.

## Results

After running, you will have:

### 1. Data in Data Lake
- **Raw data**: `data_lake/adw/raw/` - Original data from SQL Server
- **Analytics data**: `data_lake/adw/analytics/` - Processed data
  - `sales_by_year` - Revenue by year
  - `top_products` - Top 50 products
  - `sales_by_category_year` - Revenue by category and year

### 2. Reports
All reports are saved in the `reports/` directory:

- **CSV Files**: 
  - `sales_by_year_YYYYMMDD_HHMMSS.csv`
  - `top_products_YYYYMMDD_HHMMSS.csv`
  - `sales_by_category_YYYYMMDD_HHMMSS.csv`

- **JSON Files**: Similar to CSV but in JSON format

- **HTML Report**: `sales_report_YYYYMMDD_HHMMSS.html`
  - Beautiful report with charts and tables
  - Open in browser to view

### 3. Logs
- `logs/etl_pipeline.log` - Detailed execution process

## Generated Analytics

### 1. Sales by Year
Revenue overview by year:
- Total Revenue
- Average Order Value
- Order Count
- Unique Customers

### 2. Top Products
Top 50 best-selling products:
- Product Name
- Category
- Total Revenue
- Total Quantity
- Order Count
- Average Unit Price

### 3. Sales by Category and Year
Detailed revenue by category and year/month:
- Category Name
- Year, Month
- Total Revenue
- Order Line Count
- Average Line Total
- Total Quantity

## Configuration

The `config/config.yaml` file contains all configuration:

```yaml
database:
  server: "localhost"
  port: 1433
  database_name: "AdventureWorks2022"
  username: "sa"
  password: "123456"

hdfs:
  namenode: "file:///path/to/your/project/data_lake"
  # Or use HDFS: "hdfs://localhost:9000"

spark:
  app_name: "SalesCategoryAnalytics"
  executor_memory: "2g"
  executor_cores: "2"
```

## System Requirements

- Python 3.8+
- Apache Spark 3.x
- SQL Server with AdventureWorks2022 database
- JDBC Driver for SQL Server (included in project)

## Troubleshooting

### Database connection error
- Check if SQL Server is running
- Verify username/password in `config/config.yaml`
- Check port 1433

### JDBC driver error
- Check driver path in `config/config.yaml`
- Ensure the `.jar` file exists

### HDFS error
- If you don't have HDFS, the project will use local filesystem
- Check the path in `config/config.yaml`

## View Results

### View HTML Report
```bash
# Windows
start reports/sales_report_*.html

# Linux/Mac  
open reports/sales_report_*.html
```

### View CSV in Excel
Open CSV file in Excel or any spreadsheet application.

### View data in Spark
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ViewData").getOrCreate()
df = spark.read.parquet("data_lake/adw/analytics/sales_by_year")
df.show()
```

## Support

See `QUICK_START.md` for more detailed instructions.

---

**Happy Analyzing!**
