# Practice Guide with AdventureWorks2022
## Applying Roadmap to Real Database

---

## Why AdventureWorks2022 is Suitable?

AdventureWorks2022 is a sample database from Microsoft with:
- **Diverse data**: Sales, Production, Human Resources, Purchasing
- **Complex relationships**: Many tables with complex relationships
- **Large data**: Hundreds of thousands of records
- **Realistic**: Similar to real production databases
- **Free**: Provided by Microsoft for free

---

## AdventureWorks2022 Database Structure

### Main Schemas:
- **Sales**: SalesOrderHeader, SalesOrderDetail, Customer, Store
- **Production**: Product, ProductCategory, ProductSubcategory, Inventory
- **HumanResources**: Employee, Department, Shift
- **Purchasing**: PurchaseOrderHeader, PurchaseOrderDetail, Vendor

### Important Tables for Data Engineering:

#### Sales Schema
```
Sales.SalesOrderHeader (31,465 rows)
├── SalesOrderID (PK)
├── OrderDate
├── CustomerID
├── SalesPersonID
├── TotalDue
└── ...

Sales.SalesOrderDetail (121,317 rows)
├── SalesOrderDetailID (PK)
├── SalesOrderID (FK)
├── ProductID (FK)
├── OrderQty
├── UnitPrice
└── LineTotal

Sales.Customer (19,820 rows)
├── CustomerID (PK)
├── PersonID
└── StoreID
```

#### Production Schema
```
Production.Product (504 rows)
├── ProductID (PK)
├── Name
├── ProductNumber
├── ProductSubcategoryID (FK)
├── StandardCost
└── ListPrice

Production.ProductSubcategory (37 rows)
├── ProductSubcategoryID (PK)
├── Name
├── ProductCategoryID (FK)
└── ...

Production.ProductCategory (4 rows)
├── ProductCategoryID (PK)
├── Name
└── ...
```

---

## Applying Roadmap with AdventureWorks2022

### Stage 1: Foundation

#### Exercise 1.1: SQL Fundamentals with AdventureWorks
```sql
-- 1. Basic SELECT
SELECT TOP 10 * FROM Sales.SalesOrderHeader;

-- 2. WHERE clause
SELECT * FROM Sales.SalesOrderHeader 
WHERE OrderDate >= '2013-01-01';

-- 3. JOINs
SELECT 
    h.SalesOrderID,
    h.OrderDate,
    h.TotalDue,
    c.CustomerID
FROM Sales.SalesOrderHeader h
INNER JOIN Sales.Customer c ON h.CustomerID = c.CustomerID;

-- 4. Aggregations
SELECT 
    YEAR(OrderDate) AS Year,
    COUNT(*) AS OrderCount,
    SUM(TotalDue) AS TotalRevenue
FROM Sales.SalesOrderHeader
GROUP BY YEAR(OrderDate)
ORDER BY Year;

-- 5. Window Functions
SELECT 
    SalesOrderID,
    OrderDate,
    TotalDue,
    ROW_NUMBER() OVER (PARTITION BY YEAR(OrderDate) ORDER BY TotalDue DESC) AS Rank
FROM Sales.SalesOrderHeader;
```

**Goal**: Get familiar with database and write complex queries

---

### Stage 2: Hadoop Ecosystem

#### Exercise 2.1: Export data from SQL Server
```bash
# Export SalesOrderHeader to CSV
bcp "SELECT * FROM AdventureWorks2022.Sales.SalesOrderHeader" queryout sales_order_header.csv -c -T -S localhost

# Export Product to CSV
bcp "SELECT * FROM AdventureWorks2022.Production.Product" queryout product.csv -c -T -S localhost
```

#### Exercise 2.2: Upload to HDFS
```bash
# Create directory structure
hdfs dfs -mkdir -p /adw/raw/sales
hdfs dfs -mkdir -p /adw/raw/production

# Upload files
hdfs dfs -put sales_order_header.csv /adw/raw/sales/
hdfs dfs -put product.csv /adw/raw/production/

# Check
hdfs dfs -ls -R /adw
hdfs dfs -du -h /adw/raw/sales/
```

#### Exercise 2.3: HDFS Operations
```bash
# View file content
hdfs dfs -cat /adw/raw/sales/sales_order_header.csv | head -20

# Copy file
hdfs dfs -cp /adw/raw/sales/sales_order_header.csv /adw/backup/

# Set permissions
hdfs dfs -chmod 755 /adw/raw/sales/
hdfs dfs -chown hdfs:hdfs /adw/raw/sales/

# Check replication
hdfs dfs -ls /adw/raw/sales/sales_order_header.csv
```

**Goal**: Master HDFS commands with real data

---

### Stage 3: Spark Core (RDD)

#### Exercise 3.1: Read data from HDFS
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AdventureWorksRDD") \
    .getOrCreate()

sc = spark.sparkContext

# Read file from HDFS
sales_rdd = sc.textFile("hdfs://localhost:9000/adw/raw/sales/sales_order_header.csv")

# View number of partitions
print(f"Number of partitions: {sales_rdd.getNumPartitions()}")

# View first few lines
sales_rdd.take(5)
```

#### Exercise 3.2: Transformations with RDD
```python
# Parse CSV (skip header)
header = sales_rdd.first()
sales_data = sales_rdd.filter(lambda line: line != header)

# Map: Extract OrderDate and TotalDue
def parse_line(line):
    parts = line.split(',')
    try:
        order_date = parts[2]  # Assume OrderDate is in column 3
        total_due = float(parts[20])  # Assume TotalDue is in column 21
        return (order_date, total_due)
    except:
        return None

sales_parsed = sales_data.map(parse_line).filter(lambda x: x is not None)

# Filter: Only get orders from 2013
sales_2013 = sales_parsed.filter(lambda x: x[0].startswith('2013'))

# Reduce: Calculate total revenue
total_revenue = sales_2013.map(lambda x: x[1]).reduce(lambda a, b: a + b)
print(f"Total Revenue 2013: {total_revenue}")
```

#### Exercise 3.3: GroupBy and Aggregations
```python
# Group by Year
def extract_year(date_total):
    date, total = date_total
    year = date.split('-')[0]
    return (year, total)

sales_by_year = sales_parsed.map(extract_year) \
    .groupByKey() \
    .mapValues(lambda values: sum(values))

sales_by_year.collect()
```

#### Exercise 3.4: Join RDDs
```python
# Read Product RDD
product_rdd = sc.textFile("hdfs://localhost:9000/adw/raw/production/product.csv")
product_header = product_rdd.first()
product_data = product_rdd.filter(lambda line: line != header)

# Parse Product: (ProductID, Name)
def parse_product(line):
    parts = line.split(',')
    return (parts[0], parts[1])  # ProductID, Name

products = product_data.map(parse_product)

# Read SalesOrderDetail
detail_rdd = sc.textFile("hdfs://localhost:9000/adw/raw/sales/sales_order_detail.csv")
detail_header = detail_rdd.first()
detail_data = detail_rdd.filter(lambda line: line != header)

# Parse Detail: (ProductID, OrderQty)
def parse_detail(line):
    parts = line.split(',')
    return (parts[4], int(parts[3]))  # ProductID, OrderQty

details = detail_data.map(parse_detail)

# Join and calculate total quantity by product
product_sales = details.join(products) \
    .map(lambda x: (x[1][1], x[1][0])) \
    .reduceByKey(lambda a, b: a + b)

product_sales.take(10)
```

#### Exercise 3.5: Caching
```python
# Cache RDD used multiple times
sales_parsed.cache()

# First time: Calculate total
total1 = sales_parsed.map(lambda x: x[1]).reduce(lambda a, b: a + b)

# Second time: Calculate average (will be faster because cached)
avg = sales_parsed.map(lambda x: x[1]).mean()

# Unpersist when no longer needed
sales_parsed.unpersist()
```

**Goal**: Master RDD transformations with real data

---

### Stage 4: Spark SQL & DataFrame

#### Exercise 4.1: Read from JDBC
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AdventureWorksDF") \
    .config("spark.jars", "file:///path/to/mssql-jdbc-12.10.1.jre8.jar") \
    .getOrCreate()

# JDBC connection
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2022;encrypt=true;trustServerCertificate=true"
props = {
    "user": "your_username",
    "password": "your_password",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Read tables
order_header = spark.read.jdbc(jdbc_url, "Sales.SalesOrderHeader", properties=props)
order_detail = spark.read.jdbc(jdbc_url, "Sales.SalesOrderDetail", properties=props)
product = spark.read.jdbc(jdbc_url, "Production.Product", properties=props)
subcategory = spark.read.jdbc(jdbc_url, "Production.ProductSubcategory", properties=props)
category = spark.read.jdbc(jdbc_url, "Production.ProductCategory", properties=props)

# View schema
order_header.printSchema()
order_header.show(5)
```

#### Exercise 4.2: DataFrame Operations
```python
from pyspark.sql.functions import col, year, sum, avg, count, when

# Select columns
order_header.select("SalesOrderID", "OrderDate", "TotalDue").show(5)

# Filter
high_value_orders = order_header.filter(col("TotalDue") > 10000)
high_value_orders.show()

# Add column
from pyspark.sql.functions import year, month
orders_with_year = order_header.withColumn("Year", year("OrderDate")) \
    .withColumn("Month", month("OrderDate"))

orders_with_year.select("SalesOrderID", "OrderDate", "Year", "Month", "TotalDue").show()
```

#### Exercise 4.3: Aggregations
```python
# Sales by Year
sales_by_year = order_header \
    .withColumn("Year", year("OrderDate")) \
    .groupBy("Year") \
    .agg(
        sum("TotalDue").alias("TotalRevenue"),
        avg("TotalDue").alias("AvgOrderValue"),
        count("*").alias("OrderCount")
    ) \
    .orderBy("Year")

sales_by_year.show()

# Top 10 Customers
from pyspark.sql.functions import desc
top_customers = order_header \
    .groupBy("CustomerID") \
    .agg(
        sum("TotalDue").alias("TotalSpent"),
        count("*").alias("OrderCount")
    ) \
    .orderBy(desc("TotalSpent")) \
    .limit(10)

top_customers.show()
```

#### Exercise 4.4: Joins
```python
# Join OrderHeader with OrderDetail
order_joined = order_header.join(
    order_detail,
    order_header.SalesOrderID == order_detail.SalesOrderID,
    "inner"
)

# Join with Product to get product name
order_with_product = order_joined.join(
    product,
    order_detail.ProductID == product.ProductID,
    "left"
)

# Join with Subcategory and Category
order_full = order_with_product \
    .join(subcategory, product.ProductSubcategoryID == subcategory.ProductSubcategoryID, "left") \
    .join(category, subcategory.ProductCategoryID == category.ProductCategoryID, "left")

order_full.select(
    "SalesOrderID",
    "OrderDate",
    "Product.Name",
    "Category.Name",
    "OrderQty",
    "LineTotal"
).show(10)
```

#### Exercise 4.5: Spark SQL
```python
# Register temp views
order_header.createOrReplaceTempView("order_header")
order_detail.createOrReplaceTempView("order_detail")
product.createOrReplaceTempView("product")
subcategory.createOrReplaceTempView("subcategory")
category.createOrReplaceTempView("category")

# SQL Query: Sales by Category
sales_by_category = spark.sql("""
    SELECT 
        c.Name AS CategoryName,
        YEAR(h.OrderDate) AS Year,
        SUM(d.LineTotal) AS TotalRevenue,
        COUNT(DISTINCT h.SalesOrderID) AS OrderCount,
        AVG(d.LineTotal) AS AvgLineTotal
    FROM order_header h
    INNER JOIN order_detail d ON h.SalesOrderID = d.SalesOrderID
    INNER JOIN product p ON d.ProductID = p.ProductID
    LEFT JOIN subcategory s ON p.ProductSubcategoryID = s.ProductSubcategoryID
    LEFT JOIN category c ON s.ProductCategoryID = c.ProductCategoryID
    WHERE c.Name IS NOT NULL
    GROUP BY c.Name, YEAR(h.OrderDate)
    ORDER BY Year, TotalRevenue DESC
""")

sales_by_category.show(50)
```

#### Exercise 4.6: Window Functions
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, row_number, lag, lead

# Rank customers by total spending
windowSpec = Window.partitionBy("Year").orderBy(desc("TotalRevenue"))

customer_ranking = order_header \
    .withColumn("Year", year("OrderDate")) \
    .groupBy("Year", "CustomerID") \
    .agg(sum("TotalDue").alias("TotalRevenue")) \
    .withColumn("Rank", rank().over(windowSpec)) \
    .filter(col("Rank") <= 10)

customer_ranking.show(30)

# Month-over-Month growth
monthly_sales = order_header \
    .withColumn("Year", year("OrderDate")) \
    .withColumn("Month", month("OrderDate")) \
    .groupBy("Year", "Month") \
    .agg(sum("TotalDue").alias("MonthlyRevenue")) \
    .orderBy("Year", "Month")

windowSpec2 = Window.orderBy("Year", "Month")
monthly_with_growth = monthly_sales \
    .withColumn("PrevMonthRevenue", lag("MonthlyRevenue", 1).over(windowSpec2)) \
    .withColumn("MoMGrowth", 
        ((col("MonthlyRevenue") - col("PrevMonthRevenue")) / col("PrevMonthRevenue")) * 100
    )

monthly_with_growth.show(50)
```

#### Exercise 4.7: Write to Parquet
```python
# Write to HDFS Parquet format
order_header.write \
    .mode("overwrite") \
    .parquet("hdfs://localhost:9000/adw/raw/sales_order_header")

order_detail.write \
    .mode("overwrite") \
    .parquet("hdfs://localhost:9000/adw/raw/sales_order_detail")

product.write \
    .mode("overwrite") \
    .parquet("hdfs://localhost:9000/adw/raw/product")

# Read back from Parquet
order_header_parquet = spark.read.parquet("hdfs://localhost:9000/adw/raw/sales_order_header")
order_header_parquet.show(5)

# Compare performance
import time

# Read from JDBC
start = time.time()
order_header_jdbc = spark.read.jdbc(jdbc_url, "Sales.SalesOrderHeader", properties=props)
order_header_jdbc.count()
jdbc_time = time.time() - start

# Read from Parquet
start = time.time()
order_header_parquet = spark.read.parquet("hdfs://localhost:9000/adw/raw/sales_order_header")
order_header_parquet.count()
parquet_time = time.time() - start

print(f"JDBC time: {jdbc_time:.2f}s")
print(f"Parquet time: {parquet_time:.2f}s")
print(f"Speedup: {jdbc_time/parquet_time:.2f}x")
```

**Goal**: Master DataFrame API and Spark SQL with real data

---

### Stage 5: Advanced Spark

#### Exercise 5.1: Optimize Spark Job
```python
# Optimization configuration
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.executor.memory", "2g")
spark.conf.set("spark.executor.cores", "2")

# Repartition data
order_header_repartitioned = order_header.repartition(10, "CustomerID")

# Coalesce to reduce partitions
order_header_coalesced = order_header.coalesce(5)
```

#### Exercise 5.2: Broadcast Join
```python
# Category and Subcategory are small tables -> use broadcast
from pyspark.sql.functions import broadcast

order_with_category = order_detail \
    .join(broadcast(product), order_detail.ProductID == product.ProductID) \
    .join(broadcast(subcategory), product.ProductSubcategoryID == subcategory.ProductSubcategoryID) \
    .join(broadcast(category), subcategory.ProductCategoryID == category.ProductCategoryID)

order_with_category.explain()  # View execution plan
```

#### Exercise 5.3: Bucketing
```python
# Bucket table by CustomerID
order_header.write \
    .mode("overwrite") \
    .bucketBy(10, "CustomerID") \
    .sortBy("OrderDate") \
    .saveAsTable("order_header_bucketed")

# Read bucketed table
bucketed_df = spark.table("order_header_bucketed")
```

#### Exercise 5.4: Analyze Spark UI
```python
# Run job and analyze
sales_by_category = spark.sql("""
    SELECT 
        c.Name AS CategoryName,
        YEAR(h.OrderDate) AS Year,
        SUM(d.LineTotal) AS TotalRevenue
    FROM order_header h
    INNER JOIN order_detail d ON h.SalesOrderID = d.SalesOrderID
    INNER JOIN product p ON d.ProductID = p.ProductID
    LEFT JOIN subcategory s ON p.ProductSubcategoryID = s.ProductSubcategoryID
    LEFT JOIN category c ON s.ProductCategoryID = c.ProductCategoryID
    GROUP BY c.Name, YEAR(h.OrderDate)
""")

# View execution plan
sales_by_category.explain(extended=True)

# Collect to trigger execution
result = sales_by_category.collect()

# Then open Spark UI: http://localhost:4040
# Analyze:
# - Stages and Tasks
# - Shuffle operations
# - Execution time
```

**Goal**: Optimize Spark jobs and understand execution model

---

### Stage 6: Data Engineering Best Practices

#### Exercise 6.1: Complete ETL Pipeline
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_data(spark, jdbc_url, props):
    """Extract data from SQL Server"""
    try:
        logger.info("Starting data extraction...")
        
        order_header = spark.read.jdbc(jdbc_url, "Sales.SalesOrderHeader", properties=props)
        order_detail = spark.read.jdbc(jdbc_url, "Sales.SalesOrderDetail", properties=props)
        product = spark.read.jdbc(jdbc_url, "Production.Product", properties=props)
        subcategory = spark.read.jdbc(jdbc_url, "Production.ProductSubcategory", properties=props)
        category = spark.read.jdbc(jdbc_url, "Production.ProductCategory", properties=props)
        
        logger.info(f"Extracted {order_header.count()} orders")
        return order_header, order_detail, product, subcategory, category
        
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        raise

def transform_data(order_header, order_detail, product, subcategory, category):
    """Transform and join data"""
    try:
        logger.info("Starting data transformation...")
        
        # Join all tables
        sales_analytics = order_detail \
            .join(order_header, "SalesOrderID", "inner") \
            .join(product, order_detail.ProductID == product.ProductID, "left") \
            .join(subcategory, product.ProductSubcategoryID == subcategory.ProductSubcategoryID, "left") \
            .join(category, subcategory.ProductCategoryID == category.ProductCategoryID, "left")
        
        # Create analytics table: Sales by Category and Year
        sales_by_category_year = sales_analytics \
            .withColumn("Year", year("OrderDate")) \
            .groupBy("Year", category.Name.alias("CategoryName")) \
            .agg(
                sum("LineTotal").alias("TotalRevenue"),
                count("*").alias("OrderLineCount"),
                avg("LineTotal").alias("AvgLineTotal")
            ) \
            .filter(col("CategoryName").isNotNull()) \
            .orderBy("Year", desc("TotalRevenue"))
        
        logger.info("Data transformation completed")
        return sales_by_category_year
        
    except Exception as e:
        logger.error(f"Error transforming data: {e}")
        raise

def load_data(df, output_path):
    """Load data to HDFS"""
    try:
        logger.info(f"Loading data to {output_path}...")
        
        df.write \
            .mode("overwrite") \
            .parquet(output_path)
        
        logger.info("Data loaded successfully")
        
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

# Main ETL pipeline
def run_etl():
    spark = SparkSession.builder \
        .appName("AdventureWorksETL") \
        .config("spark.jars", "file:///path/to/mssql-jdbc-12.10.1.jre8.jar") \
        .getOrCreate()
    
    jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2022;encrypt=true;trustServerCertificate=true"
    props = {
        "user": "your_username",
        "password": "your_password",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }
    
    try:
        # Extract
        order_header, order_detail, product, subcategory, category = extract_data(spark, jdbc_url, props)
        
        # Transform
        sales_analytics = transform_data(order_header, order_detail, product, subcategory, category)
        
        # Load
        load_data(sales_analytics, "hdfs://localhost:9000/adw/analytics/sales_by_category_year")
        
        logger.info("ETL pipeline completed successfully")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    run_etl()
```

#### Exercise 6.2: Data Quality Checks
```python
def validate_data(df, table_name):
    """Validate data quality"""
    logger.info(f"Validating {table_name}...")
    
    # Check 1: Row count
    row_count = df.count()
    logger.info(f"{table_name} row count: {row_count}")
    
    if row_count == 0:
        raise ValueError(f"{table_name} is empty!")
    
    # Check 2: Null values in key columns
    null_counts = {}
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        if null_count > 0:
            null_counts[col_name] = null_count
    
    if null_counts:
        logger.warning(f"Null values found in {table_name}: {null_counts}")
    
    # Check 3: Duplicate check
    duplicate_count = df.count() - df.distinct().count()
    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count} duplicate rows in {table_name}")
    
    # Check 4: Data range validation
    if "TotalDue" in df.columns:
        negative_values = df.filter(col("TotalDue") < 0).count()
        if negative_values > 0:
            logger.warning(f"Found {negative_values} negative TotalDue values")
    
    logger.info(f"{table_name} validation completed")
    return True

# Usage
validate_data(order_header, "order_header")
validate_data(order_detail, "order_detail")
```

#### Exercise 6.3: Incremental Load
```python
def incremental_load(spark, jdbc_url, props, last_load_date):
    """Load only new/updated records"""
    
    # Read only new orders
    query = f"""
        (SELECT * FROM Sales.SalesOrderHeader 
         WHERE ModifiedDate > '{last_load_date}') AS new_orders
    """
    
    new_orders = spark.read.jdbc(
        jdbc_url,
        query,
        properties=props
    )
    
    # Read existing data
    existing_orders = spark.read.parquet("hdfs://localhost:9000/adw/raw/sales_order_header")
    
    # Merge: Union new orders with existing
    all_orders = existing_orders.union(new_orders)
    
    # Remove duplicates (keep latest)
    all_orders_dedup = all_orders \
        .orderBy("ModifiedDate", ascending=False) \
        .dropDuplicates(["SalesOrderID"])
    
    # Write back
    all_orders_dedup.write \
        .mode("overwrite") \
        .parquet("hdfs://localhost:9000/adw/raw/sales_order_header")
    
    return all_orders_dedup.count()
```

**Goal**: Build production-ready ETL pipeline

---

## Recommended Project Structure

```
SalesCategoryAnalytics/
├── scripts/
│   ├── extract_data.py          # Extract from SQL Server
│   ├── transform_data.py        # Transform data
│   ├── load_data.py             # Load to HDFS
│   ├── etl_pipeline.py          # Full ETL pipeline
│   └── data_quality.py          # Data quality checks
├── notebooks/
│   ├── 01_sql_practice.ipynb    # SQL exercises
│   ├── 02_rdd_practice.ipynb    # RDD exercises
│   ├── 03_dataframe_practice.ipynb  # DataFrame exercises
│   ├── 04_analytics.ipynb       # Analytics queries
│   └── 05_optimization.ipynb    # Performance tuning
├── data_lake/
│   └── (will be created when running pipeline)
├── logs/
│   └── (will be created when running pipeline)
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── config/
│   └── config.yaml              # Configuration file
├── ROADMAP.md                   # Learning roadmap
├── PRACTICE_GUIDE.md            # This file
└── README.md                    # Project documentation
```

---

## Practice Checklist

### Stage 1
- [ ] Write 10 complex SQL queries with AdventureWorks
- [ ] Understand relationships between tables
- [ ] Write window functions queries

### Stage 2
- [ ] Export data from SQL Server to CSV
- [ ] Upload to HDFS and create directory structure
- [ ] Practice all HDFS commands

### Stage 3
- [ ] Read data from HDFS using RDD
- [ ] Write complex transformations
- [ ] Join multiple RDDs
- [ ] Use caching

### Stage 4
- [ ] Read data from JDBC into DataFrame
- [ ] Write DataFrame operations
- [ ] Write Spark SQL queries
- [ ] Write/Read Parquet files
- [ ] Compare performance JDBC vs Parquet

### Stage 5
- [ ] Optimize Spark jobs
- [ ] Use broadcast joins
- [ ] Analyze Spark UI
- [ ] Optimize partitioning

### Stage 6
- [ ] Build complete ETL pipeline
- [ ] Implement data quality checks
- [ ] Implement incremental load
- [ ] Add logging and error handling

---

## Portfolio Projects with AdventureWorks

### Project 1: Sales Analytics Dashboard
- Extract: From SQL Server
- Transform: Join tables, calculate metrics
- Load: To HDFS Parquet
- Analytics: Sales by Category, Year, Customer
- Output: Parquet files for reporting

### Project 2: Customer Segmentation
- Analyze customer behavior
- RFM Analysis (Recency, Frequency, Monetary)
- Clustering customers
- Output: Customer segments

### Project 3: Product Performance Analysis
- Top products by revenue
- Product category analysis
- Inventory analysis
- Output: Product performance metrics

---

## Practice Tips

1. **Start simple**: From SQL queries → RDD → DataFrame
2. **Compare performance**: JDBC vs Parquet, RDD vs DataFrame
3. **Analyze Spark UI**: Understand execution plan
4. **Document code**: Comment and document every step
5. **Version control**: Commit code to GitHub
6. **Experiment**: Try different ways to solve the same problem

---

## Next Steps

1. **Start with SQL**: Get familiar with AdventureWorks database
2. **Setup HDFS**: Install and configure local HDFS
3. **Practice daily**: Do exercises every day
4. **Build project**: Build complete ETL pipeline

**Good luck learning with AdventureWorks2022!**
