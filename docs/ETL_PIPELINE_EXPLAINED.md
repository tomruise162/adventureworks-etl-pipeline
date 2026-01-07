# ETL Pipeline Explanation - etl_pipeline.py

## Purpose of the Pipeline

This pipeline was created to solve the problem: **Analyze sales data from SQL Server and create analytics reports**

### Problems to Solve:
1. **Scattered data**: Sales data is spread across multiple tables in SQL Server
   - `SalesOrderHeader` - Order information
   - `SalesOrderDetail` - Details of each product in the order
   - `Product` - Product information
   - `ProductCategory`, `ProductSubcategory` - Product classification

2. **Need analytics**: Want to know:
   - How is revenue by year?
   - Which products sell best?
   - How is revenue by category?

3. **Performance**: SQL Server is not suitable for running complex analytics queries on large data

### Solution:
This ETL pipeline will:
- **Extract**: Get data from SQL Server
- **Transform**: Join tables, calculate metrics
- **Load**: Save to Data Lake (Parquet format) to:
  - Faster read speed
  - Can query multiple times
  - Suitable for analytics

---

## Overall Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    ETL PIPELINE FLOW                        │
└─────────────────────────────────────────────────────────────┘

1. EXTRACT
   ┌─────────────────────────────────────┐
   │ SQL Server Database                 │
   │ ├── SalesOrderHeader                │
   │ ├── SalesOrderDetail                │
   │ ├── Product                         │
   │ ├── ProductSubcategory              │
   │ └── ProductCategory                 │
   └──────────────┬──────────────────────┘
                  │ JDBC Connection
                  ▼
   ┌─────────────────────────────────────┐
   │ Spark DataFrames                    │
   │ (In-memory distributed data)        │
   └──────────────┬──────────────────────┘

2. VALIDATE (Data Quality Check)
   ┌─────────────────────────────────────┐
   │ - Check row count                   │
   │ - Check null values                 │
   │ - Validate data integrity           │
   └──────────────┬──────────────────────┘

3. TRANSFORM
   ┌─────────────────────────────────────┐
   │ Join Tables                         │
   │ ├── OrderDetail + OrderHeader       │
   │ ├── + Product                       │
   │ ├── + Subcategory                   │
   │ └── + Category                      │
   │                                     │
   │ Calculate Metrics                   │
   │ ├── Sales by Year                   │
   │ ├── Top Products                    │
   │ └── Sales by Category & Year        │
   └──────────────┬──────────────────────┘

4. LOAD
   ┌─────────────────────────────────────┐
   │ Data Lake (Parquet Format)          │
   │ ├── /adw/analytics/                 │
   │ │   ├── sales_by_year               │
   │ │   ├── top_products                │
   │ │   └── sales_by_category_year      │
   └─────────────────────────────────────┘
```

---

## Detailed Function Explanation

### 1. Function `extract_data()`

**Purpose**: Get data from SQL Server into Spark DataFrames

**How it works**:

```python
def extract_data(spark: SparkSession, config: dict, logger):
```

**Input**:
- `spark`: SparkSession to connect to Spark
- `config`: Dictionary containing configuration (from config.yaml)
- `logger`: Logger for logging

**Process**:

1. **Get connection information**:
   ```python
   jdbc_url = get_jdbc_url(config)  # "jdbc:sqlserver://localhost:1433..."
   props = get_jdbc_properties(config)  # username, password, driver
   ```

2. **Read list of tables to extract** from config:
   ```yaml
   tables:
     sales:
       - "Sales.SalesOrderHeader"
       - "Sales.SalesOrderDetail"
     production:
       - "Production.Product"
       - "Production.ProductSubcategory"
       - "Production.ProductCategory"
   ```

3. **Loop through each table and read**:
   ```python
   for table in tables_config.get('sales', []):
       df = spark.read.jdbc(jdbc_url, table, properties=props)
       # Read from SQL Server via JDBC
   ```

4. **Store in dictionary**:
   ```python
   dataframes['salesorderheader'] = df
   dataframes['salesorderdetail'] = df
   # ...
   ```

**Output**: Dictionary containing Spark DataFrames

**Example result**:
```python
{
    'salesorderheader': DataFrame[SalesOrderID, OrderDate, CustomerID, ...],
    'salesorderdetail': DataFrame[SalesOrderDetailID, SalesOrderID, ProductID, ...],
    'product': DataFrame[ProductID, Name, ProductSubcategoryID, ...],
    ...
}
```

---

### 2. Function `validate_data()`

**Purpose**: Check data quality after extraction

**How it works**:

```python
def validate_data(dataframes: dict, logger):
```

**Checks**:

1. **Row count**: Ensure table is not empty
   ```python
   row_count = df.count()
   if row_count == 0:
       logger.warning("Table is empty!")
   ```

2. **Null values**: Check if important columns have nulls
   ```python
   for col_name in df.columns[:5]:  # Check first 5 columns
       null_count = df.filter(col(col_name).isNull()).count()
   ```

**Why validate?**
- Detect data errors early
- Ensure pipeline doesn't run with wrong data
- Makes debugging easier

---

### 3. Function `transform_data()`

**Purpose**: Join tables and calculate analytics metrics

**This is the most important part!**

#### 3.1. Join Tables

**Problem**: Data is scattered across multiple tables

**Solution**: Join to create a complete view

```python
sales_complete = order_detail \
    .join(order_header, "SalesOrderID", "inner") \
    .join(product, order_detail.ProductID == product.ProductID, "left") \
    .join(subcategory, product.ProductSubcategoryID == subcategory.ProductSubcategoryID, "left") \
    .join(category, subcategory.ProductCategoryID == category.ProductCategoryID, "left")
```

**Explanation of each join**:

1. **OrderDetail JOIN OrderHeader**:
   - Purpose: Get order information (OrderDate, CustomerID, TotalDue)
   - Key: `SalesOrderID`
   - Type: `inner` (only get orders with both header and detail)

2. **JOIN Product**:
   - Purpose: Get product name
   - Key: `ProductID`
   - Type: `left` (keep products even if not in orders)

3. **JOIN Subcategory**:
   - Purpose: Get subcategory name
   - Key: `ProductSubcategoryID`
   - Type: `left`

4. **JOIN Category**:
   - Purpose: Get category name (Bikes, Components, Clothing, Accessories)
   - Key: `ProductCategoryID`
   - Type: `left`

**Result**: A DataFrame with complete information:
```
SalesOrderID | OrderDate | ProductName | CategoryName | LineTotal | ...
```

#### 3.2. Create Analytics Tables

Pipeline creates 3 analytics tables:

##### A. Sales by Category and Year

```python
sales_by_category_year = sales_complete \
    .withColumn("Year", year("OrderDate")) \
    .withColumn("Month", month("OrderDate")) \
    .groupBy("Year", "Month", "CategoryName") \
    .agg(
        sum("LineTotal").alias("TotalRevenue"),
        count("*").alias("OrderLineCount"),
        avg("LineTotal").alias("AvgLineTotal"),
        sum("OrderQty").alias("TotalQuantity")
    )
```

**Purpose**: View revenue by category, year, month

**Result**:
```
Year | Month | CategoryName | TotalRevenue | OrderLineCount | ...
2021 | 1     | Bikes        | 150000.00    | 500            | ...
2021 | 1     | Components   | 80000.00     | 300            | ...
```

##### B. Top Products

```python
top_products = sales_complete \
    .groupBy("ProductName", "CategoryName") \
    .agg(
        sum("LineTotal").alias("TotalRevenue"),
        sum("OrderQty").alias("TotalQuantity"),
        count("*").alias("OrderCount"),
        avg("UnitPrice").alias("AvgUnitPrice")
    ) \
    .orderBy(desc("TotalRevenue")) \
    .limit(50)
```

**Purpose**: Find top 50 best-selling products

**Result**:
```
ProductName        | CategoryName | TotalRevenue | TotalQuantity | ...
Mountain-200       | Bikes        | 500000.00    | 200           | ...
Road-250           | Bikes        | 450000.00    | 180           | ...
```

##### C. Sales by Year Summary

```python
sales_by_year = order_header \
    .withColumn("Year", year("OrderDate")) \
    .groupBy("Year") \
    .agg(
        sum("TotalDue").alias("TotalRevenue"),
        avg("TotalDue").alias("AvgOrderValue"),
        count("*").alias("OrderCount"),
        countDistinct("CustomerID").alias("UniqueCustomers")
    )
```

**Purpose**: Revenue overview by year

**Result**:
```
Year | TotalRevenue | AvgOrderValue | OrderCount | UniqueCustomers
2021 | 5000000.00   | 1500.00       | 3333       | 2000
2022 | 6000000.00   | 1600.00       | 3750       | 2200
```

---

### 4. Function `load_data()`

**Purpose**: Save analytics data to Data Lake (Parquet format)

**How it works**:

```python
def load_data(analytics: dict, config: dict, logger, spark: SparkSession):
```

**Process**:

1. **Get path** from config:
   ```python
   hdfs_analytics = get_hdfs_path(config, 'analytics')
   # "file:///D:/.../data_lake/adw/analytics"
   ```

2. **Write each analytics table**:
   ```python
   for table_name, df in analytics.items():
       output_path = f"{hdfs_analytics}/{table_name}"
       df.write.mode("overwrite").parquet(output_path)
   ```

3. **Verify**: Read back to ensure write was successful
   ```python
   verify_df = spark.read.parquet(output_path)
   logger.info(f"Written {verify_df.count()} rows")
   ```

**Why use Parquet?**
- **Columnar format**: Faster to read than CSV
- **Compressed**: Saves space
- **Schema**: Preserves data types
- **Spark native**: Spark reads Parquet very fast

**Result**: Files in `data_lake/adw/analytics/`:
```
adw/analytics/
├── sales_by_year/
│   └── part-00000-xxx.parquet
├── top_products/
│   └── part-00000-xxx.parquet
└── sales_by_category_year/
    └── part-00000-xxx.parquet
```

---

### 5. Function `main()`

**Purpose**: Orchestrate the entire pipeline

**Flow**:

```python
def main():
    # 1. Setup
    config = load_config()           # Load config from YAML
    logger = setup_logging(config)   # Setup logging
    spark = create_spark_session(config)  # Create Spark session
    
    try:
        # 2. Extract
        dataframes = extract_data(spark, config, logger)
        
        # 3. Validate
        validate_data(dataframes, logger)
        
        # 4. Transform
        analytics = transform_data(dataframes, logger)
        
        # 5. Load
        load_data(analytics, config, logger, spark)
        
        # 6. Show results
        analytics['sales_by_year'].show(10)
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()  # Close Spark session
```

**Why try-except-finally?**
- **try**: Run pipeline
- **except**: Catch errors and log
- **finally**: Ensure Spark session is always closed (whether success or failure)

---

## Detailed Data Flow

### Example with 1 order:

**Step 1: Extract**
```
SQL Server:
├── SalesOrderHeader: {SalesOrderID: 43659, OrderDate: 2011-05-31, TotalDue: 23153.23}
└── SalesOrderDetail: {SalesOrderDetailID: 1, SalesOrderID: 43659, ProductID: 776, LineTotal: 2024.99}
```

**Step 2: Join**
```
sales_complete:
SalesOrderID: 43659
OrderDate: 2011-05-31
ProductID: 776
ProductName: "Mountain-200 Silver, 38"
CategoryName: "Bikes"
LineTotal: 2024.99
```

**Step 3: Aggregate**
```
sales_by_category_year:
Year: 2011
Month: 5
CategoryName: "Bikes"
TotalRevenue: 1500000.00 (total of all Bikes in May 2011)
```

**Step 4: Load**
```
Parquet file: data_lake/adw/analytics/sales_by_category_year/part-00000.parquet
```

---

## Why Do We Need This Pipeline?

### Problems without pipeline:

1. **Query directly from SQL Server**:
   ```sql
   SELECT c.Name, YEAR(h.OrderDate), SUM(d.LineTotal)
   FROM SalesOrderHeader h
   JOIN SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
   JOIN Product p ON d.ProductID = p.ProductID
   JOIN ProductSubcategory s ON p.ProductSubcategoryID = s.ProductSubcategoryID
   JOIN ProductCategory c ON s.ProductCategoryID = c.ProductCategoryID
   GROUP BY c.Name, YEAR(h.OrderDate)
   ```
   - Slow with large data
   - Must query again each time needed
   - Slows down SQL Server

2. **No data lake**:
   - Cannot store processed data
   - Must recalculate each time

### Solution with Pipeline:

1. **Extract once**: Get data from SQL Server once
2. **Transform**: Calculate metrics once
3. **Load**: Save to data lake (Parquet)
4. **Reuse**: Can read multiple times without recalculating

**Benefits**:
- Faster: Parquet reads faster than SQL queries
- Cost-effective: Don't have to query SQL Server multiple times
- Ready: Data is ready for analytics
- Reusable: Can be used for many different reports

---

## Summary

**What does this pipeline do?**
1. Get data from SQL Server
2. Join tables together
3. Calculate analytics metrics
4. Save to data lake (Parquet)

**Result**:
- 3 analytics tables ready to query
- Data stored in Parquet format (fast, compressed)
- Can be used to create reports, dashboards, etc.

**When to run?**
- Run periodically (daily/weekly) to update analytics
- Or run once to create initial data lake

---

## Learning Points

1. **ETL Pattern**: Extract → Transform → Load
2. **Spark Joins**: Inner join, Left join
3. **Aggregations**: GroupBy, Sum, Count, Avg
4. **Data Lake**: Store processed data
5. **Parquet Format**: Columnar storage for analytics
6. **Error Handling**: Try-except-finally
7. **Logging**: Log to track pipeline

---

**Hope this explanation helps you understand the pipeline!**
