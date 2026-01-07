# Giải thích ETL Pipeline - etl_pipeline.py

## 🎯 Mục đích của Pipeline

Pipeline này được tạo ra để giải quyết bài toán: **Phân tích dữ liệu bán hàng từ SQL Server và tạo các báo cáo analytics**

### Vấn đề cần giải quyết:
1. **Data nằm rải rác**: Dữ liệu bán hàng nằm trong nhiều bảng khác nhau trong SQL Server
   - `SalesOrderHeader` - Thông tin đơn hàng
   - `SalesOrderDetail` - Chi tiết từng sản phẩm trong đơn hàng
   - `Product` - Thông tin sản phẩm
   - `ProductCategory`, `ProductSubcategory` - Phân loại sản phẩm

2. **Cần analytics**: Muốn biết:
   - Doanh thu theo năm như thế nào?
   - Sản phẩm nào bán chạy nhất?
   - Doanh thu theo từng category như thế nào?

3. **Performance**: SQL Server không phù hợp để chạy analytics queries phức tạp trên dữ liệu lớn

### Giải pháp:
Pipeline ETL này sẽ:
- **Extract**: Lấy dữ liệu từ SQL Server
- **Transform**: Join các bảng, tính toán metrics
- **Load**: Lưu vào Data Lake (Parquet format) để:
  - Tốc độ đọc nhanh hơn
  - Có thể query lại nhiều lần
  - Phù hợp cho analytics

---

## 📊 Flow Tổng Quan

```
┌─────────────────────────────────────────────────────────────┐
│                    ETL PIPELINE FLOW                        │
└─────────────────────────────────────────────────────────────┘

1. EXTRACT (Trích xuất)
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

2. VALIDATE (Kiểm tra chất lượng)
   ┌─────────────────────────────────────┐
   │ - Check row count                   │
   │ - Check null values                 │
   │ - Validate data integrity           │
   └──────────────┬──────────────────────┘

3. TRANSFORM (Biến đổi)
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

4. LOAD (Tải dữ liệu)
   ┌─────────────────────────────────────┐
   │ Data Lake (Parquet Format)          │
   │ ├── /adw/analytics/                 │
   │ │   ├── sales_by_year               │
   │ │   ├── top_products                │
   │ │   └── sales_by_category_year      │
   └─────────────────────────────────────┘
```

---

## 🔍 Giải thích Chi Tiết Từng Function

### 1. Function `extract_data()`

**Mục đích**: Lấy dữ liệu từ SQL Server vào Spark DataFrames

**Cách hoạt động**:

```python
def extract_data(spark: SparkSession, config: dict, logger):
```

**Input**:
- `spark`: SparkSession để kết nối Spark
- `config`: Dictionary chứa cấu hình (từ config.yaml)
- `logger`: Logger để ghi log

**Quy trình**:

1. **Lấy thông tin kết nối**:
   ```python
   jdbc_url = get_jdbc_url(config)  # "jdbc:sqlserver://localhost:1433..."
   props = get_jdbc_properties(config)  # username, password, driver
   ```

2. **Đọc danh sách bảng cần extract** từ config:
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

3. **Loop qua từng bảng và đọc**:
   ```python
   for table in tables_config.get('sales', []):
       df = spark.read.jdbc(jdbc_url, table, properties=props)
       # Đọc từ SQL Server qua JDBC
   ```

4. **Lưu vào dictionary**:
   ```python
   dataframes['salesorderheader'] = df
   dataframes['salesorderdetail'] = df
   # ...
   ```

**Output**: Dictionary chứa các Spark DataFrames

**Ví dụ kết quả**:
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

**Mục đích**: Kiểm tra chất lượng dữ liệu sau khi extract

**Cách hoạt động**:

```python
def validate_data(dataframes: dict, logger):
```

**Kiểm tra**:

1. **Row count**: Đảm bảo bảng không rỗng
   ```python
   row_count = df.count()
   if row_count == 0:
       logger.warning("Table is empty!")
   ```

2. **Null values**: Kiểm tra các cột quan trọng có null không
   ```python
   for col_name in df.columns[:5]:  # Check 5 cột đầu
       null_count = df.filter(col(col_name).isNull()).count()
   ```

**Tại sao cần validate?**
- Phát hiện sớm lỗi dữ liệu
- Đảm bảo pipeline không chạy với dữ liệu sai
- Giúp debug dễ hơn

---

### 3. Function `transform_data()`

**Mục đích**: Join các bảng và tính toán các metrics analytics

**Đây là phần quan trọng nhất!**

#### 3.1. Join các bảng

**Vấn đề**: Dữ liệu nằm rải rác trong nhiều bảng

**Giải pháp**: Join để tạo một view hoàn chỉnh

```python
sales_complete = order_detail \
    .join(order_header, "SalesOrderID", "inner") \
    .join(product, order_detail.ProductID == product.ProductID, "left") \
    .join(subcategory, product.ProductSubcategoryID == subcategory.ProductSubcategoryID, "left") \
    .join(category, subcategory.ProductCategoryID == category.ProductCategoryID, "left")
```

**Giải thích từng join**:

1. **OrderDetail JOIN OrderHeader**:
   - Mục đích: Lấy thông tin đơn hàng (OrderDate, CustomerID, TotalDue)
   - Key: `SalesOrderID`
   - Type: `inner` (chỉ lấy orders có cả header và detail)

2. **JOIN Product**:
   - Mục đích: Lấy tên sản phẩm
   - Key: `ProductID`
   - Type: `left` (giữ lại cả products không có trong orders)

3. **JOIN Subcategory**:
   - Mục đích: Lấy subcategory name
   - Key: `ProductSubcategoryID`
   - Type: `left`

4. **JOIN Category**:
   - Mục đích: Lấy category name (Bikes, Components, Clothing, Accessories)
   - Key: `ProductCategoryID`
   - Type: `left`

**Kết quả**: Một DataFrame có đầy đủ thông tin:
```
SalesOrderID | OrderDate | ProductName | CategoryName | LineTotal | ...
```

#### 3.2. Tạo Analytics Tables

Pipeline tạo 3 analytics tables:

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

**Mục đích**: Xem doanh thu theo từng category, năm, tháng

**Kết quả**:
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

**Mục đích**: Tìm top 50 sản phẩm bán chạy nhất

**Kết quả**:
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

**Mục đích**: Tổng quan doanh thu theo năm

**Kết quả**:
```
Year | TotalRevenue | AvgOrderValue | OrderCount | UniqueCustomers
2021 | 5000000.00   | 1500.00       | 3333       | 2000
2022 | 6000000.00   | 1600.00       | 3750       | 2200
```

---

### 4. Function `load_data()`

**Mục đích**: Lưu analytics data vào Data Lake (Parquet format)

**Cách hoạt động**:

```python
def load_data(analytics: dict, config: dict, logger, spark: SparkSession):
```

**Quy trình**:

1. **Lấy đường dẫn** từ config:
   ```python
   hdfs_analytics = get_hdfs_path(config, 'analytics')
   # "file:///D:/.../data_lake/adw/analytics"
   ```

2. **Ghi từng analytics table**:
   ```python
   for table_name, df in analytics.items():
       output_path = f"{hdfs_analytics}/{table_name}"
       df.write.mode("overwrite").parquet(output_path)
   ```

3. **Verify**: Đọc lại để đảm bảo ghi thành công
   ```python
   verify_df = spark.read.parquet(output_path)
   logger.info(f"Written {verify_df.count()} rows")
   ```

**Tại sao dùng Parquet?**
- **Columnar format**: Đọc nhanh hơn CSV
- **Compressed**: Tiết kiệm dung lượng
- **Schema**: Giữ được kiểu dữ liệu
- **Spark native**: Spark đọc Parquet rất nhanh

**Kết quả**: Files trong `data_lake/adw/analytics/`:
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

**Mục đích**: Orchestrate toàn bộ pipeline

**Flow**:

```python
def main():
    # 1. Setup
    config = load_config()           # Load config từ YAML
    logger = setup_logging(config)   # Setup logging
    spark = create_spark_session(config)  # Tạo Spark session
    
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
        spark.stop()  # Đóng Spark session
```

**Tại sao có try-except-finally?**
- **try**: Chạy pipeline
- **except**: Bắt lỗi và log
- **finally**: Đảm bảo Spark session luôn được đóng (dù thành công hay thất bại)

---

## 🔄 Luồng Dữ Liệu Chi Tiết

### Ví dụ với 1 đơn hàng:

**Bước 1: Extract**
```
SQL Server:
├── SalesOrderHeader: {SalesOrderID: 43659, OrderDate: 2011-05-31, TotalDue: 23153.23}
└── SalesOrderDetail: {SalesOrderDetailID: 1, SalesOrderID: 43659, ProductID: 776, LineTotal: 2024.99}
```

**Bước 2: Join**
```
sales_complete:
SalesOrderID: 43659
OrderDate: 2011-05-31
ProductID: 776
ProductName: "Mountain-200 Silver, 38"
CategoryName: "Bikes"
LineTotal: 2024.99
```

**Bước 3: Aggregate**
```
sales_by_category_year:
Year: 2011
Month: 5
CategoryName: "Bikes"
TotalRevenue: 1500000.00 (tổng tất cả Bikes trong tháng 5/2011)
```

**Bước 4: Load**
```
Parquet file: data_lake/adw/analytics/sales_by_category_year/part-00000.parquet
```

---

## 💡 Tại Sao Cần Pipeline Này?

### Vấn đề nếu không có pipeline:

1. **Query trực tiếp từ SQL Server**:
   ```sql
   SELECT c.Name, YEAR(h.OrderDate), SUM(d.LineTotal)
   FROM SalesOrderHeader h
   JOIN SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
   JOIN Product p ON d.ProductID = p.ProductID
   JOIN ProductSubcategory s ON p.ProductSubcategoryID = s.ProductSubcategoryID
   JOIN ProductCategory c ON s.ProductCategoryID = c.ProductCategoryID
   GROUP BY c.Name, YEAR(h.OrderDate)
   ```
   - Chậm với dữ liệu lớn
   - Phải query lại mỗi lần cần
   - Làm chậm SQL Server

2. **Không có data lake**:
   - Không thể lưu trữ dữ liệu đã xử lý
   - Phải tính toán lại mỗi lần

### Giải pháp với Pipeline:

1. **Extract một lần**: Lấy data từ SQL Server một lần
2. **Transform**: Tính toán metrics một lần
3. **Load**: Lưu vào data lake (Parquet)
4. **Reuse**: Có thể đọc lại nhiều lần mà không cần tính lại

**Lợi ích**:
- ⚡ Nhanh hơn: Parquet đọc nhanh hơn SQL queries
- 💰 Tiết kiệm: Không phải query SQL Server nhiều lần
- 📊 Sẵn sàng: Data đã sẵn sàng cho analytics
- 🔄 Tái sử dụng: Có thể dùng cho nhiều reports khác nhau

---

## 📝 Tóm Tắt

**Pipeline này làm gì?**
1. Lấy dữ liệu từ SQL Server
2. Join các bảng lại với nhau
3. Tính toán các metrics analytics
4. Lưu vào data lake (Parquet)

**Kết quả**:
- 3 analytics tables sẵn sàng để query
- Data được lưu trong Parquet format (nhanh, compressed)
- Có thể dùng để tạo reports, dashboards, v.v.

**Khi nào chạy?**
- Chạy định kỳ (daily/weekly) để cập nhật analytics
- Hoặc chạy một lần để tạo data lake ban đầu

---

## 🎓 Điểm Học Tập

1. **ETL Pattern**: Extract → Transform → Load
2. **Spark Joins**: Inner join, Left join
3. **Aggregations**: GroupBy, Sum, Count, Avg
4. **Data Lake**: Lưu trữ dữ liệu đã xử lý
5. **Parquet Format**: Columnar storage cho analytics
6. **Error Handling**: Try-except-finally
7. **Logging**: Ghi log để theo dõi pipeline

---

**Hy vọng giải thích này giúp bạn hiểu rõ pipeline! 🚀**

