# Sales Category Analytics - Complete Data Engineering Project

Project hoàn chỉnh để load data từ SQL Server, xử lý với Apache Spark, và tạo reports.

## Tính năng

- **Extract**: Load data từ SQL Server (AdventureWorks2022)
- **Transform**: Xử lý và join data với Spark
- **Load**: Lưu data vào data lake (Parquet format)
- **Reports**: Tạo reports CSV, JSON, và HTML đẹp mắt

## Cài đặt nhanh

### 1. Cài đặt dependencies
```bash
pip install -r requirements.txt
```

### 2. Cấu hình
Tạo file cấu hình từ template:
```bash
# Copy template file
cp config/config.yaml.example config/config.yaml
```

Sau đó chỉnh sửa `config/config.yaml` với thông tin database của bạn:
```yaml
database:
  username: "your_username"
  password: "your_password"
```

### 3. Chạy pipeline
```bash
python scripts/main.py
```

## Cấu trúc Project

```
SalesCategoryAnalytics/
├── config/
│   └── config.yaml              # Cấu hình database và Spark
├── scripts/
│   ├── basic/                   # Scripts học tập (xem scripts/basic/README.md)
│   │   ├── extract_data.py     # Script cơ bản để học extract data
│   │   └── print_output.py     # Script cơ bản để test kết nối
│   ├── main.py                  # Script chính - chạy toàn bộ pipeline
│   ├── etl_pipeline.py          # ETL pipeline (Extract, Transform, Load)
│   ├── generate_reports.py     # Tạo reports từ analytics
│   └── utils.py                # Utility functions
├── reports/                     # Reports được tạo ở đây
│   ├── sales_by_year_*.csv
│   ├── top_products_*.csv
│   ├── sales_by_category_*.csv
│   └── sales_report_*.html     # HTML report đẹp
├── logs/                        # Log files
│   └── etl_pipeline.log
├── data_lake/                   # Data storage
│   └── adw/
│       ├── raw/                # Raw data
│       └── analytics/         # Processed analytics
├── requirements.txt            # Python dependencies
├── QUICK_START.md             # Hướng dẫn nhanh
└── README.md                   # File này
```

## Cách sử dụng

### Scripts Production (Khuyến nghị)

**Chạy pipeline hoàn chỉnh:**
```bash
python scripts/main.py
```

Script này sẽ:
1. Extract data từ SQL Server
2. Transform và join các bảng
3. Load vào data lake
4. Tạo reports (CSV, JSON, HTML)

**Chạy từng bước riêng lẻ:**

Chỉ chạy ETL:
```bash
python scripts/etl_pipeline.py
```

Chỉ tạo reports (sau khi đã chạy ETL):
```bash
python scripts/generate_reports.py
```

### Scripts Học Tập (Basic)

Nếu bạn muốn học từng bước cơ bản, có thể chạy các scripts trong `scripts/basic/`:

```bash
# Test kết nối và hiển thị data đơn giản
python scripts/basic/print_output.py

# Học cách extract data từ nhiều bảng
python scripts/basic/extract_data.py
```

**Lưu ý**: Các scripts basic không có error handling và config file, chỉ để học tập. Xem `scripts/basic/README.md` để biết thêm.

## Kết quả

Sau khi chạy xong, bạn sẽ có:

### 1. Data trong Data Lake
- **Raw data**: `data_lake/adw/raw/` - Dữ liệu gốc từ SQL Server
- **Analytics data**: `data_lake/adw/analytics/` - Dữ liệu đã xử lý
  - `sales_by_year` - Doanh thu theo năm
  - `top_products` - Top 50 sản phẩm
  - `sales_by_category_year` - Doanh thu theo category và năm

### 2. Reports
Tất cả reports được lưu trong thư mục `reports/`:

- **CSV Files**: 
  - `sales_by_year_YYYYMMDD_HHMMSS.csv`
  - `top_products_YYYYMMDD_HHMMSS.csv`
  - `sales_by_category_YYYYMMDD_HHMMSS.csv`

- **JSON Files**: Tương tự như CSV nhưng format JSON

- **HTML Report**: `sales_report_YYYYMMDD_HHMMSS.html`
  - Report đẹp mắt với charts và tables
  - Mở bằng browser để xem

### 3. Logs
- `logs/etl_pipeline.log` - Chi tiết quá trình chạy

## Analytics được tạo

### 1. Sales by Year
Tổng quan doanh thu theo năm:
- Total Revenue
- Average Order Value
- Order Count
- Unique Customers

### 2. Top Products
Top 50 sản phẩm bán chạy nhất:
- Product Name
- Category
- Total Revenue
- Total Quantity
- Order Count
- Average Unit Price

### 3. Sales by Category and Year
Doanh thu chi tiết theo category và năm/tháng:
- Category Name
- Year, Month
- Total Revenue
- Order Line Count
- Average Line Total
- Total Quantity

## Cấu hình

File `config/config.yaml` chứa tất cả cấu hình:

```yaml
database:
  server: "localhost"
  port: 1433
  database_name: "AdventureWorks2022"
  username: "sa"
  password: "123456"

hdfs:
  namenode: "file:///D:/DE_project/SalesCategoryAnalytics/data_lake"
  # Hoặc dùng HDFS: "hdfs://localhost:9000"

spark:
  app_name: "SalesCategoryAnalytics"
  executor_memory: "2g"
  executor_cores: "2"
```

## Yêu cầu hệ thống

- Python 3.8+
- Apache Spark 3.x
- SQL Server với AdventureWorks2022 database
- JDBC Driver cho SQL Server (đã có trong project)

## Troubleshooting

### Lỗi kết nối database
- Kiểm tra SQL Server đang chạy
- Kiểm tra username/password trong `config/config.yaml`
- Kiểm tra port 1433

### Lỗi JDBC driver
- Kiểm tra đường dẫn driver trong `config/config.yaml`
- Đảm bảo file `.jar` tồn tại

### Lỗi HDFS
- Nếu không có HDFS, project sẽ dùng local filesystem
- Kiểm tra đường dẫn trong `config/config.yaml`

## Xem kết quả

### Xem HTML Report
```bash
# Windows
start reports/sales_report_*.html

# Linux/Mac  
open reports/sales_report_*.html
```

### Xem CSV trong Excel
Mở file CSV trong Excel hoặc bất kỳ spreadsheet nào.

### Xem data trong Spark
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ViewData").getOrCreate()
df = spark.read.parquet("data_lake/adw/analytics/sales_by_year")
df.show()
```

## Hỗ trợ

Xem `QUICK_START.md` để hướng dẫn chi tiết hơn.

---

**Happy Analyzing! 📊**
