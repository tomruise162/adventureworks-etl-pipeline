# Quick Start Guide

## Cài đặt nhanh

### 1. Cài đặt dependencies
```bash
pip install -r requirements.txt
```

### 2. Cấu hình database
Tạo file cấu hình từ template:
```bash
# Copy template file
cp config/config.yaml.example config/config.yaml
```

Sau đó mở file `config/config.yaml` và chỉnh sửa thông tin kết nối database của bạn:
```yaml
database:
  server: "localhost"
  port: 1433
  database_name: "AdventureWorks2022"
  username: "your_username"
  password: "your_password"
```

### 3. Kiểm tra JDBC Driver
Đảm bảo đường dẫn JDBC driver trong `config/config.yaml` đúng:
```yaml
jdbc_driver:
  path: "file:///D:/DE_project/sqljdbc_12.10/enu/jars/mssql-jdbc-12.10.1.jre8.jar"
```

### 4. Chạy pipeline hoàn chỉnh
```bash
python scripts/main.py
```

Hoặc chạy từng bước:

**Chỉ chạy ETL:**
```bash
python scripts/etl_pipeline.py
```

**Chỉ tạo reports (sau khi đã chạy ETL):**
```bash
python scripts/generate_reports.py
```

## Kết quả

Sau khi chạy xong, bạn sẽ có:

1. **Data trong HDFS** (hoặc local filesystem):
   - Raw data: `/adw/raw/`
   - Analytics data: `/adw/analytics/`

2. **Reports trong thư mục `reports/`:**
   - `sales_by_year_YYYYMMDD_HHMMSS.csv` - Doanh thu theo năm
   - `top_products_YYYYMMDD_HHMMSS.csv` - Top sản phẩm
   - `sales_by_category_YYYYMMDD_HHMMSS.csv` - Doanh thu theo category
   - `sales_report_YYYYMMDD_HHMMSS.html` - HTML report đẹp

3. **Logs trong thư mục `logs/`:**
   - `etl_pipeline.log` - Chi tiết quá trình chạy

## Xem kết quả

Mở file HTML report trong browser để xem kết quả đẹp mắt:
```bash
# Windows
start reports/sales_report_*.html

# Linux/Mac
open reports/sales_report_*.html
```

## Cấu trúc Project

```
SalesCategoryAnalytics/
├── config/
│   └── config.yaml          # Cấu hình database và Spark
├── scripts/
│   ├── main.py              # Script chính - chạy tất cả
│   ├── etl_pipeline.py      # ETL pipeline
│   ├── generate_reports.py  # Tạo reports
│   └── utils.py             # Utilities
├── reports/                 # Reports được tạo ở đây
├── logs/                    # Log files
├── data_lake/              # Data storage
└── requirements.txt        # Dependencies
```

## Troubleshooting

### Lỗi kết nối database
- Kiểm tra SQL Server đang chạy
- Kiểm tra username/password trong config.yaml
- Kiểm tra port 1433 có mở không

### Lỗi JDBC driver
- Kiểm tra đường dẫn driver trong config.yaml
- Đảm bảo file .jar tồn tại

### Lỗi HDFS
- Nếu không có HDFS, có thể sửa config để dùng local filesystem:
  ```yaml
  hdfs:
    namenode: "file:///D:/DE_project/SalesCategoryAnalytics/data_lake"
  ```

## Hỗ trợ

Xem file `README.md` để biết thêm chi tiết về project.

