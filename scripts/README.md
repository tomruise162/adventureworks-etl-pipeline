# Scripts - Production Ready

Thư mục này chứa các scripts production-ready cho project.

## Cấu trúc

```
scripts/
├── main.py                   # Script chính - chạy toàn bộ pipeline
├── etl_pipeline.py           # ETL pipeline hoàn chỉnh
├── generate_reports.py       # Tạo reports (CSV, JSON, HTML)
└── utils.py                  # Utility functions
```

## Scripts Production

### 1. `main.py` - Script Chính
**Chức năng**: Chạy toàn bộ pipeline từ đầu đến cuối

**Bao gồm**:
- Extract data từ SQL Server
- Transform và join data
- Load vào data lake
- Generate reports

**Cách chạy**:
```bash
python scripts/main.py
```

### 2. `etl_pipeline.py` - ETL Pipeline
**Chức năng**: Extract, Transform, Load data

**Tính năng**:
- Extract từ SQL Server qua JDBC (Java Database Connectivity)
- Transform: Join các bảng, tính toán metrics
- Load vào data lake (Parquet format)
- Data quality validation
- Logging đầy đủ
- Error handling

**Cách chạy**:
```bash
python scripts/etl_pipeline.py
```

### 3. `generate_reports.py` - Tạo Reports
**Chức năng**: Tạo reports từ analytics data

**Tính năng**:
- Export CSV reports
- Export JSON reports
- Generate HTML report đẹp mắt
- Summary metrics

**Cách chạy**:
```bash
python scripts/generate_reports.py
```

**Lưu ý**: Cần chạy `etl_pipeline.py` trước để có data

### 4. `utils.py` - Utilities
**Chức năng**: Các hàm tiện ích

**Bao gồm**:
- `load_config()` - Load config từ YAML
- `setup_logging()` - Setup logging
- `create_spark_session()` - Tạo SparkSession với config
- `get_jdbc_properties()` - Lấy JDBC properties
- `get_hdfs_path()` - Lấy HDFS paths


## Workflow
### Chạy Pipeline Hoàn Chỉnh
```bash
python scripts/main.py
```

### Chạy Từng Bước
```bash
# Bước 1: ETL
python scripts/etl_pipeline.py

# Bước 2: Generate Reports
python scripts/generate_reports.py
```

## Cấu hình

Tất cả cấu hình trong `config/config.yaml`:
- Database connection
- Spark settings
- HDFS paths
- Logging config

## Output

Sau khi chạy, bạn sẽ có:
- **Data**: `data_lake/adw/` (raw + analytics)
- **Reports**: `reports/` (CSV, JSON, HTML)
- **Logs**: `logs/etl_pipeline.log`

## Troubleshooting

Xem `QUICK_START.md` hoặc `README.md` ở thư mục gốc để biết thêm chi tiết.

