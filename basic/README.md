# Basic Scripts - Scripts Học Tập

Thư mục này chứa các scripts cơ bản để học tập và thực hành.

## Mục đích

Các scripts trong thư mục này được thiết kế để:
- Học cách kết nối SQL Server với Spark
- Hiểu cách đọc data từ database
- Thực hành các thao tác cơ bản với Spark DataFrame
- Làm quen với PySpark syntax

## Các Scripts

### 1. `extract_data.py`
**Mục đích**: Học cách extract data từ SQL Server

**Chức năng**:
- Tạo SparkSession
- Kết nối SQL Server qua JDBC
- Đọc nhiều bảng từ database
- Hiển thị số lượng rows

**Cách chạy**:
```bash
python scripts/basic/extract_data.py
```

### 2. `print_output.py`
**Mục đích**: Test kết nối và hiển thị data đơn giản

**Chức năng**:
- Kết nối SQL Server
- Đọc một bảng
- Hiển thị sample data và schema

**Cách chạy**:
```bash
python scripts/basic/print_output.py
```

## Lưu ý

- Các scripts này **KHÔNG** có error handling đầy đủ
- Các scripts này **KHÔNG** sử dụng config file
- Hardcode connection strings (chỉ để học tập)
- Không có logging
- Không có data validation

## Scripts Production

Để xem các scripts production-ready với đầy đủ tính năng, xem:
- `scripts/main.py` - Script chính chạy toàn bộ pipeline
- `scripts/etl_pipeline.py` - ETL pipeline hoàn chỉnh
- `scripts/generate_reports.py` - Tạo reports

## Học Tập

Sử dụng các scripts này để:
1. Hiểu cách Spark kết nối với database
2. Học cách đọc data từ JDBC
3. Thực hành với Spark DataFrame API
4. Làm quen với PySpark

Sau khi hiểu rõ, chuyển sang sử dụng các scripts production trong thư mục `scripts/`.

