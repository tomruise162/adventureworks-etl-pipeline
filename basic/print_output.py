"""
BASIC SCRIPT - For Learning Purpose
Đây là script cơ bản để test kết nối và hiển thị data

Mục đích: Học cách kết nối SQL Server và hiển thị data đơn giản
"""
from pyspark.sql import SparkSession

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("SalesCategoryAnalytics") \
    .config("spark.jars", "file:///D:/DE_project/sqljdbc_12.10/enu/jars/mssql-jdbc-12.10.1.jre8.jar") \
    .getOrCreate()

# Kết nối JDBC
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2022;encrypt=true;trustServerCertificate=true"
props = {
    "user": "sa",
    "password": "123456",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Đọc dữ liệu
print("Loading data from Sales.SalesOrderDetail...")
order_detail = spark.read.jdbc(jdbc_url, "Sales.SalesOrderDetail", properties=props)

# Hiển thị dữ liệu
print("\nData loaded successfully!")
print(f"Total rows: {order_detail.count()}")
print("\nFirst 5 rows:")
order_detail.show(5)

print("\nSchema:")
order_detail.printSchema()

