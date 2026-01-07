"""
BASIC SCRIPT - For Learning Purpose
Đây là script cơ bản để học cách extract data từ SQL Server

Mục đích: Học cách kết nối và đọc data từ SQL Server bằng Spark JDBC
"""
from pyspark.sql import SparkSession

# Tạo SparkSession với driver JDBC đúng
spark = SparkSession.builder \
    .appName("SalesCategoryAnalytics") \
    .config("spark.jars", "file:///D:/DE_project/sqljdbc_12.10/enu/jars/mssql-jdbc-12.10.1.jre8.jar") \
    .getOrCreate()

# Thông tin kết nối JDBC
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2022;encrypt=true;trustServerCertificate=true"
props = {
    "user": "sa",
    "password": "123456",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Đọc dữ liệu từ SQL Server
print("Reading data from SQL Server...")
order_detail = spark.read.jdbc(jdbc_url, "Sales.SalesOrderDetail", properties=props)
order_header = spark.read.jdbc(jdbc_url, "Sales.SalesOrderHeader", properties=props)
product = spark.read.jdbc(jdbc_url, "Production.Product", properties=props)
subcategory = spark.read.jdbc(jdbc_url, "Production.ProductSubcategory", properties=props)
category = spark.read.jdbc(jdbc_url, "Production.ProductCategory", properties=props)

print("\nData loaded successfully!")
print(f"Order Detail rows: {order_detail.count()}")
print(f"Order Header rows: {order_header.count()}")
print(f"Product rows: {product.count()}")

# Xem sample data
print("\nSample Order Detail:")
order_detail.show(5)

