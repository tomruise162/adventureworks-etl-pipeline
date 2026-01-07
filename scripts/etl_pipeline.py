"""
Complete ETL Pipeline for AdventureWorks2022 Sales Analytics
This is a production-ready example following best practices
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils import (
    load_config, 
    setup_logging, 
    create_spark_session,
    get_jdbc_properties,
    get_jdbc_url,
    get_hdfs_path
)

def extract_data(spark: SparkSession, config: dict, logger):
    """Extract data from SQL Server"""
    try:
        logger.info("=" * 50)
        logger.info("STEP 1: EXTRACT - Starting data extraction...")
        logger.info("=" * 50)
        
        jdbc_url = get_jdbc_url(config)
        props = get_jdbc_properties(config)
        
        # Extract tables
        tables_config = config.get('tables', {})
        
        dataframes = {}
        
        # Extract Sales tables
        logger.info("Extracting Sales tables...")
        for table in tables_config.get('sales', []):
            logger.info(f"  - Reading {table}...")
            df = spark.read.jdbc(jdbc_url, table, properties=props)
            table_name = table.split('.')[-1].lower()
            dataframes[table_name] = df
            logger.info(f"Loaded {df.count()} rows from {table}")
        
        # Extract Production tables
        logger.info("Extracting Production tables...")
        for table in tables_config.get('production', []):
            logger.info(f"  - Reading {table}...")
            df = spark.read.jdbc(jdbc_url, table, properties=props)
            table_name = table.split('.')[-1].lower()
            dataframes[table_name] = df
            logger.info(f"Loaded {df.count()} rows from {table}")
        
        logger.info("Data extraction completed successfully")
        return dataframes
        
    except Exception as e:
        logger.error(f"Error extracting data: {e}", exc_info=True)
        raise

def transform_data(dataframes: dict, logger):
    """Transform and join data"""
    try:
        logger.info("=" * 50)
        logger.info("STEP 2: TRANSFORM - Starting data transformation...")
        logger.info("=" * 50)
        
        # Get dataframes
        order_header = dataframes['salesorderheader']
        order_detail = dataframes['salesorderdetail']
        product = dataframes['product']
        subcategory = dataframes['productsubcategory']
        category = dataframes['productcategory']
        
        logger.info("Joining tables...")
        
        # Join all tables to create complete sales view
        sales_complete = order_detail \
            .join(order_header, "SalesOrderID", "inner") \
            .join(product, order_detail.ProductID == product.ProductID, "left") \
            .join(subcategory, product.ProductSubcategoryID == subcategory.ProductSubcategoryID, "left") \
            .join(category, subcategory.ProductCategoryID == category.ProductCategoryID, "left")
        
        logger.info(f"Joined tables: {sales_complete.count()} rows")
        
        # Create analytics table: Sales by Category and Year
        logger.info("Creating analytics: Sales by Category and Year...")
        sales_by_category_year = sales_complete \
            .withColumn("Year", year("OrderDate").cast("string")) \
            .withColumn("Month", month("OrderDate")) \
            .groupBy("Year", "Month", category.Name.alias("CategoryName")) \
            .agg(
                sum("LineTotal").alias("TotalRevenue"),
                count("*").alias("OrderLineCount"),
                avg("LineTotal").alias("AvgLineTotal"),
                sum("OrderQty").alias("TotalQuantity")
            ) \
            .filter(col("CategoryName").isNotNull()) \
            .orderBy("Year", "Month", desc("TotalRevenue"))
        
        logger.info(f"Created analytics: {sales_by_category_year.count()} rows")
        
        # Create analytics: Top Products
        logger.info("Creating analytics: Top Products...")
        top_products = sales_complete \
            .groupBy(
                product.Name.alias("ProductName"),
                category.Name.alias("CategoryName")
            ) \
            .agg(
                sum("LineTotal").alias("TotalRevenue"),
                sum("OrderQty").alias("TotalQuantity"),
                count("*").alias("OrderCount"),
                avg("UnitPrice").alias("AvgUnitPrice")
            ) \
            .filter(col("CategoryName").isNotNull()) \
            .orderBy(desc("TotalRevenue")) \
            .limit(50)
        
        logger.info(f"Created top products: {top_products.count()} rows")
        
        # Create analytics: Sales by Year Summary
        logger.info("Creating analytics: Sales by Year Summary...")
        sales_by_year = order_header \
            .withColumn("Year", year("OrderDate")) \
            .groupBy("Year") \
            .agg(
                sum("TotalDue").alias("TotalRevenue"),
                avg("TotalDue").alias("AvgOrderValue"),
                count("*").alias("OrderCount"),
                countDistinct("CustomerID").alias("UniqueCustomers")
            ) \
            .orderBy("Year")
        
        logger.info(f"Created year summary: {sales_by_year.count()} rows")
        
        analytics = {
            'sales_by_category_year': sales_by_category_year,
            'top_products': top_products,
            'sales_by_year': sales_by_year
        }
        
        logger.info("Data transformation completed successfully")
        return analytics
        
    except Exception as e:
        logger.error(f"Error transforming data: {e}", exc_info=True)
        raise

def load_data(analytics: dict, config: dict, logger, spark: SparkSession):
    """Load data to HDFS"""
    try:
        logger.info("=" * 50)
        logger.info("STEP 3: LOAD - Starting data loading...")
        logger.info("=" * 50)
        
        hdfs_base = get_hdfs_path(config, 'raw')
        hdfs_analytics = get_hdfs_path(config, 'analytics')
        
        # Load raw data
        logger.info("Loading raw data to HDFS...")
        for table_name, df in analytics.items():
            if table_name.startswith('sales_by') or table_name.startswith('top_'):
                # Analytics data
                output_path = f"{hdfs_analytics}/{table_name}"
            else:
                # Raw data
                output_path = f"{hdfs_base}/{table_name}"
            
            logger.info(f"  - Writing {table_name} to {output_path}...")
            df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            # Verify
            verify_df = spark.read.parquet(output_path)
            logger.info(f"Written {verify_df.count()} rows")
        
        logger.info("Data loading completed successfully")
        
    except Exception as e:
        logger.error(f"Error loading data: {e}", exc_info=True)
        raise

def validate_data(dataframes: dict, logger):
    """Validate data quality"""
    logger.info("=" * 50)
    logger.info("DATA QUALITY VALIDATION")
    logger.info("=" * 50)
    
    for name, df in dataframes.items():
        logger.info(f"Validating {name}...")
        
        # Check row count
        row_count = df.count()
        logger.info(f"  - Row count: {row_count:,}")
        
        if row_count == 0:
            logger.warning(f"Warning: {name} is empty!")
        
        # Check for nulls in key columns
        null_checks = {}
        for col_name in df.columns[:5]:  # Check first 5 columns
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                null_checks[col_name] = null_count
        
        if null_checks:
            logger.warning(f"Warning: Null values found: {null_checks}")
        else:
            logger.info(f"No nulls in key columns")

def run_etl_pipeline(spark=None, config=None, logger=None):
    """Run ETL pipeline with optional Spark session"""
    # Load configuration if not provided
    if config is None:
        config = load_config()
    
    # Setup logging if not provided
    if logger is None:
        logger = setup_logging(config)
    
    # Create Spark session if not provided
    spark_provided = spark is not None
    if spark is None:
        spark = create_spark_session(config)
    
    logger.info("=" * 50)
    logger.info("ADVENTUREWORKS ETL PIPELINE STARTED")
    logger.info("=" * 50)
    
    try:
        # Extract
        dataframes = extract_data(spark, config, logger)
        
        # Validate
        validate_data(dataframes, logger)
        
        # Transform
        analytics = transform_data(dataframes, logger)
        
        # Load
        load_data(analytics, config, logger, spark)
        
        logger.info("=" * 50)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        
        # Show sample results
        logger.info("\nSample Results:")
        logger.info("\nSales by Year:")
        analytics['sales_by_year'].show(10)
        
        logger.info("\nTop 10 Products:")
        analytics['top_products'].show(10)
        
        return analytics
        
    except Exception as e:
        logger.error(f"ETL PIPELINE FAILED: {e}", exc_info=True)
        raise
    finally:
        # Only stop Spark session if we created it
        if not spark_provided:
            spark.stop()
            logger.info("Spark session stopped")

def main():
    """Main ETL pipeline - standalone execution"""
    run_etl_pipeline()

if __name__ == "__main__":
    main()

