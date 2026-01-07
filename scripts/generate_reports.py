"""
Generate Reports from Analytics Data
Exports reports to CSV, JSON, and HTML formats
"""
import json
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils import load_config, get_hdfs_path, setup_logging, create_spark_session, get_project_root

def export_to_csv(df, output_path, logger):
    """Export DataFrame to CSV"""
    try:
        logger.info(f"Exporting to CSV: {output_path}")
        df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(output_path)
        logger.info(f"CSV exported successfully")
    except Exception as e:
        logger.error(f"Error exporting CSV: {e}")
        raise

def export_to_json(df, output_path, logger):
    """Export DataFrame to JSON"""
    try:
        logger.info(f"Exporting to JSON: {output_path}")
        df.coalesce(1).write \
            .mode("overwrite") \
            .json(output_path)
        logger.info(f"JSON exported successfully")
    except Exception as e:
        logger.error(f"Error exporting JSON: {e}")
        raise

def generate_html_report(analytics_data, output_path, logger):
    """Generate HTML report"""
    try:
        logger.info(f"Generating HTML report: {output_path}")
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Sales Analytics Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #555;
            margin-top: 30px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            font-weight: bold;
        }}
        tr:nth-child(even) {{
            background-color: #f2f2f2;
        }}
        tr:hover {{
            background-color: #e8f5e9;
        }}
        .summary {{
            background-color: #e3f2fd;
            padding: 15px;
            border-radius: 5px;
            margin: 20px 0;
        }}
        .metric {{
            display: inline-block;
            margin: 10px 20px;
            padding: 10px;
            background-color: white;
            border-radius: 5px;
            min-width: 150px;
        }}
        .metric-label {{
            font-size: 12px;
            color: #666;
        }}
        .metric-value {{
            font-size: 24px;
            font-weight: bold;
            color: #2196F3;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Sales Analytics Report</h1>
        <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        
        <div class="summary">
            <h2>Executive Summary</h2>
            <div class="metric">
                <div class="metric-label">Total Revenue</div>
                <div class="metric-value">${analytics_data.get('total_revenue', 0):,.2f}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Total Orders</div>
                <div class="metric-value">{analytics_data.get('total_orders', 0):,}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Unique Customers</div>
                <div class="metric-value">{analytics_data.get('unique_customers', 0):,}</div>
            </div>
            <div class="metric">
                <div class="metric-label">Average Order Value</div>
                <div class="metric-value">${analytics_data.get('avg_order_value', 0):,.2f}</div>
            </div>
        </div>
        
        <h2>Sales by Year</h2>
        {analytics_data.get('sales_by_year_html', '')}
        
        <h2>Top 20 Products by Revenue</h2>
        {analytics_data.get('top_products_html', '')}
        
        <h2>Sales by Category and Year</h2>
        {analytics_data.get('sales_by_category_html', '')}
    </div>
</body>
</html>
"""
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated successfully")
        
    except Exception as e:
        logger.error(f"Error generating HTML report: {e}")
        raise

def main():
    """Main function to generate reports"""
    config = load_config()
    logger = setup_logging(config)
    spark = create_spark_session(config)
    
    try:
        generate_reports(spark, config, logger)
    except Exception as e:
        logger.error(f"Report generation failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

def dataframe_to_html_table(df, limit=20):
    """Convert DataFrame to HTML table"""
    try:
        rows = df.limit(limit).collect()
        if not rows:
            return "<p>No data available</p>"
        
        # Get column names
        columns = df.columns
        
        # Build HTML table
        html = "<table>\n<thead>\n<tr>"
        for col in columns:
            html += f"<th>{col}</th>"
        html += "</tr>\n</thead>\n<tbody>\n"
        
        for row in rows:
            html += "<tr>"
            for col in columns:
                value = row[col]
                if value is None:
                    value = ""
                elif isinstance(value, float):
                    value = f"${value:,.2f}" if 'revenue' in col.lower() or 'total' in col.lower() or 'price' in col.lower() else f"{value:,.2f}"
                elif isinstance(value, int) and col.lower() != "year":  # Exclude formatting for 'Year'
                    value = f"{value:,}"
                html += f"<td>{value}</td>"
            html += "</tr>\n"
        
        html += "</tbody>\n</table>"
        return html
    except Exception as e:
        return f"<p>Error generating table: {e}</p>"

def generate_reports(spark: SparkSession, config: dict, logger):
    """Generate all reports from analytics data"""
    try:
        logger.info("=" * 50)
        logger.info("GENERATING REPORTS")
        logger.info("=" * 50)
        
        # Create reports directory relative to project root
        project_root = get_project_root()
        reports_dir = project_root / "reports"
        os.makedirs(reports_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Read analytics data from HDFS
        hdfs_analytics = get_hdfs_path(config, 'analytics')
        
        logger.info("Reading analytics data...")
        sales_by_year = spark.read.parquet(f"{hdfs_analytics}/sales_by_year")
        top_products = spark.read.parquet(f"{hdfs_analytics}/top_products")
        sales_by_category_year = spark.read.parquet(f"{hdfs_analytics}/sales_by_category_year")
        
        # Calculate summary metrics
        total_revenue = sales_by_year.agg(sum("TotalRevenue").alias("total")).collect()[0]["total"]
        total_orders = sales_by_year.agg(sum("OrderCount").alias("total")).collect()[0]["total"]
        unique_customers = sales_by_year.agg(max("UniqueCustomers").alias("total")).collect()[0]["total"]
        avg_order_value = sales_by_year.agg(avg("AvgOrderValue").alias("avg")).collect()[0]["avg"]
        
        analytics_data = {
            'total_revenue': float(total_revenue) if total_revenue else 0,
            'total_orders': int(total_orders) if total_orders else 0,
            'unique_customers': int(unique_customers) if unique_customers else 0,
            'avg_order_value': float(avg_order_value) if avg_order_value else 0,
            'sales_by_year_html': dataframe_to_html_table(sales_by_year, limit=10),
            'top_products_html': dataframe_to_html_table(top_products, limit=20),
            'sales_by_category_html': dataframe_to_html_table(sales_by_category_year, limit=30)
        }
        
        # Export CSV reports
        logger.info("Exporting CSV reports...")
        export_to_csv(sales_by_year, str(reports_dir / f"sales_by_year_{timestamp}.csv"), logger)
        export_to_csv(top_products, str(reports_dir / f"top_products_{timestamp}.csv"), logger)
        export_to_csv(sales_by_category_year, str(reports_dir / f"sales_by_category_{timestamp}.csv"), logger)
        
        # Export JSON reports
        logger.info("Exporting JSON reports...")
        export_to_json(sales_by_year, str(reports_dir / f"sales_by_year_{timestamp}.json"), logger)
        export_to_json(top_products, str(reports_dir / f"top_products_{timestamp}.json"), logger)
        export_to_json(sales_by_category_year, str(reports_dir / f"sales_by_category_{timestamp}.json"), logger)
        
        # Generate HTML report
        logger.info("Generating HTML report...")
        html_path = str(reports_dir / f"sales_report_{timestamp}.html")
        generate_html_report(analytics_data, html_path, logger)
        
        logger.info("=" * 50)
        logger.info("REPORTS GENERATED SUCCESSFULLY")
        logger.info("=" * 50)
        logger.info(f"Reports saved to: {reports_dir}")
        logger.info(f"HTML Report: {html_path}")
        
        # Print summary
        logger.info("\nSummary Metrics:")
        logger.info(f"  Total Revenue: ${analytics_data['total_revenue']:,.2f}")
        logger.info(f"  Total Orders: {analytics_data['total_orders']:,}")
        logger.info(f"  Unique Customers: {analytics_data['unique_customers']:,}")
        logger.info(f"  Average Order Value: ${analytics_data['avg_order_value']:,.2f}")
        
        return reports_dir
        
    except Exception as e:
        logger.error(f"Error generating reports: {e}", exc_info=True)
        raise

