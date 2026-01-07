"""
Main script to run complete ETL Pipeline and Generate Reports
Run this script to execute the full data pipeline
"""
import sys
import os

# Add scripts directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from etl_pipeline import run_etl_pipeline
from generate_reports import generate_reports
from utils import load_config, setup_logging, create_spark_session

def main():
    """Main function to run complete pipeline"""
    # Load configuration
    config = load_config()
    
    # Setup logging
    logger = setup_logging(config)
    
    logger.info("=" * 60)
    logger.info("SALES CATEGORY ANALYTICS - COMPLETE PIPELINE")
    logger.info("=" * 60)
    
    # Create Spark session (shared between ETL and Reports)
    spark = create_spark_session(config)
    
    try:
        # Step 1: Run ETL Pipeline (pass Spark session)
        logger.info("\n" + "=" * 60)
        logger.info("PHASE 1: ETL PIPELINE")
        logger.info("=" * 60)
        run_etl_pipeline(spark=spark, config=config, logger=logger)
        
        # Step 2: Generate Reports (use same Spark session)
        logger.info("\n" + "=" * 60)
        logger.info("PHASE 2: REPORT GENERATION")
        logger.info("=" * 60)
        reports_dir = generate_reports(spark, config, logger)
        
        logger.info("\n" + "=" * 60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"Check reports in: {reports_dir}/")
        logger.info("Open HTML report in browser to view results")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()

