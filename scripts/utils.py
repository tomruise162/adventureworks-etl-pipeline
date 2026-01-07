"""
Utility functions for AdventureWorks ETL Pipeline
"""
import yaml
import logging
import os
from pathlib import Path
from pyspark.sql import SparkSession
from typing import Dict, Any

def get_project_root():
    """Get project root directory"""
    # Get directory of this file (utils.py)
    current_file = Path(__file__).resolve()
    # Go up one level from scripts/ to project root
    project_root = current_file.parent.parent
    return project_root

def load_config(config_path: str = None) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    if config_path is None:
        # Auto-detect config file from project root
        project_root = get_project_root()
        config_path = project_root / "config" / "config.yaml"
    else:
        # If relative path provided, try to resolve from project root
        if not Path(config_path).is_absolute():
            project_root = get_project_root()
            config_path = project_root / config_path
    
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}. Current working directory: {os.getcwd()}")
    
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    return config

def setup_logging(config: Dict[str, Any]) -> logging.Logger:
    """Setup logging configuration"""
    log_config = config.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO'))
    
    # Resolve log directory relative to project root
    log_dir_str = log_config.get('log_dir', './logs')
    if Path(log_dir_str).is_absolute():
        log_dir = Path(log_dir_str)
    else:
        project_root = get_project_root()
        log_dir = project_root / log_dir_str
    
    log_file = log_config.get('log_file', 'etl_pipeline.log')
    
    # Create log directory if not exists
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / log_file),
            logging.StreamHandler()
        ]
    )
    
    return logging.getLogger(__name__)

def create_spark_session(config: Dict[str, Any]) -> SparkSession:
    """Create SparkSession with configuration"""
    spark_config = config.get('spark', {})
    jdbc_config = config.get('jdbc_driver', {})
    
    spark = SparkSession.builder \
        .appName(spark_config.get('app_name', 'SalesCategoryAnalytics')) \
        .config("spark.jars", jdbc_config.get('path', '')) \
        .config("spark.executor.memory", spark_config.get('executor_memory', '2g')) \
        .config("spark.executor.cores", spark_config.get('executor_cores', '2')) \
        .config("spark.sql.shuffle.partitions", spark_config.get('shuffle_partitions', '200')) \
        .config("spark.default.parallelism", spark_config.get('default_parallelism', '200')) \
        .getOrCreate()
    
    return spark

def get_jdbc_properties(config: Dict[str, Any]) -> Dict[str, str]:
    """Get JDBC connection properties"""
    db_config = config.get('database', {})
    
    return {
        "user": db_config.get('username', 'sa'),
        "password": db_config.get('password', ''),
        "driver": db_config.get('driver', 'com.microsoft.sqlserver.jdbc.SQLServerDriver')
    }

def get_jdbc_url(config: Dict[str, Any]) -> str:
    """Get JDBC URL"""
    db_config = config.get('database', {})
    return db_config.get('jdbc_url', '')

def get_hdfs_path(config: Dict[str, Any], path_type: str = 'raw') -> str:
    """Get HDFS path based on type"""
    hdfs_config = config.get('hdfs', {})
    namenode = hdfs_config.get('namenode', 'hdfs://localhost:9000')
    
    if path_type == 'raw':
        base_path = hdfs_config.get('raw_path', '/adw/raw')
    elif path_type == 'processed':
        base_path = hdfs_config.get('processed_path', '/adw/processed')
    elif path_type == 'analytics':
        base_path = hdfs_config.get('analytics_path', '/adw/analytics')
    else:
        base_path = hdfs_config.get('base_path', '/adw')
    
    return f"{namenode}{base_path}"

