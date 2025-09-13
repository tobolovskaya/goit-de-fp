"""
Utility functions for batch data lake pipeline
"""
import os
import requests
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, isnull

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_file_from_ftp(url, local_path):
    """
    Download file from FTP server
    
    Args:
        url (str): FTP URL to download from
        local_path (str): Local path to save the file
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"Downloading from {url}")
        response = requests.get(url, timeout=300)  # 5 minute timeout
        
        if response.status_code == 200:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            with open(local_path, 'wb') as file:
                file.write(response.content)
            
            logger.info(f"File downloaded successfully: {local_path}")
            return True
        else:
            logger.error(f"Failed to download file. Status code: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Error downloading file: {str(e)}")
        return False

def validate_dataframe(df: DataFrame, required_columns: list, table_name: str):
    """
    Validate DataFrame structure and data quality
    
    Args:
        df (DataFrame): Spark DataFrame to validate
        required_columns (list): List of required column names
        table_name (str): Name of the table for logging
    
    Returns:
        bool: True if validation passes, False otherwise
    """
    try:
        # Check if DataFrame is not empty
        if df.count() == 0:
            logger.warning(f"DataFrame {table_name} is empty")
            return False
        
        # Check required columns
        df_columns = df.columns
        missing_columns = [col for col in required_columns if col not in df_columns]
        
        if missing_columns:
            logger.error(f"Missing required columns in {table_name}: {missing_columns}")
            return False
        
        logger.info(f"DataFrame {table_name} validation passed")
        logger.info(f"Columns: {df_columns}")
        logger.info(f"Row count: {df.count()}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error validating DataFrame {table_name}: {str(e)}")
        return False

def clean_numeric_columns(df: DataFrame, numeric_columns: list):
    """
    Clean numeric columns by removing null, NaN, and invalid values
    
    Args:
        df (DataFrame): Input DataFrame
        numeric_columns (list): List of numeric column names
    
    Returns:
        DataFrame: Cleaned DataFrame
    """
    try:
        logger.info(f"Cleaning numeric columns: {numeric_columns}")
        
        for col_name in numeric_columns:
            if col_name in df.columns:
                # Filter out null, NaN, and non-positive values
                df = df.filter(
                    col(col_name).isNotNull() & 
                    ~isnan(col(col_name)) &
                    (col(col_name) > 0)
                )
        
        logger.info("Numeric columns cleaned successfully")
        return df
        
    except Exception as e:
        logger.error(f"Error cleaning numeric columns: {str(e)}")
        raise e

def log_dataframe_info(df: DataFrame, stage: str, table_name: str):
    """
    Log DataFrame information for monitoring
    
    Args:
        df (DataFrame): DataFrame to log info about
        stage (str): Processing stage (e.g., 'bronze', 'silver', 'gold')
        table_name (str): Name of the table
    """
    try:
        count = df.count()
        columns = df.columns
        
        logger.info(f"=== {stage.upper()} STAGE - {table_name.upper()} ===")
        logger.info(f"Row count: {count}")
        logger.info(f"Columns ({len(columns)}): {columns}")
        
        # Show schema
        logger.info("Schema:")
        df.printSchema()
        
        # Show sample data
        if count > 0:
            logger.info("Sample data (first 5 rows):")
            df.show(5, truncate=False)
        
    except Exception as e:
        logger.error(f"Error logging DataFrame info: {str(e)}")

def ensure_directory_exists(path: str):
    """
    Ensure directory exists, create if it doesn't
    
    Args:
        path (str): Directory path
    """
    try:
        os.makedirs(path, exist_ok=True)
        logger.info(f"Directory ensured: {path}")
    except Exception as e:
        logger.error(f"Error creating directory {path}: {str(e)}")
        raise e