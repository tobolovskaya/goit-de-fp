"""
Configuration settings for batch data lake pipeline
"""
import os

class Config:
    """Configuration class for batch processing pipeline"""
    
    # FTP Server settings
    FTP_BASE_URL = "https://ftp.goit.study/neoversity/"
    
    # Data paths
    BASE_DATA_PATH = "data"
    LANDING_PATH = os.path.join(BASE_DATA_PATH, "landing")
    BRONZE_PATH = os.path.join(BASE_DATA_PATH, "bronze")
    SILVER_PATH = os.path.join(BASE_DATA_PATH, "silver")
    GOLD_PATH = os.path.join(BASE_DATA_PATH, "gold")
    
    # Table names
    TABLES = ["athlete_bio", "athlete_event_results"]
    
    # Spark configuration
    SPARK_CONFIG = {
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true"
    }
    
    # Data quality settings
    REQUIRED_COLUMNS = {
        "athlete_bio": ["athlete_id", "height", "weight", "sex"],
        "athlete_event_results": ["athlete_id", "sport", "country_noc"]
    }
    
    @classmethod
    def get_table_path(cls, layer, table_name):
        """Get path for a specific table in a specific layer"""
        layer_paths = {
            "landing": cls.LANDING_PATH,
            "bronze": cls.BRONZE_PATH,
            "silver": cls.SILVER_PATH,
            "gold": cls.GOLD_PATH
        }
        return os.path.join(layer_paths[layer], table_name)
    
    @classmethod
    def get_ftp_url(cls, table_name):
        """Get FTP URL for a specific table"""
        return f"{cls.FTP_BASE_URL}{table_name}.csv"