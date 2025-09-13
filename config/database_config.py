"""
Database configuration module for Olympic data streaming pipeline
"""
import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseConfig:
    """MySQL database configuration"""
    
    MYSQL_HOST = os.getenv('MYSQL_HOST', '217.61.57.46')
    MYSQL_PORT = os.getenv('MYSQL_PORT', '3306')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE', 'olympic_dataset')
    MYSQL_USER = os.getenv('MYSQL_USER', 'username')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', 'password')
    
    @classmethod
    def get_jdbc_url(cls):
        """Get JDBC URL for MySQL connection"""
        return f"jdbc:mysql://{cls.MYSQL_HOST}:{cls.MYSQL_PORT}/{cls.MYSQL_DATABASE}"
    
    @classmethod
    def get_jdbc_properties(cls):
        """Get JDBC connection properties"""
        return {
            "user": cls.MYSQL_USER,
            "password": cls.MYSQL_PASSWORD,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

class KafkaConfig:
    """Kafka configuration"""
    
    BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    INPUT_TOPIC = os.getenv('KAFKA_INPUT_TOPIC', 'athlete_event_results')
    OUTPUT_TOPIC = os.getenv('KAFKA_OUTPUT_TOPIC', 'enriched_athlete_data')

class SparkConfig:
    """Spark configuration"""
    
    APP_NAME = os.getenv('SPARK_APP_NAME', 'OlympicDataStreamingPipeline')
    MYSQL_CONNECTOR_JAR = "mysql-connector-j-8.0.32.jar"