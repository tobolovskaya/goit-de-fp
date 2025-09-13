"""
Data reader module for reading data from MySQL and Kafka
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from config.database_config import DatabaseConfig, KafkaConfig, SparkConfig

class DataReader:
    """Class for reading data from various sources"""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.db_config = DatabaseConfig()
        self.kafka_config = KafkaConfig()
    
    def read_athlete_bio_data(self):
        """
        Read athlete biological data from MySQL table
        Returns filtered DataFrame with valid height and weight data
        """
        print("Reading athlete bio data from MySQL...")
        
        # Read data from MySQL
        athlete_bio_df = self.spark.read.format('jdbc').options(
            url=self.db_config.get_jdbc_url(),
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='athlete_bio',
            user=self.db_config.MYSQL_USER,
            password=self.db_config.MYSQL_PASSWORD
        ).load()
        
        # Filter out records with empty or non-numeric height and weight
        filtered_df = athlete_bio_df.filter(
            col("height").isNotNull() & 
            col("weight").isNotNull() &
            ~isnan(col("height")) & 
            ~isnan(col("weight")) &
            (col("height") > 0) &
            (col("weight") > 0)
        )
        
        print(f"Loaded {athlete_bio_df.count()} total records")
        print(f"After filtering: {filtered_df.count()} valid records")
        
        return filtered_df
    
    def read_athlete_event_results_from_mysql(self):
        """
        Read athlete event results from MySQL table
        """
        print("Reading athlete event results from MySQL...")
        
        event_results_df = self.spark.read.format('jdbc').options(
            url=self.db_config.get_jdbc_url(),
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='athlete_event_results',
            user=self.db_config.MYSQL_USER,
            password=self.db_config.MYSQL_PASSWORD
        ).load()
        
        print(f"Loaded {event_results_df.count()} event results from MySQL")
        return event_results_df
    
    def write_to_kafka_topic(self, df, topic_name):
        """
        Write DataFrame to Kafka topic
        """
        print(f"Writing data to Kafka topic: {topic_name}")
        
        # Convert DataFrame to JSON format for Kafka
        kafka_df = df.select(
            col("athlete_id").cast("string").alias("key"),
            to_json(struct([col(c) for c in df.columns])).alias("value")
        )
        
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config.BOOTSTRAP_SERVERS) \
            .option("topic", topic_name) \
            .save()
        
        print(f"Successfully wrote data to Kafka topic: {topic_name}")
    
    def read_from_kafka_stream(self, topic_name):
        """
        Read streaming data from Kafka topic
        """
        print(f"Setting up Kafka stream reader for topic: {topic_name}")
        
        # Define schema for athlete event results
        event_results_schema = StructType([
            StructField("athlete_id", IntegerType(), True),
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("country_noc", StringType(), True),
            StructField("event", StringType(), True),
            StructField("year", IntegerType(), True)
        ])
        
        # Read from Kafka stream
        kafka_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config.BOOTSTRAP_SERVERS) \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data from Kafka
        parsed_stream = kafka_stream.select(
            col("key").cast("string"),
            from_json(col("value").cast("string"), event_results_schema).alias("data")
        ).select("data.*")
        
        return parsed_stream