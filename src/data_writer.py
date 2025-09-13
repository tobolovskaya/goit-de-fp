"""
Data writer module for writing data to Kafka and MySQL
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_json, struct
from config.database_config import DatabaseConfig, KafkaConfig

class DataWriter:
    """Class for writing data to various destinations"""
    
    def __init__(self):
        self.db_config = DatabaseConfig()
        self.kafka_config = KafkaConfig()
    
    def write_to_kafka(self, df: DataFrame, topic_name: str):
        """
        # Етап 6.а): Запис у вихідний Kafka-топік
        Write DataFrame to Kafka topic
        """
        print(f"Writing batch to Kafka topic: {topic_name}")
        print(f"# Етап 6.а): Запис збагачених даних у вихідний Kafka-топік {topic_name}")
        
        # Convert DataFrame to JSON format for Kafka
        kafka_df = df.select(
            col("sport").alias("key"),  # Use sport as key for partitioning
            to_json(struct([col(c) for c in df.columns])).alias("value")
        )
        
        kafka_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config.BOOTSTRAP_SERVERS) \
            .option("topic", topic_name) \
            .mode("append") \
            .save()
        
        print(f"Successfully wrote batch to Kafka topic: {topic_name}")
    
    def write_to_mysql(self, df: DataFrame, table_name: str):
        """
        # Етап 6.b): Запис у базу даних MySQL
        Write DataFrame to MySQL table
        """
        print(f"Writing batch to MySQL table: {table_name}")
        print(f"# Етап 6.b): Запис збагачених даних у базу даних MySQL, таблиця {table_name}")
        
        df.write \
            .format("jdbc") \
            .option("url", self.db_config.get_jdbc_url()) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", table_name) \
            .option("user", self.db_config.MYSQL_USER) \
            .option("password", self.db_config.MYSQL_PASSWORD) \
            .mode("append") \
            .save()
        
        print(f"Successfully wrote batch to MySQL table: {table_name}")
    
    def foreach_batch_function(self, batch_df: DataFrame, batch_id: int):
        """
        # Використання функції forEachBatch для стріму даних
        Function to process each batch - writes to both Kafka and MySQL
        """
        print(f"Processing batch {batch_id} with {batch_df.count()} records")
        print(f"# Використання функції forEachBatch для обробки мікробатчу {batch_id}")
        
        # Показати приклад даних для демонстрації
        print("=== ДЕМОНСТРАЦІЯ РЕЗУЛЬТАТІВ ===")
        print("Приклад оброблених даних:")
        batch_df.show(10, truncate=False)
        
        try:
            # Етап 6.а): Запис у вихідний Kafka-топік
            self.write_to_kafka(batch_df, self.kafka_config.OUTPUT_TOPIC)
            
            # Етап 6.b): Запис у базу даних
            self.write_to_mysql(batch_df, "enriched_athlete_stats")
            
            print(f"Successfully processed batch {batch_id}")
            print("=== КІНЕЦЬ ДЕМОНСТРАЦІЇ РЕЗУЛЬТАТІВ ===")
            
        except Exception as e:
            print(f"Error processing batch {batch_id}: {str(e)}")
            raise e