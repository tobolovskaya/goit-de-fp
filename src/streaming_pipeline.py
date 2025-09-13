"""
Main streaming pipeline for Olympic data processing
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config.database_config import SparkConfig, KafkaConfig
from src.data_reader import DataReader
from src.data_processor import DataProcessor
from src.data_writer import DataWriter

class StreamingPipeline:
    """Main streaming pipeline class"""
    
    def __init__(self):
        self.spark_config = SparkConfig()
        self.kafka_config = KafkaConfig()
        self.spark = None
        self.data_reader = None
        self.data_processor = None
        self.data_writer = None
    
    def initialize_spark_session(self):
        """Initialize Spark session with required configurations"""
        print("Initializing Spark session...")
        
        self.spark = SparkSession.builder \
            .appName(self.spark_config.APP_NAME) \
            .config("spark.jars", self.spark_config.MYSQL_CONNECTOR_JAR) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Set log level to reduce noise
        self.spark.sparkContext.setLogLevel("WARN")
        
        print("Spark session initialized successfully")
        return self.spark
    
    def initialize_components(self):
        """Initialize pipeline components"""
        self.data_reader = DataReader(self.spark)
        self.data_processor = DataProcessor()
        self.data_writer = DataWriter()
    
    def setup_static_data(self):
        """Load and cache static athlete bio data"""
        print("Loading static athlete bio data...")
        
        # Read and cache athlete bio data
        athlete_bio_df = self.data_reader.read_athlete_bio_data()
        athlete_bio_df.cache()  # Cache for repeated joins
        
        return athlete_bio_df
    
    def populate_kafka_topic(self):
        """Read data from MySQL and write to Kafka topic"""
        print("Populating Kafka topic with athlete event results...")
        print("# Підготовка: Заповнення Kafka-топіку даними з MySQL для демонстрації streaming")
        
        # Read athlete event results from MySQL
        event_results_df = self.data_reader.read_athlete_event_results_from_mysql()
        
        # Write to Kafka topic
        self.data_reader.write_to_kafka_topic(
            event_results_df, 
            self.kafka_config.INPUT_TOPIC
        )
        
        print("Kafka topic populated successfully")
    
    def run_streaming_pipeline(self, athlete_bio_df):
        """Run the main streaming pipeline"""
        print("Starting streaming pipeline...")
        print("# ПОЧАТОК ВИКОНАННЯ ОСНОВНОГО STREAMING PIPELINE")
        
        # Read streaming data from Kafka
        event_stream = self.data_reader.read_from_kafka_stream(
            self.kafka_config.INPUT_TOPIC
        )
        
        # Join with athlete bio data
        enriched_stream = self.data_processor.join_athlete_data(
            event_stream, 
            athlete_bio_df
        )
        
        # Calculate statistics
        stats_stream = self.data_processor.calculate_sport_statistics(enriched_stream)
        
        # Prepare for output
        output_stream = self.data_processor.prepare_for_output(stats_stream)
        
        # Start streaming with forEachBatch
        print("Starting stream processing...")
        print("# Запуск streaming обробки з використанням forEachBatch")
        
        query = output_stream.writeStream \
            .foreachBatch(self.data_writer.foreach_batch_function) \
            .outputMode("update") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .trigger(processingTime='30 seconds') \
            .start()
        
        print("Streaming pipeline started. Waiting for termination...")
        print("# Streaming pipeline запущено. Очікування завершення...")
        print("# Для зупинки натисніть Ctrl+C")
        query.awaitTermination()
    
    def run(self):
        """Run the complete pipeline"""
        try:
            # Initialize Spark session
            self.initialize_spark_session()
            
            # Initialize components
            self.initialize_components()
            
            # Load static data
            athlete_bio_df = self.setup_static_data()
            
            # Populate Kafka topic (run once)
            self.populate_kafka_topic()
            
            # Run streaming pipeline
            self.run_streaming_pipeline(athlete_bio_df)
            
        except Exception as e:
            print(f"Pipeline error: {str(e)}")
            raise e
        finally:
            if self.spark:
                self.spark.stop()
                print("Spark session stopped")

if __name__ == "__main__":
    pipeline = StreamingPipeline()
    pipeline.run()