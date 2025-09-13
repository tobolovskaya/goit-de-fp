"""
Data processor module for transforming and enriching data
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, current_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

class DataProcessor:
    """Class for processing and transforming data"""
    
    def __init__(self):
        pass
    
    def join_athlete_data(self, event_stream_df: DataFrame, athlete_bio_df: DataFrame):
        """
        # Етап 4: Об'єднання даних з результатами змагань з біологічними даними за ключем athlete_id
        Join event results with athlete biological data
        """
        print("Joining event results with athlete bio data...")
        print("# Етап 4: Об'єднання даних з результатами змагань з Kafka-топіку з біологічними даними з MySQL таблиці за допомогою ключа athlete_id")
        
        # Perform inner join on athlete_id
        enriched_df = event_stream_df.join(
            athlete_bio_df,
            on="athlete_id",
            how="inner"
        )
        
        print("Successfully joined event and bio data")
        return enriched_df
    
    def calculate_sport_statistics(self, enriched_df: DataFrame):
        """
        # Етап 5: Розрахунок середнього зросту і ваги атлетів для кожного виду спорту, типу медалі, статі, країни
        Calculate average height and weight by sport, medal, sex, and country
        """
        print("Calculating sport statistics...")
        print("# Етап 5: Знаходження середнього зросту і ваги атлетів індивідуально для кожного виду спорту, типу медалі або її відсутності, статі, країни (country_noc)")
        
        # Handle null medals (no medal won)
        processed_df = enriched_df.withColumn(
            "medal_status",
            when(col("medal").isNull() | (col("medal") == ""), "No Medal")
            .otherwise(col("medal"))
        )
        
        # Group by sport, medal_status, sex, and country_noc
        # Calculate average height and weight
        stats_df = processed_df.groupBy(
            "sport",
            "medal_status", 
            "sex",
            "country_noc"
        ).agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight")
        ).withColumn(
            "timestamp",
            current_timestamp()
        ).withColumn(
            "processing_timestamp", 
            current_timestamp()
        )
        
        print("# Додавання timestamp, коли розрахунки були зроблені")
        
        print("Successfully calculated sport statistics")
        return stats_df
    
    def prepare_for_output(self, stats_df: DataFrame):
        """
        Prepare data for output to Kafka and database
        """
        print("Preparing data for output...")
        
        # Round averages to 2 decimal places
        output_df = stats_df.select(
            col("sport"),
            col("medal_status"),
            col("sex"),
            col("country_noc"),
            col("avg_height").cast("decimal(5,2)").alias("avg_height"),
            col("avg_weight").cast("decimal(5,2)").alias("avg_weight"),
            col("timestamp")
        )
        
        return output_df