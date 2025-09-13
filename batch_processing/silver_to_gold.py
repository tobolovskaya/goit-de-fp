"""
Silver to Gold layer processing
Joins tables and calculates aggregated statistics
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col, when, isnan, isnull
from pyspark.sql.types import DoubleType, IntegerType

def process_silver_to_gold(spark):
    """
    # Етап 1: Зчитування двох таблиць із silver layer
    # Етап 2: Виконання об'єднання та деяких перетворень
    # Етап 3: Запис таблиці в папку gold
    Process data from silver to gold layer
    """
    print("Processing data from silver to gold layer...")
    print("# Обробка даних з silver до gold layer")
    
    # Define paths
    athlete_bio_path = "data/silver/athlete_bio"
    athlete_event_results_path = "data/silver/athlete_event_results"
    gold_path = "data/gold/avg_stats"
    
    # Read from Silver layer
    print(f"Reading athlete_bio from: {athlete_bio_path}")
    print("# Етап 1: Зчитування двох таблиць із silver layer")
    athlete_bio_df = spark.read.parquet(athlete_bio_path)
    
    print(f"Reading athlete_event_results from: {athlete_event_results_path}")
    athlete_event_results_df = spark.read.parquet(athlete_event_results_path)
    
    print(f"Loaded {athlete_bio_df.count()} athlete_bio records")
    print(f"Loaded {athlete_event_results_df.count()} athlete_event_results records")
    
    # Convert weight and height to numeric types
    print("Converting weight and height to numeric types...")
    athlete_bio_df = athlete_bio_df.withColumn("weight", col("weight").cast(DoubleType())) \
                                   .withColumn("height", col("height").cast(DoubleType()))
    
    # Filter out invalid weight and height values
    print("Filtering out invalid weight and height values...")
    athlete_bio_df = athlete_bio_df.filter(
        col("weight").isNotNull() & 
        col("height").isNotNull() &
        ~isnan(col("weight")) & 
        ~isnan(col("height")) &
        (col("weight") > 0) &
        (col("height") > 0)
    )
    
    print(f"After filtering: {athlete_bio_df.count()} valid athlete_bio records")
    
    # Join tables on athlete_id
    print("Joining tables on athlete_id...")
    print("# Етап 2: Виконання об'єднання таблиць за колонкою athlete_id")
    joined_df = athlete_event_results_df.join(
        athlete_bio_df,
        on="athlete_id",
        how="inner"
    )
    
    print(f"Joined dataset contains {joined_df.count()} records")
    
    # Handle null medals (athletes who didn't win medals)
    joined_df = joined_df.withColumn(
        "medal_clean",
        when(col("medal").isNull() | (col("medal") == ""), "No Medal")
        .otherwise(col("medal"))
    )
    
    # Calculate average statistics by sport, medal, sex, and country_noc
    print("Calculating average statistics...")
    print("# Розрахунок середніх значень weight і height для кожної комбінації sport, medal, sex, country_noc")
    avg_stats_df = joined_df.groupBy(
        "sport",
        col("medal_clean").alias("medal"),
        "sex",
        "country_noc"
    ).agg(
        avg("weight").alias("avg_weight"),
        avg("height").alias("avg_height")
    ).withColumn(
        "timestamp",
        current_timestamp()
    ).withColumn(
        "processing_timestamp",
        current_timestamp()
    )
    
    print("# Додавання колонки timestamp з часовою міткою виконання програми")
    
    # Round averages to 2 decimal places
    avg_stats_df = avg_stats_df.select(
        col("sport"),
        col("medal"),
        col("sex"),
        col("country_noc"),
        col("avg_weight").cast("decimal(10,2)").alias("avg_weight"),
        col("avg_height").cast("decimal(10,2)").alias("avg_height"),
        col("timestamp")
    )
    
    print(f"Generated {avg_stats_df.count()} aggregated statistics records")
    
    # Show sample data
    print("Sample of aggregated data:")
    avg_stats_df.show(10, truncate=False)
    
    # Write to Gold layer
    print(f"Writing to gold layer: {gold_path}")
    print("# Етап 3: Запис даних в gold/avg_stats")
    avg_stats_df.write.mode("overwrite").parquet(gold_path)
    
    print("Successfully processed data to gold layer!")

def main():
    """
    Main function
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        process_silver_to_gold(spark)
        print("Silver to Gold processing completed successfully!")
        
    except Exception as e:
        print(f"Error in silver_to_gold processing: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()