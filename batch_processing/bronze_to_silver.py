"""
Bronze to Silver layer processing
Cleans text data and removes duplicates
"""
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, current_timestamp, col
from pyspark.sql.types import StringType

def clean_text(text):
    """
    Clean text by removing special characters except basic punctuation
    """
    if text is None:
        return None
    return re.sub(r'[^a-zA-Z0-9,."\']', '', str(text))

def process_bronze_to_silver(spark, table_name):
    """
    Process data from bronze to silver layer
    """
    print(f"Processing {table_name} from bronze to silver...")
    
    # Define paths
    bronze_path = f"data/bronze/{table_name}"
    silver_path = f"data/silver/{table_name}"
    
    # Read from Bronze layer
    print(f"Reading from bronze layer: {bronze_path}")
    df = spark.read.parquet(bronze_path)
    
    print(f"Loaded {df.count()} records from bronze {table_name}")
    
    # Create UDF for text cleaning
    clean_text_udf = udf(clean_text, StringType())
    
    # Apply text cleaning to all string columns
    string_columns = [field.name for field in df.schema.fields if field.dataType == StringType()]
    print(f"Cleaning text columns: {string_columns}")
    
    for col_name in string_columns:
        if col_name != "load_timestamp":  # Skip timestamp column
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))
    
    # Remove duplicates
    print("Removing duplicates...")
    initial_count = df.count()
    df = df.dropDuplicates()
    final_count = df.count()
    
    print(f"Removed {initial_count - final_count} duplicate records")
    
    # Add silver processing timestamp
    df = df.withColumn("silver_timestamp", current_timestamp())
    
    # Write to Silver layer
    print(f"Writing to silver layer: {silver_path}")
    df.write.mode("overwrite").parquet(silver_path)
    
    print(f"Successfully processed {table_name} to silver layer with {final_count} records")

def main():
    """
    Main function to process both tables
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Process both tables
        tables = ["athlete_bio", "athlete_event_results"]
        
        for table in tables:
            process_bronze_to_silver(spark, table)
            
        print("All tables processed successfully from bronze to silver!")
        
    except Exception as e:
        print(f"Error in bronze_to_silver processing: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()