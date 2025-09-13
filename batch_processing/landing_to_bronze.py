"""
Landing to Bronze layer processing
Downloads data from FTP server and converts CSV to Parquet format
"""
import os
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def download_data(table_name, local_file_path):
    """
    # Етап 1: Завантаження файлу з FTP-сервера в оригінальному форматі CSV
    Download data from FTP server
    """
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = url + table_name + ".csv"
    print(f"Downloading from {downloading_url}")
    print(f"# Етап 1: Завантаження файлу {table_name}.csv з FTP-сервера")
    
    response = requests.get(downloading_url)
    
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        # Open the local file in write-binary mode and write the content of the response to it
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
        print(f"File downloaded successfully and saved as {local_file_path}")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")
        sys.exit(1)

def process_landing_to_bronze(spark, table_name):
    """
    # Етап 2: Читання CSV файлу за допомогою Spark та збереження у форматі Parquet
    Process data from landing to bronze layer
    """
    print(f"Processing {table_name} from landing to bronze...")
    print(f"# Етап 2: Обробка {table_name} з landing до bronze layer")
    
    # Define paths
    landing_path = f"data/landing/{table_name}.csv"
    bronze_path = f"data/bronze/{table_name}"
    
    # Download data from FTP
    download_data(table_name, landing_path)
    
    # Read CSV file with Spark
    print(f"Reading CSV file: {landing_path}")
    print(f"# Читання CSV файлу за допомогою Spark: {landing_path}")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(landing_path)
    
    # Add processing timestamp
    df = df.withColumn("load_timestamp", current_timestamp())
    
    print(f"Loaded {df.count()} records from {table_name}")
    print("Schema:")
    df.printSchema()
    
    # Write to Bronze layer in Parquet format
    print(f"Writing to bronze layer: {bronze_path}")
    print(f"# Збереження у форматі Parquet в папку bronze/{table_name}")
    df.write.mode("overwrite").parquet(bronze_path)
    
    print(f"Successfully processed {table_name} to bronze layer")

def main():
    """
    Main function to process both tables
    """
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("LandingToBronze") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Process both tables
        tables = ["athlete_bio", "athlete_event_results"]
        
        for table in tables:
            process_landing_to_bronze(spark, table)
            
        print("All tables processed successfully from landing to bronze!")
        
    except Exception as e:
        print(f"Error in landing_to_bronze processing: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()