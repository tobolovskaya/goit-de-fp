"""
Демонстрація результатів роботи streaming pipeline
Цей файл показує, як перевірити результати в базі даних та Kafka
"""
from pyspark.sql import SparkSession
from config.database_config import DatabaseConfig, KafkaConfig
import time

def show_database_results():
    """
    Демонстрація результатів в базі даних MySQL
    """
    print("=" * 60)
    print("ДЕМОНСТРАЦІЯ РЕЗУЛЬТАТІВ В БАЗІ ДАНИХ")
    print("=" * 60)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DemoResults") \
        .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    db_config = DatabaseConfig()
    
    try:
        # Read results from MySQL table
        results_df = spark.read.format('jdbc').options(
            url=db_config.get_jdbc_url(),
            driver='com.mysql.cj.jdbc.Driver',
            dbtable='enriched_athlete_stats',
            user=db_config.MYSQL_USER,
            password=db_config.MYSQL_PASSWORD
        ).load()
        
        print(f"Загальна кількість записів в таблиці: {results_df.count()}")
        print("\nСхема таблиці:")
        results_df.printSchema()
        
        print("\nПриклад даних з бази (перші 20 записів):")
        results_df.show(20, truncate=False)
        
        print("\nСтатистика по видам спорту:")
        results_df.groupBy("sport").count().orderBy("count", ascending=False).show(10)
        
        print("\nСтатистика по медалям:")
        results_df.groupBy("medal_status").count().show()
        
        print("\nСтатистика по країнах (топ 10):")
        results_df.groupBy("country_noc").count().orderBy("count", ascending=False).show(10)
        
    except Exception as e:
        print(f"Помилка при читанні з бази даних: {str(e)}")
    finally:
        spark.stop()

def show_kafka_results():
    """
    Демонстрація результатів в Kafka топіку
    """
    print("=" * 60)
    print("ДЕМОНСТРАЦІЯ РЕЗУЛЬТАТІВ В KAFKA ТОПІКУ")
    print("=" * 60)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DemoKafkaResults") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    kafka_config = KafkaConfig()
    
    try:
        # Read from Kafka topic (batch mode for demo)
        kafka_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config.BOOTSTRAP_SERVERS) \
            .option("subscribe", kafka_config.OUTPUT_TOPIC) \
            .option("startingOffsets", "earliest") \
            .option("endingOffsets", "latest") \
            .load()
        
        print(f"Кількість повідомлень в Kafka топіку '{kafka_config.OUTPUT_TOPIC}': {kafka_df.count()}")
        
        # Parse JSON messages
        from pyspark.sql.functions import col, from_json
        from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType
        
        # Define schema for the JSON data
        json_schema = StructType([
            StructField("sport", StringType(), True),
            StructField("medal_status", StringType(), True),
            StructField("sex", StringType(), True),
            StructField("country_noc", StringType(), True),
            StructField("avg_height", DecimalType(5,2), True),
            StructField("avg_weight", DecimalType(5,2), True),
            StructField("timestamp", TimestampType(), True)
        ])
        
        parsed_df = kafka_df.select(
            col("key").cast("string"),
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value").cast("string"), json_schema).alias("data")
        ).select("key", "kafka_timestamp", "data.*")
        
        print("\nПриклад повідомлень з Kafka топіку:")
        parsed_df.show(10, truncate=False)
        
        print("\nСтатистика повідомлень по ключах (спорт):")
        parsed_df.groupBy("key").count().orderBy("count", ascending=False).show(10)
        
    except Exception as e:
        print(f"Помилка при читанні з Kafka: {str(e)}")
        print("Переконайтеся, що Kafka запущена та топік містить дані")
    finally:
        spark.stop()

def main():
    """
    Головна функція для демонстрації результатів
    """
    print("ДЕМОНСТРАЦІЯ РЕЗУЛЬТАТІВ STREAMING PIPELINE")
    print("Цей скрипт показує результати роботи pipeline в базі даних та Kafka")
    print()
    
    try:
        # Show database results
        show_database_results()
        
        print("\n" + "=" * 60)
        time.sleep(2)  # Short pause between demos
        
        # Show Kafka results
        show_kafka_results()
        
        print("\n" + "=" * 60)
        print("ДЕМОНСТРАЦІЯ ЗАВЕРШЕНА")
        print("=" * 60)
        
    except Exception as e:
        print(f"Помилка в демонстрації: {str(e)}")

if __name__ == "__main__":
    main()