"""
Демонстрація результатів роботи batch data lake pipeline
Цей файл показує результати обробки даних на всіх рівнях
"""
import os
from pyspark.sql import SparkSession

def show_data_structure():
    """
    Показати структуру створеного data lake
    """
    print("=" * 60)
    print("СТРУКТУРА DATA LAKE")
    print("=" * 60)
    
    if os.path.exists("data"):
        for root, dirs, files in os.walk("data"):
            level = root.replace("data", "").count(os.sep)
            indent = " " * 2 * level
            print(f"{indent}{os.path.basename(root)}/")
            subindent = " " * 2 * (level + 1)
            for file in files:
                print(f"{subindent}{file}")
    else:
        print("Директорія data не знайдена. Запустіть спочатку pipeline.")

def show_gold_results():
    """
    Демонстрація результатів з gold layer
    """
    print("=" * 60)
    print("РЕЗУЛЬТАТИ GOLD LAYER - АГРЕГОВАНІ СТАТИСТИКИ")
    print("=" * 60)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DemoGoldResults") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        gold_path = "data/gold/avg_stats"
        
        if os.path.exists(gold_path):
            # Read gold layer results
            gold_df = spark.read.parquet(gold_path)
            
            print(f"Загальна кількість записів в gold layer: {gold_df.count()}")
            print("\nСхема даних:")
            gold_df.printSchema()
            
            print("\nПриклад агрегованих даних (перші 20 записів):")
            gold_df.show(20, truncate=False)
            
            print("\nСтатистика по видам спорту:")
            gold_df.groupBy("sport").count().orderBy("count", ascending=False).show(15)
            
            print("\nСтатистика по медалям:")
            gold_df.groupBy("medal").count().show()
            
            print("\nСтатистика по країнах (топ 15):")
            gold_df.groupBy("country_noc").count().orderBy("count", ascending=False).show(15)
            
            print("\nПриклад середніх значень по спорту:")
            gold_df.select("sport", "avg_height", "avg_weight") \
                   .groupBy("sport") \
                   .avg("avg_height", "avg_weight") \
                   .orderBy("sport") \
                   .show(10)
            
        else:
            print(f"Gold layer не знайдений за шляхом: {gold_path}")
            print("Запустіть спочатку silver_to_gold.py")
            
    except Exception as e:
        print(f"Помилка при читанні gold layer: {str(e)}")
    finally:
        spark.stop()

def show_layer_comparison():
    """
    Порівняння кількості записів на різних рівнях
    """
    print("=" * 60)
    print("ПОРІВНЯННЯ РІВНІВ DATA LAKE")
    print("=" * 60)
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("LayerComparison") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        layers = ["bronze", "silver", "gold"]
        tables = ["athlete_bio", "athlete_event_results"]
        
        for layer in layers:
            print(f"\n--- {layer.upper()} LAYER ---")
            
            if layer == "gold":
                # Gold layer has different structure
                gold_path = "data/gold/avg_stats"
                if os.path.exists(gold_path):
                    df = spark.read.parquet(gold_path)
                    print(f"avg_stats: {df.count()} записів")
            else:
                for table in tables:
                    path = f"data/{layer}/{table}"
                    if os.path.exists(path):
                        df = spark.read.parquet(path)
                        print(f"{table}: {df.count()} записів")
                    else:
                        print(f"{table}: не знайдено")
                        
    except Exception as e:
        print(f"Помилка при порівнянні рівнів: {str(e)}")
    finally:
        spark.stop()

def main():
    """
    Головна функція для демонстрації результатів
    """
    print("ДЕМОНСТРАЦІЯ РЕЗУЛЬТАТІВ BATCH DATA LAKE PIPELINE")
    print("Цей скрипт показує результати роботи multi-hop data lake")
    print()
    
    try:
        # Show data structure
        show_data_structure()
        
        print("\n")
        
        # Show layer comparison
        show_layer_comparison()
        
        print("\n")
        
        # Show gold results
        show_gold_results()
        
        print("\n" + "=" * 60)
        print("ДЕМОНСТРАЦІЯ ЗАВЕРШЕНА")
        print("=" * 60)
        
    except Exception as e:
        print(f"Помилка в демонстрації: {str(e)}")

if __name__ == "__main__":
    main()