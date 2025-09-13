"""
Main entry point for the Olympic Data Streaming Pipeline
"""
import sys
import os

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.streaming_pipeline import StreamingPipeline

def main():
    """Main function to run the streaming pipeline"""
    print("=" * 60)
    print("ФІНАЛЬНИЙ ПРОЄКТ - ЧАСТИНА 1")
    print("Olympic Data Streaming Pipeline")
    print("Building End-to-End Streaming Pipeline for Betting Company")
    print("=" * 60)
    print("ЕТАПИ ВИКОНАННЯ:")
    print("1. Зчитування фізичних показників атлетів з MySQL")
    print("2. Фільтрація даних (видалення порожніх значень)")
    print("3. Робота з Kafka-топіками")
    print("4. Об'єднання даних за athlete_id")
    print("5. Розрахунок середніх значень")
    print("6. Запис результатів (forEachBatch)")
    print("=" * 60)
    
    try:
        # Create and run pipeline
        pipeline = StreamingPipeline()
        pipeline.run()
        
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
    except Exception as e:
        print(f"Pipeline failed with error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()