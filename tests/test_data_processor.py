"""
Unit tests for data processor module
"""
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from src.data_processor import DataProcessor

class TestDataProcessor(unittest.TestCase):
    """Test cases for DataProcessor class"""
    
    @classmethod
    def setUpClass(cls):
        """Set up Spark session for testing"""
        cls.spark = SparkSession.builder \
            .appName("TestDataProcessor") \
            .master("local[2]") \
            .getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")
        
        cls.processor = DataProcessor()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up Spark session"""
        cls.spark.stop()
    
    def test_join_athlete_data(self):
        """Test joining athlete event data with bio data"""
        # Create sample event data
        event_schema = StructType([
            StructField("athlete_id", IntegerType(), True),
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True)
        ])
        
        event_data = [(1, "Swimming", "Gold"), (2, "Running", None)]
        event_df = self.spark.createDataFrame(event_data, event_schema)
        
        # Create sample bio data
        bio_schema = StructType([
            StructField("athlete_id", IntegerType(), True),
            StructField("height", FloatType(), True),
            StructField("weight", FloatType(), True),
            StructField("sex", StringType(), True)
        ])
        
        bio_data = [(1, 180.0, 75.0, "M"), (2, 165.0, 60.0, "F")]
        bio_df = self.spark.createDataFrame(bio_data, bio_schema)
        
        # Test join
        result_df = self.processor.join_athlete_data(event_df, bio_df)
        
        # Verify results
        self.assertEqual(result_df.count(), 2)
        self.assertIn("height", result_df.columns)
        self.assertIn("weight", result_df.columns)
        self.assertIn("sport", result_df.columns)
    
    def test_calculate_sport_statistics(self):
        """Test calculation of sport statistics"""
        # Create sample enriched data
        schema = StructType([
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("sex", StringType(), True),
            StructField("country_noc", StringType(), True),
            StructField("height", FloatType(), True),
            StructField("weight", FloatType(), True)
        ])
        
        data = [
            ("Swimming", "Gold", "M", "USA", 180.0, 75.0),
            ("Swimming", "Gold", "M", "USA", 185.0, 80.0),
            ("Running", None, "F", "GBR", 165.0, 55.0)
        ]
        
        enriched_df = self.spark.createDataFrame(data, schema)
        
        # Test statistics calculation
        stats_df = self.processor.calculate_sport_statistics(enriched_df)
        
        # Verify results
        self.assertIn("avg_height", stats_df.columns)
        self.assertIn("avg_weight", stats_df.columns)
        self.assertIn("timestamp", stats_df.columns)
        
        # Check that we have the expected number of groups
        self.assertGreaterEqual(stats_df.count(), 1)

if __name__ == "__main__":
    unittest.main()