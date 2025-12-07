"""
ETL Pipeline: Bitcoin Blocks + Market Prices
Combines blockchain data with price data for feature engineering
"""

import os
import yaml
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from datetime import datetime


class BitcoinETL:
    """ETL Pipeline for Bitcoin prediction project"""
    
    def __init__(self, config_path: str = "config/bda_project_config.yml"):
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        
    def _load_config(self, path: str) -> dict:
        """Load configuration from YAML"""
        full_path = os.path.expanduser(f"~/bda-project/{path}")
        with open(full_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with config"""
        spark_conf = self.config['spark']
        
        spark = SparkSession.builder \
            .appName(spark_conf['app_name']) \
            .master(spark_conf['master']) \
            .config("spark.executor.memory", spark_conf['executor_memory']) \
            .config("spark.driver.memory", spark_conf['driver_memory']) \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        print(f" Spark session created: {spark.version}")
        return spark
    
    def load_blocks(self) -> DataFrame:
        """Load blocks from parsed parquet"""
        blocks_path = os.path.expanduser(f"~/bda-project/data/processed/blocks.parquet")
        
        if not os.path.exists(blocks_path):
            raise FileNotFoundError(f"Blocks not found at {blocks_path}. Run parser first!")
        
        df = self.spark.read.parquet(blocks_path)
        print(f" Loaded {df.count()} blocks")
        return df
    
    def load_prices(self) -> DataFrame:
        """Load and clean price data from Kaggle CSVs"""
        prices_dir = os.path.expanduser(f"~/bda-project/{self.config['data']['prices_dir']}")
        
        print(f" Loading prices from {prices_dir}")
        
        # Read all CSVs
        df = self.spark.read.option("header", True) \
            .option("inferSchema", True) \
            .csv(f"{prices_dir}/*.csv")
        
        print(f" Original columns: {df.columns}")
        
        # Standardize column names (different datasets have different names)
        col_mapping = {
            'Timestamp': 'timestamp',
            'Date': 'timestamp',
            'date': 'timestamp',
            'Open time': 'timestamp',  # Binance format
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume_(BTC)': 'volume',
            'Volume_(Currency)': 'volume',
            'Volume': 'volume',
            'Weighted_Price': 'weighted_price'
        }
        
        for old_col, new_col in col_mapping.items():
            if old_col in df.columns:
                df = df.withColumnRenamed(old_col, new_col)
        
        # Clean timestamp column (trim whitespace if it's a string)
        if 'timestamp' in df.columns:
            df = df.withColumn('timestamp', trim(col('timestamp')))
        
        print(f" Renamed columns: {df.columns}")
        
        # Check if timestamp column exists after renaming
        if 'timestamp' not in df.columns:
            raise ValueError(f"No timestamp column found! Available columns: {df.columns}")
        
        # Convert timestamp to unix timestamp if it's a string
        timestamp_type = str(df.schema['timestamp'].dataType)
        print(f" Timestamp type: {timestamp_type}")
        
        if 'String' in timestamp_type:
            # Try different datetime formats
            # Format: 'yyyy-MM-dd HH:mm:ss.SSSSSS' (with microseconds)
            df = df.withColumn(
                'timestamp',
                coalesce(
                    unix_timestamp(col('timestamp'), 'yyyy-MM-dd HH:mm:ss.SSSSSS'),
                    unix_timestamp(col('timestamp'), 'yyyy-MM-dd HH:mm:ss'),
                    unix_timestamp(col('timestamp'))
                )
            )
        elif 'Long' in timestamp_type or 'Integer' in timestamp_type:
            # Binance timestamps are in milliseconds, convert to seconds
            # Check if values are too large (> year 2100 in seconds = 4102444800)
            df = df.withColumn(
                'timestamp',
                when(col('timestamp') > 4102444800, col('timestamp') / 1000)
                .otherwise(col('timestamp'))
            )
        
        # Keep only needed columns
        required_cols = ['timestamp', 'open', 'high', 'low', 'close']
        
        # Add volume if available
        if 'volume' in df.columns:
            required_cols.append('volume')
        else:
            print(" Volume column not found, creating dummy volume")
            df = df.withColumn('volume', lit(0.0))
            required_cols.append('volume')
        
        available_cols = [c for c in required_cols if c in df.columns]
        df = df.select(*available_cols)
        
        # Remove nulls
        df = df.dropna()
        
        # Add datetime column
        df = df.withColumn('datetime', from_unixtime(col('timestamp')))
        
        print(f" Loaded {df.count()} price records")
        return df
    
    def create_features(self, blocks_df: DataFrame, prices_df: DataFrame) -> DataFrame:
        """
        Create features for prediction
        
        Features:
        - Block features: tx_count, block_size
        - Price features: returns, volatility, moving averages
        - On-chain features: block frequency
        """
        print(" Creating features...")
        
        # === Price Features ===
        # Sort by timestamp
        prices_df = prices_df.orderBy('timestamp')
        
        # Returns
        prices_df = prices_df.withColumn(
            'return_1h',
            (col('close') - lag('close', 1).over(Window.orderBy('timestamp'))) / lag('close', 1).over(Window.orderBy('timestamp'))
        )
        
        # Moving averages
        window_24h = Window.orderBy('timestamp').rowsBetween(-24, 0)
        prices_df = prices_df.withColumn('ma_24h', avg('close').over(window_24h))
        prices_df = prices_df.withColumn('volume_24h', sum('volume').over(window_24h))
        
        # Volatility (rolling std of returns)
        prices_df = prices_df.withColumn('volatility_24h', stddev('return_1h').over(window_24h))
        
        # === Block Features (aggregated per hour) ===
        blocks_df = blocks_df.withColumn('hour', from_unixtime(col('timestamp'), 'yyyy-MM-dd HH:00:00'))
        
        block_features = blocks_df.groupBy('hour').agg(
            count('*').alias('blocks_per_hour'),
            sum('tx_count').alias('total_txs'),
            avg('size').alias('avg_block_size')
        )
        
        # Convert hour to timestamp for joining
        block_features = block_features.withColumn(
            'timestamp',
            unix_timestamp(col('hour'), 'yyyy-MM-dd HH:mm:ss')
        )
        
        # === Join prices with block features ===
        # Round price timestamps to hour
        prices_df = prices_df.withColumn(
            'hour_ts',
            from_unixtime(floor(col('timestamp') / 3600) * 3600, 'yyyy-MM-dd HH:00:00')
        )
        
        # Join
        features_df = prices_df.join(
            block_features.select('hour', 'blocks_per_hour', 'total_txs', 'avg_block_size'),
            prices_df.hour_ts == block_features.hour,
            'left'
        )
        
        # Fill missing block features with 0
        features_df = features_df.fillna(0, subset=['blocks_per_hour', 'total_txs', 'avg_block_size'])
        
        # === Target variable ===
        horizon = self.config['prediction']['horizon_minutes']
        
        # Future price (for direction prediction)
        features_df = features_df.withColumn(
            'future_close',
            lead('close', horizon).over(Window.orderBy('timestamp'))
        )
        
        # Direction: 1 if price goes up, 0 if down
        features_df = features_df.withColumn(
            'target_direction',
            when(col('future_close') > col('close'), 1).otherwise(0)
        )
        
        # Magnitude: % change
        features_df = features_df.withColumn(
            'target_magnitude',
            (col('future_close') - col('close')) / col('close') * 100
        )
        
        # Drop rows without target (last N rows)
        features_df = features_df.dropna(subset=['target_direction'])
        
        print(f" Features created: {features_df.count()} samples, {len(features_df.columns)} features")
        
        return features_df
    
    def split_data(self, df: DataFrame, train_ratio: float = 0.8):
        """Split data chronologically"""
        # Get split timestamp (80% for training)
        total_rows = df.count()
        split_point = int(total_rows * train_ratio)
        
        df_sorted = df.orderBy('timestamp')
        df_with_index = df_sorted.withColumn('row_id', monotonically_increasing_id())
        
        train_df = df_with_index.filter(col('row_id') < split_point).drop('row_id')
        test_df = df_with_index.filter(col('row_id') >= split_point).drop('row_id')
        
        print(f" Train set: {train_df.count()} samples")
        print(f" Test set: {test_df.count()} samples")
        
        return train_df, test_df
    
    def save_features(self, df: DataFrame, filename: str):
        """Save features to parquet"""
        output_path = os.path.expanduser(f"~/bda-project/data/processed/{filename}")
        df.write.mode("overwrite").parquet(output_path)
        print(f" Saved to {output_path}")
    
    def run(self):
        """Run full ETL pipeline"""
        print("="*60)
        print(" Starting ETL Pipeline")
        print("="*60)
        
        # 1. Load data
        blocks_df = self.load_blocks()
        prices_df = self.load_prices()
        
        # 2. Create features
        features_df = self.create_features(blocks_df, prices_df)
        
        # 3. Split data
        train_df, test_df = self.split_data(features_df)
        
        # 4. Save
        self.save_features(train_df, "train_features.parquet")
        self.save_features(test_df, "test_features.parquet")
        
        # 5. Show sample
        print("\n Feature columns:")
        print(features_df.columns)
        
        print("\n Sample features:")
        features_df.select(
            'datetime', 'close', 'return_1h', 'ma_24h', 
            'blocks_per_hour', 'total_txs', 'target_direction'
        ).show(10)
        
        print("\n ETL Pipeline completed!")
        
        self.spark.stop()


if __name__ == "__main__":
    etl = BitcoinETL()
    etl.run()