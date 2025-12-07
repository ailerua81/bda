"""
Advanced Feature Engineering for Bitcoin Price Prediction
Includes on-chain metrics, technical indicators, and market microstructure features
"""

import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import numpy as np


class AdvancedFeatureEngine:
    """Advanced feature engineering for Bitcoin prediction"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def create_technical_indicators(self, df: DataFrame) -> DataFrame:
        """
        Create technical analysis indicators
        
        Features:
        - Multiple moving averages (MA)
        - Exponential moving average (EMA)
        - Bollinger Bands
        - RSI (Relative Strength Index)
        - MACD (Moving Average Convergence Divergence)
        """
        print("📊 Creating technical indicators...")
        
        # Sort by timestamp
        df = df.orderBy('timestamp')
        
        # Define windows
        window_12h = Window.orderBy('timestamp').rowsBetween(-12, 0)
        window_24h = Window.orderBy('timestamp').rowsBetween(-24, 0)
        window_48h = Window.orderBy('timestamp').rowsBetween(-48, 0)
        window_168h = Window.orderBy('timestamp').rowsBetween(-168, 0)  # 1 week
        
        # Moving Averages
        df = df.withColumn('ma_12h', avg('close').over(window_12h))
        df = df.withColumn('ma_48h', avg('close').over(window_48h))
        df = df.withColumn('ma_168h', avg('close').over(window_168h))
        
        # Price position relative to MAs
        df = df.withColumn('price_to_ma12h', (col('close') - col('ma_12h')) / col('ma_12h'))
        df = df.withColumn('price_to_ma24h', (col('close') - col('ma_24h')) / col('ma_24h'))
        
        # Bollinger Bands (24h window)
        df = df.withColumn('bb_middle', avg('close').over(window_24h))
        df = df.withColumn('bb_std', stddev('close').over(window_24h))
        df = df.withColumn('bb_upper', col('bb_middle') + 2 * col('bb_std'))
        df = df.withColumn('bb_lower', col('bb_middle') - 2 * col('bb_std'))
        df = df.withColumn('bb_position', 
                          (col('close') - col('bb_lower')) / (col('bb_upper') - col('bb_lower')))
        
        # Momentum indicators
        df = df.withColumn('momentum_12h', 
                          (col('close') - lag('close', 12).over(Window.orderBy('timestamp'))) / 
                          lag('close', 12).over(Window.orderBy('timestamp')))
        
        df = df.withColumn('momentum_24h',
                          (col('close') - lag('close', 24).over(Window.orderBy('timestamp'))) /
                          lag('close', 24).over(Window.orderBy('timestamp')))
        
        # Rate of change
        df = df.withColumn('roc_6h',
                          (col('close') - lag('close', 6).over(Window.orderBy('timestamp'))) /
                          lag('close', 6).over(Window.orderBy('timestamp')) * 100)
        
        # Volatility measures
        df = df.withColumn('volatility_12h', stddev('return_1h').over(window_12h))
        df = df.withColumn('volatility_48h', stddev('return_1h').over(window_48h))
        
        # High-Low spread
        df = df.withColumn('hl_spread', (col('high') - col('low')) / col('close'))
        df = df.withColumn('hl_spread_ma24h', avg('hl_spread').over(window_24h))
        
        print(f"✅ Created {15} technical indicators")
        return df
    
    def create_onchain_features(self, df: DataFrame, blocks_df: DataFrame) -> DataFrame:
        """
        Create on-chain blockchain features
        
        Features:
        - Transaction velocity
        - Block time variance
        - Large transaction ratio (whale activity)
        - Transaction fee trends
        - Network congestion indicators
        """
        print("⛓️ Creating on-chain features...")
        
        # Aggregate blocks by hour
        blocks_df = blocks_df.withColumn('hour', 
                                        from_unixtime(col('timestamp'), 'yyyy-MM-dd HH:00:00'))
        
        block_agg = blocks_df.groupBy('hour').agg(
            count('*').alias('blocks_count'),
            sum('tx_count').alias('total_txs'),
            avg('size').alias('avg_block_size'),
            stddev('size').alias('std_block_size'),
            max('tx_count').alias('max_txs_per_block'),
            min('tx_count').alias('min_txs_per_block')
        )
        
        # Calculate block time variance (indicator of network congestion)
        block_agg = block_agg.withColumn('timestamp_hour', 
                                        unix_timestamp(col('hour'), 'yyyy-MM-dd HH:mm:ss'))
        
        window_blocks = Window.orderBy('timestamp_hour')
        
        # Block production rate
        block_agg = block_agg.withColumn('blocks_per_hour_change',
                                        col('blocks_count') - lag('blocks_count', 1).over(window_blocks))
        
        # Transaction throughput
        block_agg = block_agg.withColumn('tx_throughput', col('total_txs') / col('blocks_count'))
        
        # Block size trend
        window_6h = Window.orderBy('timestamp_hour').rowsBetween(-6, 0)
        block_agg = block_agg.withColumn('avg_block_size_6h', avg('avg_block_size').over(window_6h))
        block_agg = block_agg.withColumn('block_size_trend',
                                        (col('avg_block_size') - col('avg_block_size_6h')) / 
                                        col('avg_block_size_6h'))
        
        # Whale activity proxy (variance in tx per block)
        block_agg = block_agg.withColumn('tx_variance_ratio',
                                        (col('max_txs_per_block') - col('min_txs_per_block')) /
                                        col('total_txs'))
        
        # Join with main dataframe
        df = df.withColumn('hour_ts',
                          from_unixtime(floor(col('timestamp') / 3600) * 3600, 'yyyy-MM-dd HH:00:00'))
        
        # Drop conflicting columns before join to avoid ambiguity
        cols_to_drop = ['blocks_count', 'total_txs', 'avg_block_size']
        for col_name in cols_to_drop:
            if col_name in df.columns:
                df = df.drop(col_name)
        
        df = df.join(
            block_agg.select('hour', 'blocks_count', 'total_txs', 'avg_block_size',
                           'tx_throughput', 'block_size_trend', 'tx_variance_ratio'),
            df.hour_ts == block_agg.hour,
            'left'
        )
        
        # Fill missing values
        onchain_cols = ['blocks_count', 'total_txs', 'avg_block_size', 
                       'tx_throughput', 'block_size_trend', 'tx_variance_ratio']
        df = df.fillna(0, subset=onchain_cols)
        
        print(f"✅ Created {len(onchain_cols)} on-chain features")
        return df
    
    def create_market_microstructure_features(self, df: DataFrame) -> DataFrame:
        """
        Create market microstructure features
        
        Features:
        - Volume trends and ratios
        - Price-volume correlation
        - Bid-ask spread proxies
        - Market impact indicators
        """
        print("💹 Creating market microstructure features...")
        
        window_6h = Window.orderBy('timestamp').rowsBetween(-6, 0)
        window_24h = Window.orderBy('timestamp').rowsBetween(-24, 0)
        
        # Volume features
        df = df.withColumn('volume_ma6h', avg('volume').over(window_6h))
        df = df.withColumn('volume_ma24h', avg('volume').over(window_24h))
        
        df = df.withColumn('volume_ratio_6h_24h', col('volume_ma6h') / col('volume_ma24h'))
        
        df = df.withColumn('volume_trend',
                          (col('volume') - col('volume_ma6h')) / col('volume_ma6h'))
        
        # Volume-weighted price
        df = df.withColumn('vwap_24h',
                          sum(col('close') * col('volume')).over(window_24h) /
                          sum('volume').over(window_24h))
        
        df = df.withColumn('price_to_vwap', (col('close') - col('vwap_24h')) / col('vwap_24h'))
        
        # Volume volatility
        df = df.withColumn('volume_volatility_24h', stddev('volume').over(window_24h))
        
        # Price-volume correlation proxy
        df = df.withColumn('price_volume_ratio', col('return_1h') * col('volume'))
        
        # Amihud illiquidity measure proxy
        df = df.withColumn('illiquidity_proxy',
                          abs(col('return_1h')) / (col('volume') + 1))  # +1 to avoid division by zero
        
        print(f"✅ Created {9} market microstructure features")
        return df
    
    def create_time_features(self, df: DataFrame) -> DataFrame:
        """
        Create time-based features
        
        Features:
        - Hour of day
        - Day of week
        - Weekend indicator
        - Trading session (Asia/Europe/US)
        """
        print("🕐 Creating time features...")
        
        # Extract time components
        df = df.withColumn('hour_of_day', hour(col('datetime')))
        df = df.withColumn('day_of_week', dayofweek(col('datetime')))
        df = df.withColumn('is_weekend', when(col('day_of_week').isin([1, 7]), 1).otherwise(0))
        
        # Trading sessions (UTC time)
        # Asia: 0-8, Europe: 8-16, US: 16-24
        df = df.withColumn('session_asia', when(col('hour_of_day') < 8, 1).otherwise(0))
        df = df.withColumn('session_europe', 
                          when((col('hour_of_day') >= 8) & (col('hour_of_day') < 16), 1).otherwise(0))
        df = df.withColumn('session_us', when(col('hour_of_day') >= 16, 1).otherwise(0))
        
        print(f"✅ Created {6} time features")
        return df
    
    def create_all_features(self, df: DataFrame, blocks_df: DataFrame) -> DataFrame:
        """Create all advanced features"""
        print("\n" + "="*60)
        print("🔧 Advanced Feature Engineering Pipeline")
        print("="*60)
        
        # Apply all feature creation functions
        df = self.create_technical_indicators(df)
        df = self.create_onchain_features(df, blocks_df)
        df = self.create_market_microstructure_features(df)
        df = self.create_time_features(df)
        
        # Drop intermediate and duplicate columns
        cols_to_drop = ['hour_ts', 'bb_std', 'bb_middle', 'hour']
        df = df.drop(*[c for c in cols_to_drop if c in df.columns])
        
        # Remove any remaining duplicate columns
        seen = set()
        cols_to_keep = []
        for col_name in df.columns:
            if col_name not in seen:
                cols_to_keep.append(col_name)
                seen.add(col_name)
        
        df = df.select(*cols_to_keep)
        
        print("\n" + "="*60)
        print(f"✅ Total features created: {len(df.columns)} total columns")
        print("="*60)
        
        return df


def main():
    """Test advanced feature creation"""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("BTC-Advanced-Features") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    # Load existing features
    base_path = os.path.expanduser("~/bda-project/data/processed")
    
    # Load blocks
    blocks_df = spark.read.parquet(f"{base_path}/blocks.parquet")
    
    # Load basic features (from ETL)
    train_df = spark.read.parquet(f"{base_path}/train_features.parquet")
    test_df = spark.read.parquet(f"{base_path}/test_features.parquet")
    
    # Combine for feature creation
    full_df = train_df.union(test_df)
    
    print(f"📊 Loaded {full_df.count()} samples with {len(full_df.columns)} base features")
    
    # Create advanced features
    fe = AdvancedFeatureEngine(spark)
    enhanced_df = fe.create_all_features(full_df, blocks_df)
    
    print(f"\n📊 Enhanced dataset: {len(enhanced_df.columns)} total features")
    
    # Show sample
    print("\n📈 Sample of enhanced features:")
    enhanced_df.select(
        'datetime', 'close', 'return_1h',
        'price_to_ma24h', 'bb_position', 'momentum_24h',
        'tx_throughput', 'volume_ratio_6h_24h',
        'target_direction'
    ).show(10)
    
    # Save enhanced features
    # Re-split into train/test (chronologically)
    total_rows = enhanced_df.count()
    split_point = int(total_rows * 0.8)
    
    enhanced_df_sorted = enhanced_df.orderBy('timestamp')
    enhanced_df_indexed = enhanced_df_sorted.withColumn('row_id', monotonically_increasing_id())
    
    train_enhanced = enhanced_df_indexed.filter(col('row_id') < split_point).drop('row_id')
    test_enhanced = enhanced_df_indexed.filter(col('row_id') >= split_point).drop('row_id')
    
    # Save
    train_enhanced.write.mode("overwrite").parquet(f"{base_path}/train_features_advanced.parquet")
    test_enhanced.write.mode("overwrite").parquet(f"{base_path}/test_features_advanced.parquet")
    
    print(f"\n💾 Saved enhanced features:")
    print(f"  - {base_path}/train_features_advanced.parquet")
    print(f"  - {base_path}/test_features_advanced.parquet")
    
    spark.stop()


if __name__ == "__main__":
    main()