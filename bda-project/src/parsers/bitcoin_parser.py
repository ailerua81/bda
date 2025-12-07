"""
Bitcoin Block Parser for PySpark
Parses raw .dat files from Bitcoin Core into structured tables
"""

import struct
import hashlib
from typing import List, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_unixtime, explode
import glob
import os


class BitcoinBlockParser:
    """Parser for Bitcoin block files (.dat)"""
    
    # Magic bytes for mainnet
    MAGIC_MAINNET = b'\xf9\xbe\xb4\xd9'
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def parse_block_files(self, blocks_dir: str) -> DataFrame:
        """
        Parse all blk*.dat files in directory
        
        Args:
            blocks_dir: Path to blocks directory (e.g., data/blocks/blocks/)
        
        Returns:
            DataFrame with columns: block_hash, height, timestamp, tx_count, size
        """
        print(f" Parsing blocks from: {blocks_dir}")
        
        # Get all block files
        block_files = sorted(glob.glob(os.path.join(blocks_dir, "blk*.dat")))
        print(f" Found {len(block_files)} block files")
        
        if not block_files:
            raise FileNotFoundError(f"No block files found in {blocks_dir}")
        
        # Parse blocks from all files
        all_blocks = []
        for i, blk_file in enumerate(block_files[:5]):  # Limit to first 5 files for demo
            print(f" Parsing {os.path.basename(blk_file)} ({i+1}/{min(5, len(block_files))})...")
            blocks = self._parse_single_file(blk_file)
            all_blocks.extend(blocks)
            print(f"  Extracted {len(blocks)} blocks")
        
        print(f"\n Total blocks parsed: {len(all_blocks)}")
        
        # Create DataFrame
        schema = StructType([
            StructField("block_hash", StringType(), False),
            StructField("version", LongType(), False),
            StructField("prev_block", StringType(), False),
            StructField("merkle_root", StringType(), False),
            StructField("timestamp", LongType(), False),
            StructField("bits", LongType(), False),
            StructField("nonce", LongType(), False),
            StructField("tx_count", LongType(), False),
            StructField("size", LongType(), False),
        ])
        
        df = self.spark.createDataFrame(all_blocks, schema=schema)
        
        # Add readable timestamp
        df = df.withColumn("datetime", from_unixtime(col("timestamp")))
        
        return df
    
    def _parse_single_file(self, filepath: str) -> List[Dict[str, Any]]:
        """Parse a single blk*.dat file"""
        blocks = []
        
        with open(filepath, 'rb') as f:
            while True:
                # Read magic bytes
                magic = f.read(4)
                if len(magic) < 4:
                    break
                
                if magic != self.MAGIC_MAINNET:
                    # Skip to next potential magic bytes
                    continue
                
                # Read block size
                size_bytes = f.read(4)
                if len(size_bytes) < 4:
                    break
                block_size = struct.unpack('<I', size_bytes)[0]
                
                # Read block data
                block_data = f.read(block_size)
                if len(block_data) < block_size:
                    break
                
                try:
                    block_info = self._parse_block_header(block_data)
                    if block_info:
                        blocks.append(block_info)
                except Exception as e:
                    # Skip malformed blocks
                    continue
        
        return blocks
    
    def _parse_block_header(self, data: bytes) -> Dict[str, Any]:
        """Parse block header (80 bytes)"""
        if len(data) < 80:
            return None
        
        # Parse header fields (using long to handle unsigned 32-bit values)
        version = int(struct.unpack('<I', data[0:4])[0])
        prev_block = data[4:36][::-1].hex()  # Reverse for display
        merkle_root = data[36:68][::-1].hex()
        timestamp = int(struct.unpack('<I', data[68:72])[0])
        bits = int(struct.unpack('<I', data[72:76])[0])
        nonce = int(struct.unpack('<I', data[76:80])[0])
        
        # Calculate block hash (double SHA256 of header)
        header = data[0:80]
        block_hash = hashlib.sha256(hashlib.sha256(header).digest()).digest()
        block_hash = block_hash[::-1].hex()  # Reverse for display
        
        # Parse transaction count (varint after header)
        tx_count = self._read_varint(data[80:])
        
        return {
            'block_hash': block_hash,
            'version': version,
            'prev_block': prev_block,
            'merkle_root': merkle_root,
            'timestamp': timestamp,
            'bits': bits,
            'nonce': nonce,
            'tx_count': tx_count,
            'size': len(data)
        }
    
    def _read_varint(self, data: bytes) -> int:
        """Read variable-length integer"""
        if len(data) == 0:
            return 0
        
        first = data[0]
        if first < 0xfd:
            return first
        elif first == 0xfd and len(data) >= 3:
            return struct.unpack('<H', data[1:3])[0]
        elif first == 0xfe and len(data) >= 5:
            return struct.unpack('<I', data[1:5])[0]
        elif first == 0xff and len(data) >= 9:
            return struct.unpack('<Q', data[1:9])[0]
        return 0
    
    def extract_transactions_simple(self, blocks_df: DataFrame) -> DataFrame:
        """
        Create a simplified transactions table
        Note: Full tx parsing is complex, this creates estimated data
        """
        print(" Creating simplified transactions table...")
        
        # For baseline, we'll create synthetic tx data based on block info
        # In a full implementation, you'd parse actual transactions from raw data
        
        from pyspark.sql.functions import monotonically_increasing_id, rand
        
        txs_df = blocks_df.select(
            monotonically_increasing_id().alias("tx_id"),
            col("block_hash"),
            col("timestamp"),
            col("datetime"),
            (rand() * 10).cast("double").alias("amount_btc"),  # Synthetic for demo
            col("tx_count")
        )
        
        print(f" Created {txs_df.count()} transaction records")
        
        return txs_df


def main():
    """Test the parser"""
    spark = SparkSession.builder \
        .appName("BTC-Block-Parser") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    parser = BitcoinBlockParser(spark)
    
    # Parse blocks
    blocks_dir = os.path.expanduser("~/bda-project/data/blocks/blocks")
    blocks_df = parser.parse_block_files(blocks_dir)
    
    # Show results
    print("\n Blocks DataFrame Schema:")
    blocks_df.printSchema()
    
    print("\n Sample Blocks:")
    blocks_df.select("block_hash", "datetime", "tx_count", "size").show(10, truncate=False)
    
    print("\n Statistics:")
    blocks_df.describe(["tx_count", "size"]).show()
    
    # Extract transactions
    txs_df = parser.extract_transactions_simple(blocks_df)
    
    print("\n Sample Transactions:")
    txs_df.show(10)
    
    # Save to parquet
    output_dir = os.path.expanduser("~/bda-project/data/processed")
    os.makedirs(output_dir, exist_ok=True)
    
    blocks_df.write.mode("overwrite").parquet(f"{output_dir}/blocks.parquet")
    txs_df.write.mode("overwrite").parquet(f"{output_dir}/transactions.parquet")
    
    print(f"\n Saved to {output_dir}/")
    
    spark.stop()


if __name__ == "__main__":
    main()