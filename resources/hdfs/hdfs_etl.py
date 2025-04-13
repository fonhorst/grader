#!/usr/bin/env python3
import os
import time
import logging
import argparse
import pandas as pd
from hdfs import InsecureClient
from typing import List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class HDFSETLPipeline:
    def __init__(self, hdfs_url: str, input_dir: str, output_dir: str, check_interval: int = 60):
        """
        Initialize the HDFS ETL pipeline.
        
        Args:
            hdfs_url: HDFS WebHDFS URL
            input_dir: Input directory path in HDFS
            output_dir: Output directory path in HDFS
            check_interval: Interval in seconds to check for new files
        """
        self.client = InsecureClient(hdfs_url)
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.check_interval = check_interval
        self.processed_files = set()
        
        # Create output directory if it doesn't exist
        if not self.client.status(self.output_dir, strict=False):
            self.client.makedirs(self.output_dir)
            logger.info(f"Created output directory: {self.output_dir}")

    def get_new_files(self) -> List[str]:
        """Get list of new files in the input directory."""
        try:
            files = self.client.list(self.input_dir)
            new_files = [f for f in files if f not in self.processed_files]
            return new_files
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []

    def read_file(self, file_path: str) -> pd.DataFrame:
        """Read a CSV file from HDFS into a pandas DataFrame."""
        try:
            with self.client.read(f"{self.input_dir}/{file_path}") as reader:
                df = pd.read_csv(reader)
                logger.info(f"Successfully read file: {file_path}")
                return df
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {e}")
            return pd.DataFrame()

    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the data according to requirements."""
        try:
            # Filter successful transactions
            df = df[df['status'] == 'success'].copy()
            
            # Convert amounts to USD
            df['amount_usd'] = df.apply(
                lambda row: row['amount'] if row['currency'] == 'USD' else row['amount'] * 1.1,
                axis=1
            )
            
            # Extract date from timestamp
            df['date'] = pd.to_datetime(df['timestamp']).dt.date
            
            # Aggregate data
            result = df.groupby('date').agg({
                'amount_usd': ['sum', 'count', 'mean']
            }).reset_index()
            
            # Rename columns
            result.columns = ['date', 'total_amount', 'transaction_count', 'average_amount']
            
            logger.info("Successfully processed data")
            return result
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            return pd.DataFrame()

    def write_output(self, df: pd.DataFrame, date: str):
        """Write processed data to HDFS."""
        try:
            output_path = f"{self.output_dir}/{date}/aggregated_data.csv"
            
            # Create date directory if it doesn't exist
            date_dir = f"{self.output_dir}/{date}"
            if not self.client.status(date_dir, strict=False):
                self.client.makedirs(date_dir)
            
            # Write data
            with self.client.write(output_path, overwrite=True) as writer:
                df.to_csv(writer, index=False)
            
            logger.info(f"Successfully wrote data to: {output_path}")
        except Exception as e:
            logger.error(f"Error writing output: {e}")

    def run(self, duration_minutes: int = 30):
        """Run the ETL pipeline for the specified duration."""
        end_time = time.time() + (duration_minutes * 60)
        
        logger.info(f"Starting ETL pipeline for {duration_minutes} minutes")
        
        while time.time() < end_time:
            try:
                new_files = self.get_new_files()
                
                for file in new_files:
                    logger.info(f"Processing new file: {file}")
                    
                    # Read and process the file
                    df = self.read_file(file)
                    if not df.empty:
                        processed_df = self.process_data(df)
                        
                        if not processed_df.empty:
                            # Write output for each date in the processed data
                            for date in processed_df['date'].unique():
                                date_df = processed_df[processed_df['date'] == date]
                                self.write_output(date_df, str(date))
                            
                            self.processed_files.add(file)
                
                time.sleep(self.check_interval)
                
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(self.check_interval)

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='HDFS ETL Pipeline for processing transaction data')
    
    parser.add_argument(
        '--hdfs-url',
        type=str,
        default='http://localhost:9870',
        help='HDFS WebHDFS URL (default: http://localhost:9870)'
    )
    
    parser.add_argument(
        '--input-dir',
        type=str,
        default='/user/input/transactions',
        help='Input directory path in HDFS (default: /user/input/transactions)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='/user/output/aggregated',
        help='Output directory path in HDFS (default: /user/output/aggregated)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=30,
        help='Duration to run the pipeline in minutes (default: 30)'
    )
    
    parser.add_argument(
        '--check-interval',
        type=int,
        default=60,
        help='Interval in seconds to check for new files (default: 60)'
    )
    
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_arguments()
    
    # Create and run the pipeline
    pipeline = HDFSETLPipeline(
        hdfs_url=args.hdfs_url,
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        check_interval=args.check_interval
    )
    pipeline.run(duration_minutes=args.duration)

if __name__ == "__main__":
    main() 