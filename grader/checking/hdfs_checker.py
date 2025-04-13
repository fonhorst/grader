# TODO: implement hdfs checker
# 1. It should start a script with HDFS ETL Pipeline in a separate process supplying all required parameters
# 2. It should start a routing that writes test data periodically to HDFS folder, 
# being monitored by the checkerby the script. 
# The routing should write 3 files with period of 5 seconds and wait another 5 seconds 
# after the last file.
# 3. The checker should check if the script has been alive for all the required time when the routing has been active.
# 4. The checker should stop the script at this moment. 
# The first check is successful if there is no error until the script is stopped.
# Otherwise, the checker should return a failure and stop here.
# 5. The checker should check if the output folder now:
# - the folder should contain all required files with propoer naming
# - each file must have correct columns of proper data types
# - each file must be compared with the golden version of the file 
# (the golden version coresponds to the inputs given by the routine 
# and both supplied to the checker through it's constructor arguments)

import os
import time
import subprocess
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from .base import BaseChecker, CheckResult

class HDFSChecker(BaseChecker):
    def __init__(self, hdfs_url: str, input_dir: str, output_dir: str, 
                 test_data: List[Dict[str, Any]], golden_data: Dict[str, pd.DataFrame]):
        """
        Initialize the HDFS checker.
        
        Args:
            hdfs_url: HDFS WebHDFS URL
            input_dir: Input directory path in HDFS
            output_dir: Output directory path in HDFS
            test_data: List of test data dictionaries to write to HDFS
            golden_data: Dictionary of golden data DataFrames for each date
        """
        super().__init__()
        self.hdfs_url = hdfs_url
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.test_data = test_data
        self.golden_data = golden_data
        self.process: Optional[subprocess.Popen] = None
        self.script_path = Path(__file__).parent.parent.parent / "resources" / "hdfs" / "hdfs_etl.py"

    def start_etl_pipeline(self) -> None:
        """Start the ETL pipeline in a separate process."""
        cmd = [
            "python", str(self.script_path),
            "--hdfs-url", self.hdfs_url,
            "--input-dir", self.input_dir,
            "--output-dir", self.output_dir,
            "--duration", "30",
            "--check-interval", "5"
        ]
        
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait a bit for the process to start
        time.sleep(2)

    def write_test_data(self) -> None:
        """Write test data to HDFS with specified intervals."""
        from hdfs import InsecureClient
        
        client = InsecureClient(self.hdfs_url)
        
        # Create input directory if it doesn't exist
        if not client.status(self.input_dir, strict=False):
            client.makedirs(self.input_dir)
        
        # Write each test data file with 5-second intervals
        for i, data in enumerate(self.test_data):
            file_name = f"test_data_{i+1}.csv"
            file_path = f"{self.input_dir}/{file_name}"
            
            # Convert data to DataFrame and write to HDFS
            df = pd.DataFrame(data)
            with client.write(file_path, overwrite=True) as writer:
                df.to_csv(writer, index=False)
            
            time.sleep(5)  # Wait 5 seconds between writes
        
        # Wait another 5 seconds after the last file
        time.sleep(5)

    def check_output_files(self) -> CheckResult:
        """Check if output files match the golden data."""
        from hdfs import InsecureClient
        
        client = InsecureClient(self.hdfs_url)
        
        try:
            # Get all date directories in output directory
            dates = client.list(self.output_dir)
            
            for date in dates:
                output_file = f"{self.output_dir}/{date}/aggregated_data.csv"
                
                # Check if file exists
                if not client.status(output_file, strict=False):
                    return CheckResult(False, f"Output file not found: {output_file}")
                
                # Read output file
                with client.read(output_file) as reader:
                    output_df = pd.read_csv(reader)
                
                # Check if date exists in golden data
                if date not in self.golden_data:
                    return CheckResult(False, f"Unexpected date in output: {date}")
                
                # Compare with golden data
                golden_df = self.golden_data[date]
                
                # Check columns
                if not all(col in output_df.columns for col in golden_df.columns):
                    return CheckResult(False, f"Missing columns in output file for date {date}")
                
                # Check data types
                for col in golden_df.columns:
                    if output_df[col].dtype != golden_df[col].dtype:
                        return CheckResult(False, f"Wrong data type for column {col} in date {date}")
                
                # Compare values (with small tolerance for floating point numbers)
                if not output_df.equals(golden_df):
                    return CheckResult(False, f"Data mismatch for date {date}")
            
            return CheckResult(True, "All output files match golden data")
            
        except Exception as e:
            return CheckResult(False, f"Error checking output files: {str(e)}")

    def run(self) -> CheckResult:
        """Run the checker."""
        try:
            # Start ETL pipeline
            self.start_etl_pipeline()
            if not self.process:
                return CheckResult(False, "Failed to start ETL pipeline")
            
            # Write test data
            self.write_test_data()
            
            # Check if process is still running
            if self.process.poll() is not None:
                return CheckResult(False, "ETL pipeline terminated prematurely")
            
            # Stop the process
            self.process.terminate()
            self.process.wait(timeout=5)
            
            # Check output files
            return self.check_output_files()
            
        except Exception as e:
            return CheckResult(False, f"Error during checking: {str(e)}")
        
        finally:
            # Ensure process is terminated
            if self.process and self.process.poll() is None:
                self.process.terminate()
                self.process.wait(timeout=5)


