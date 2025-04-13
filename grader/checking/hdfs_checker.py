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
from contextlib import contextmanager
from .base import LabChecker, CheckerReport, CheckReport
from hdfs import InsecureClient
from .utils import is_hdfs_path, download_from_hdfs, get_local_script_path

@contextmanager
def temporary_hdfs_directory(client: InsecureClient, base_dir: str, prefix: str = "check_") -> str:
    """Context manager for creating and cleaning up a temporary HDFS directory."""
    import uuid
    
    # Create a unique subdirectory name
    temp_dir = f"{base_dir}/{prefix}{uuid.uuid4().hex}"
    
    try:
        # Create the temporary directory
        client.makedirs(temp_dir)
        yield temp_dir
    finally:
        # Clean up the temporary directory and its contents
        try:
            if client.status(temp_dir, strict=False):
                # List all files in the directory
                files = client.list(temp_dir)
                # Delete all files
                for file in files:
                    client.delete(f"{temp_dir}/{file}")
                # Delete the directory itself
                client.delete(temp_dir)
        except Exception as e:
            print(f"Warning: Failed to clean up temporary directory {temp_dir}: {e}")

class HDFSChecker(LabChecker):
    def __init__(self, hdfs_url: str, base_dir: str, 
                 test_data: List[Dict[str, Any]], golden_data: Dict[str, pd.DataFrame],
                 process_start_delay: int = 2,
                 file_write_interval: int = 5,
                 final_wait_time: int = 5,
                 etl_duration: int = 30,
                 etl_check_interval: int = 5):
        """
        Initialize the HDFS checker.
        
        Args:
            hdfs_url: HDFS WebHDFS URL
            base_dir: Base directory path in HDFS where temporary directories will be created
            test_data: List of test data dictionaries to write to HDFS
            golden_data: Dictionary of golden data DataFrames for each date
            process_start_delay: Delay in seconds after starting the ETL process
            file_write_interval: Interval in seconds between writing test files
            final_wait_time: Time to wait after writing the last file
            etl_duration: Duration in minutes for the ETL process to run
            etl_check_interval: Interval in seconds for the ETL process to check for new files
        """
        super().__init__()
        self.hdfs_url = hdfs_url
        self.base_dir = base_dir
        self.test_data = test_data
        self.golden_data = golden_data
        self.process: Optional[subprocess.Popen] = None
        self.script_path = Path(__file__).parent.parent.parent / "resources" / "hdfs" / "hdfs_etl.py"
        
        # Delay parameters
        self.process_start_delay = process_start_delay
        self.file_write_interval = file_write_interval
        self.final_wait_time = final_wait_time
        self.etl_duration = etl_duration
        self.etl_check_interval = etl_check_interval

    def start_etl_pipeline(self, input_dir: str, output_dir: str) -> None:
        """Start the ETL pipeline in a separate process."""
        # Get local path to script if it's on HDFS
        local_script_path = get_local_script_path(
            str(self.script_path),
            self.hdfs_url.split(':')[0],  # Extract host from URL
            int(self.hdfs_url.split(':')[1])  # Extract port from URL
        )
        
        cmd = [
            "python", local_script_path,
            "--hdfs-url", self.hdfs_url,
            "--input-dir", input_dir,
            "--output-dir", output_dir,
            "--duration", str(self.etl_duration),
            "--check-interval", str(self.etl_check_interval)
        ]
        
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # Wait for the process to start
        time.sleep(self.process_start_delay)

    def write_test_data(self, input_dir: str) -> None:
        """Write test data to HDFS with specified intervals."""
        client = InsecureClient(self.hdfs_url)
        
        # Write each test data file with specified intervals
        for i, data in enumerate(self.test_data):
            file_name = f"test_data_{i+1}.csv"
            file_path = f"{input_dir}/{file_name}"
            
            # Convert data to DataFrame and write to HDFS
            df = pd.DataFrame(data)
            with client.write(file_path, overwrite=True) as writer:
                df.to_csv(writer, index=False)
            
            time.sleep(self.file_write_interval)  # Wait between writes
        
        # Wait after the last file
        time.sleep(self.final_wait_time)

    def check_output_files(self, report: CheckerReport, output_dir: str) -> None:
        """Check if output files match the golden data."""
        client = InsecureClient(self.hdfs_url)
        
        try:
            # Get all date directories in output directory
            dates = client.list(output_dir)
            
            for date in dates:
                output_file = f"{output_dir}/{date}/aggregated_data.csv"
                
                # Check if file exists
                if not client.status(output_file, strict=False):
                    report.fail(
                        f"Check output file for date {date}",
                        f"Output file not found: {output_file}",
                        check_group="Output Files"
                    )
                    return
                
                # Read output file
                with client.read(output_file) as reader:
                    output_df = pd.read_csv(reader)
                
                # Check if date exists in golden data
                if date not in self.golden_data:
                    report.fail(
                        f"Check date {date} in golden data",
                        f"Unexpected date in output: {date}",
                        check_group="Output Files"
                    )
                    return
                
                # Compare with golden data
                golden_df = self.golden_data[date]
                
                # Check columns
                if not all(col in output_df.columns for col in golden_df.columns):
                    report.fail(
                        f"Check columns for date {date}",
                        f"Missing columns in output file for date {date}",
                        check_group="Output Files"
                    )
                    return
                
                # Check data types
                for col in golden_df.columns:
                    if output_df[col].dtype != golden_df[col].dtype:
                        report.fail(
                            f"Check data types for date {date}",
                            f"Wrong data type for column {col} in date {date}",
                            check_group="Output Files"
                        )
                        return
                
                # Compare values using DataFrame.compare
                try:
                    differences = output_df.compare(
                        golden_df,
                        align_axis=1,  # Align differences horizontally
                        keep_shape=True,  # Keep all rows and columns
                        keep_equal=False,  # Don't show equal values
                        result_names=('output', 'golden')  # Custom names for comparison
                    )
                    
                    if not differences.empty:
                        # Format differences for error message
                        diff_str = differences.to_string()
                        report.fail(
                            f"Check data values for date {date}",
                            f"Data mismatch found:\n{diff_str}",
                            check_group="Output Files"
                        )
                        return
                    
                except ValueError as e:
                    report.fail(
                        f"Check data values for date {date}",
                        f"Error comparing data: {str(e)}",
                        check_group="Output Files"
                    )
                    return
                
                report.success(
                    f"Check output file for date {date}",
                    check_group="Output Files"
                )
            
        except Exception as e:
            report.fail(
                "Check output files",
                f"Error checking output files: {str(e)}",
                check_group="Output Files"
            )

    def run_checks(self) -> CheckerReport:
        """Run the checker."""
        report = CheckerReport()
        client = InsecureClient(self.hdfs_url)
        
        try:
            # Create temporary directories for input and output
            with temporary_hdfs_directory(client, self.base_dir, "hdfs_checker_") as tmp_dir:
                input_dir = client.makedirs(f"{tmp_dir}/input")
                output_dir = client.makedirs(f"{tmp_dir}/output")
                
                # Start ETL pipeline
                self.start_etl_pipeline(input_dir, output_dir)
                if not self.process:
                    report.fail(
                        "Start ETL pipeline",
                        "Failed to start ETL pipeline",
                        check_group="Process"
                    )
                    return report
                
                report.success(
                    "Start ETL pipeline",
                    check_group="Process"
                )
                
                # Write test data
                self.write_test_data(input_dir)
                report.success(
                    "Write test data",
                    check_group="Process"
                )
                
                # Check if process is still running
                if self.process.poll() is not None:
                    report.fail(
                        "Check ETL pipeline running",
                        "ETL pipeline terminated prematurely",
                        check_group="Process"
                    )
                    return report
                
                report.success(
                    "Check ETL pipeline running",
                    check_group="Process"
                )
                
                # Stop the process
                self.process.terminate()
                self.process.wait(timeout=5)
                report.success(
                    "Stop ETL pipeline",
                    check_group="Process"
                )
                
                # Check output files
                self.check_output_files(report, output_dir)
            
        except Exception as e:
            report.fail(
                "Run HDFS ETL checker",
                f"Error during checking: {str(e)}",
                check_group="Process"
            )
        
        finally:
            # Ensure process is terminated
            if self.process and self.process.poll() is None:
                self.process.terminate()
                self.process.wait(timeout=5)
        
        return report


