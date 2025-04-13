# HDFS ETL Pipeline

This is a Python implementation of an ETL pipeline that processes transaction data from HDFS.

## Requirements

- Python 3.7+
- HDFS cluster with WebHDFS enabled
- Required Python packages (see requirements.txt)

## Installation

1. Install the required packages:
```bash
pip install -r requirements.txt
```

2. Make sure you have access to an HDFS cluster with WebHDFS enabled.

## Usage

Run the ETL pipeline with the following command:

```bash
python hdfs_etl.py [options]
```

### Command Line Options

- `--hdfs-url`: HDFS WebHDFS URL (default: "http://localhost:9870")
- `--input-dir`: Input directory path in HDFS (default: "/user/input/transactions")
- `--output-dir`: Output directory path in HDFS (default: "/user/output/aggregated")
- `--duration`: Duration to run the pipeline in minutes (default: 30)
- `--check-interval`: Interval in seconds to check for new files (default: 60)

### Examples

Run with default settings:
```bash
python hdfs_etl.py
```

Run with custom HDFS URL and input/output directories:
```bash
python hdfs_etl.py --hdfs-url "http://hdfs-cluster:9870" --input-dir "/data/input" --output-dir "/data/output"
```

Run for a specific duration with custom check interval:
```bash
python hdfs_etl.py --duration 60 --check-interval 30
```

The script will:
- Monitor the input directory for new files
- Process each new file
- Filter successful transactions
- Convert amounts to USD
- Aggregate data by date
- Write results to the output directory

## Sample Data

A sample data file (`sample_data.csv`) is provided for testing. The expected output for this sample data would be:

| date       | total_amount | transaction_count | average_amount |
|------------|--------------|-------------------|----------------|
| 2023-12-20 | 850.00       | 3                 | 283.33         |

## Logging

The script logs all important events to the console, including:
- File discovery
- File reading
- Data processing
- Output writing
- Any errors that occur

## Error Handling

The script includes error handling for:
- HDFS connection issues
- File reading/writing errors
- Data processing errors
- Temporary HDFS unavailability 