# Stock Market Data ETL Pipeline

A high-performance ETL (Extract, Transform, Load) pipeline designed to process large volumes of stock market data (up to 100GB across multiple CSV files) and load it into a PostgreSQL database for analysis.

## Features

- **Memory-efficient processing**: Processes large CSV files in manageable chunks
- **Parallel execution**: Uses Python's `concurrent.futures` for multi-process execution
- **Data validation**: Validates and cleans input data before loading
- **Checkpoint support**: Resume capability if the process is interrupted
- **Automated returns calculation**: Automatically computes daily stock returns
- **Configurable**: YAML-based configuration for easy customization
- **Robust error handling**: Graceful handling of interruptions and errors

## Requirements

- Python 3.6+
- PostgreSQL database
- Required Python packages:
  - pandas
  - numpy
  - psycopg2
  - pyyaml

## Configuration

The ETL process is configured through a YAML file (`config.yaml`). Here's a sample configuration:

```yaml
# Database settings
database:
  url: "postgresql://user:password@localhost/stocks_db"
  schema: "public"
  
# Input data settings
input:
  directory: "/path/to/csv/files"
  file_pattern: "*.csv"
  
# Processing settings
processing:
  chunk_size: 100000  # Number of rows to process at once
  max_workers: 3      # Number of parallel processes
  
# Logging settings
logging:
  level: "INFO"       # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
  log_file: "etl.log" # Optional log file
  
# Checkpoint settings
checkpoint:
  enabled: true       # Enable checkpointing for resume capability
  interval: 2         # Save checkpoint every N files
  file: "etl_checkpoint.json"
```

## Input Data Format

The ETL process expects CSV files with the following columns:
- `date`: Trading date
- `id`: Stock identifier (1-200)
- `price`: Stock price
- `trade_volume`: Number of shares traded

## Database Schema

The pipeline creates three tables:

1. **price_table**: Daily stock prices
   - `date` (PRIMARY KEY)
   - `stk_001` through `stk_200` (price values)

2. **volume_table**: Daily trading volumes
   - `date` (PRIMARY KEY)
   - `stk_001` through `stk_200` (volume values)

3. **returns_table**: Daily returns (calculated)
   - `date` (PRIMARY KEY)
   - `stk_001` through `stk_200` (return values)

## Usage

Run the ETL process:

```bash
python stock_etl.py --config config.yaml
```

Additional options:
- `--input-dir PATH`: Override input directory from config
- `--db-url URL`: Override database URL from config
- `--resume`: Resume from checkpoint if available

## Process Flow

1. **Setup**: Parse configuration, set up logging, create database tables
2. **Extract**: Read CSV files in chunks to minimize memory usage
3. **Transform**: Validate and clean data
4. **Load**: Insert processed data into PostgreSQL tables
5. **Calculate**: Compute daily returns based on price data

## Performance Considerations

- The script uses chunking to minimize memory usage
- Parallel processing of files increases throughput
- Database indexes are created for optimized queries
- UPSERTs are used to handle potential duplicates
- Checkpointing allows for resuming interrupted operations

## Error Handling

- Input validation ensures data quality
- Graceful handling of interruptions via signal handlers
- Comprehensive logging for troubleshooting
- Transaction management prevents partial updates

