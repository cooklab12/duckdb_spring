import duckdb
import argparse
from datetime import datetime
import os
import subprocess
import json
from pathlib import Path
import tempfile
import shutil

def parse_arguments():
    """Parse command line arguments for date filters and file paths"""
    parser = argparse.ArgumentParser(description='Join data from multiple Parquet files with date filtering and output as JSON')
    
    # File paths
    parser.add_argument('--table1', required=True, help='HDFS path to first Parquet file/directory')
    parser.add_argument('--table2', required=True, help='HDFS path to second Parquet file/directory')
    parser.add_argument('--table3', required=True, help='HDFS path to third Parquet file/directory')
    
    # Date filters
    parser.add_argument('--start_date', required=True, help='Start date for filtering (YYYY-MM-DD)')
    parser.add_argument('--end_date', required=True, help='End date for filtering (YYYY-MM-DD)')
    
    # Date column name
    parser.add_argument('--date_column', default='date', help='Name of the date column to filter on')
    
    # Output options
    parser.add_argument('--output', default='joined_results.json', 
                       help='Local output file path for the joined results in JSON format')
    
    return parser.parse_args()

def validate_date(date_str):
    """Validate date format"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Please use YYYY-MM-DD format.")

def copy_from_hdfs(hdfs_path, local_dir):
    """Copy a file or directory from HDFS to local filesystem"""
    local_path = os.path.join(local_dir, Path(hdfs_path).name)
    print(f"Copying from HDFS: {hdfs_path} -> {local_path}")
    
    # Run hdfs dfs -get command
    result = subprocess.run(['hdfs', 'dfs', '-get', hdfs_path, local_path], 
                           capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Failed to copy from HDFS: {result.stderr}")
        
    return local_path

def join_parquet_data(table1_path, table2_path, table3_path, 
                      date_column, start_date, end_date, 
                      output_path):
    """Join three Parquet tables with date filtering using DuckDB and output as JSON"""
    
    # Create a temporary directory for local data files
    temp_dir = tempfile.mkdtemp()
    try:
        # Copy Parquet files from HDFS to local filesystem
        local_table1 = copy_from_hdfs(table1_path, temp_dir)
        local_table2 = copy_from_hdfs(table2_path, temp_dir)
        local_table3 = copy_from_hdfs(table3_path, temp_dir)
        
        # Connect to in-memory DuckDB
        con = duckdb.connect(':memory:')
        
        # Register tables from local Parquet files
        con.execute(f"CREATE VIEW table1 AS SELECT * FROM parquet_scan('{local_table1}')")
        con.execute(f"CREATE VIEW table2 AS SELECT * FROM parquet_scan('{local_table2}')")
        con.execute(f"CREATE VIEW table3 AS SELECT * FROM parquet_scan('{local_table3}')")
        
        # Build the join query - adjust join conditions based on your data schema
        query = f"""
        SELECT t1.*, t2.*, t3.*
        FROM table1 t1
        JOIN table2 t2 ON t1.join_key1 = t2.join_key1
        JOIN table3 t3 ON t1.join_key2 = t3.join_key2
        WHERE {date_column} BETWEEN '{start_date}' AND '{end_date}'
        """
        
        # Execute the query and fetch all results
        result_df = con.execute(query).fetchdf()
        
        # Get result count
        result_count = len(result_df)
        print(f"Query returned {result_count} rows")
        
        # Convert to JSON and save to the output file
        # Handle NaN and dates correctly for JSON serialization
        result_json = result_df.to_json(orient='records')
        with open(output_path, 'w') as f:
            f.write(result_json)
            
        print(f"Results saved to JSON file: {output_path}")
        
        return result_count
        
    finally:
        # Clean up - remove the temporary directory and all its contents
        print(f"Cleaning up temporary files in: {temp_dir}")
        shutil.rmtree(temp_dir, ignore_errors=True)

def main():
    # Parse arguments
    args = parse_arguments()
    
    # Validate dates
    validate_date(args.start_date)
    validate_date(args.end_date)
    
    print(f"Processing Parquet files from HDFS:")
    print(f"- Table 1: {args.table1}")
    print(f"- Table 2: {args.table2}")
    print(f"- Table 3: {args.table3}")
    print(f"Filtering where {args.date_column} is between {args.start_date} and {args.end_date}")
    
    try:
        # Perform the join operation
        result_count = join_parquet_data(
            args.table1, args.table2, args.table3,
            args.date_column, args.start_date, args.end_date,
            args.output
        )
        
        print(f"Join completed successfully!")
        print(f"- {result_count} rows written to {args.output} in JSON format")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
