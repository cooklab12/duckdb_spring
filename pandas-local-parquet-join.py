import pandas as pd
import argparse
from datetime import datetime
import os

def parse_arguments():
    """Parse command line arguments for date filters and file paths"""
    parser = argparse.ArgumentParser(description='Join data from multiple local Parquet files with date filtering using pandas')
    
    # File paths
    parser.add_argument('--table1', required=True, help='Path to first Parquet file/directory')
    parser.add_argument('--table2', required=True, help='Path to second Parquet file/directory')
    parser.add_argument('--table3', required=True, help='Path to third Parquet file/directory')
    
    # Date filters
    parser.add_argument('--start_date', required=True, help='Start date for filtering (YYYY-MM-DD)')
    parser.add_argument('--end_date', required=True, help='End date for filtering (YYYY-MM-DD)')
    
    # Date column name
    parser.add_argument('--date_column', default='date', help='Name of the date column to filter on')
    
    # Join columns
    parser.add_argument('--join_key1', required=True, help='Join key between table1 and table2')
    parser.add_argument('--join_key2', required=True, help='Join key between merged table and table3')
    
    # Output options
    parser.add_argument('--output', required=True, help='Local path for the joined results in JSON format')
    
    # Processing options
    parser.add_argument('--chunk_size', type=int, default=100000, 
                        help='Chunk size for processing large files (default: 100000)')
    
    return parser.parse_args()

def validate_date(date_str):
    """Validate date format"""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format: {date_str}. Please use YYYY-MM-DD format.")

def join_parquet_data(table1_path, table2_path, table3_path, 
                     date_column, start_date, end_date, 
                     join_key1, join_key2, output_path, chunk_size):
    """Join three Parquet tables with date filtering using pandas"""
    
    # Define a date filter function
    def date_filter(df):
        if date_column in df.columns:
            # Convert column to datetime if not already
            if not pd.api.types.is_datetime64_dtype(df[date_column]):
                df[date_column] = pd.to_datetime(df[date_column])
            return df[(df[date_column] >= start_date) & (df[date_column] <= end_date)]
        return df
    
    # Read table2 and table3 into memory
    print(f"Reading table2 from {table2_path}")
    df2 = pd.read_parquet(table2_path)
    
    print(f"Reading table3 from {table3_path}")
    df3 = pd.read_parquet(table3_path)
    
    # Process table1 in chunks to handle potentially large files
    df_chunks = []
    print(f"Reading and processing table1 from {table1_path} in chunks of {chunk_size}")
    
    # Check if we're dealing with a directory of parquet files or a single file
    if os.path.isdir(table1_path):
        # For directory, we'll read each file in chunks
        parquet_files = [os.path.join(table1_path, f) for f in os.listdir(table1_path) 
                         if f.endswith('.parquet')]
        
        for file in parquet_files:
            for chunk in pd.read_parquet(file, chunksize=chunk_size):
                # Process each chunk
                processed_chunk = process_chunk(chunk, df2, df3, date_column, 
                                               start_date, end_date, join_key1, join_key2)
                if processed_chunk is not None and not processed_chunk.empty:
                    df_chunks.append(processed_chunk)
    else:
        # For single file, read in chunks
        for chunk in pd.read_parquet(table1_path, chunksize=chunk_size):
            processed_chunk = process_chunk(chunk, df2, df3, date_column, 
                                           start_date, end_date, join_key1, join_key2)
            if processed_chunk is not None and not processed_chunk.empty:
                df_chunks.append(processed_chunk)
    
    # Combine all chunks
    if df_chunks:
        final_df = pd.concat(df_chunks, ignore_index=True)
        row_count = len(final_df)
        
        # Write result to JSON
        print(f"Writing {row_count} rows to {output_path}")
        final_df.to_json(output_path, orient='records', lines=False)
        
        return row_count
    else:
        print("No data found matching the criteria")
        # Create empty JSON file
        with open(output_path, 'w') as f:
            f.write('[]')
        return 0

def process_chunk(chunk, df2, df3, date_column, start_date, end_date, join_key1, join_key2):
    """Process a chunk of data by filtering and joining"""
    # Filter by date if the date column is in this table
    if date_column in chunk.columns:
        if not pd.api.types.is_datetime64_dtype(chunk[date_column]):
            chunk[date_column] = pd.to_datetime(chunk[date_column])
        chunk = chunk[(chunk[date_column] >= start_date) & (chunk[date_column] <= end_date)]
    
    # Skip further processing if chunk is empty after filtering
    if chunk.empty:
        return None
    
    # Join with table2
    merged = pd.merge(chunk, df2, on=join_key1, how='inner')
    
    # Apply date filter if date column is in merged result
    if date_column in merged.columns:
        if not pd.api.types.is_datetime64_dtype(merged[date_column]):
            merged[date_column] = pd.to_datetime(merged[date_column])
        merged = merged[(merged[date_column] >= start_date) & (merged[date_column] <= end_date)]
    
    # Skip further processing if merged result is empty
    if merged.empty:
        return None
    
    # Join with table3
    result = pd.merge(merged, df3, on=join_key2, how='inner')
    
    # Apply date filter if date column is in final result
    if date_column in result.columns:
        if not pd.api.types.is_datetime64_dtype(result[date_column]):
            result[date_column] = pd.to_datetime(result[date_column])
        result = result[(result[date_column] >= start_date) & (result[date_column] <= end_date)]
    
    return result

def main():
    # Parse arguments
    args = parse_arguments()
    
    # Validate dates
    start_date = validate_date(args.start_date)
    end_date = validate_date(args.end_date)
    
    print(f"Processing local Parquet files:")
    print(f"- Table 1: {args.table1}")
    print(f"- Table 2: {args.table2}")
    print(f"- Table 3: {args.table3}")
    print(f"- Joining on keys: {args.join_key1} and {args.join_key2}")
    print(f"- Filtering where {args.date_column} is between {args.start_date} and {args.end_date}")
    
    try:
        # Perform the join operation
        result_count = join_parquet_data(
            args.table1, args.table2, args.table3,
            args.date_column, start_date, end_date,
            args.join_key1, args.join_key2, args.output, args.chunk_size
        )
        
        print(f"Join completed successfully!")
        print(f"- {result_count} rows written to {args.output} in JSON format")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
