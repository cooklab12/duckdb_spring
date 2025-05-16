import duckdb
import json
import os
import sys
from pathlib import Path

def query_parquet_files(parquet_files, sql_query, output_file=None):
    """
    Query multiple parquet files using DuckDB and output results as JSON.
    DuckDB can directly query parquet files without creating intermediate tables/views.
    
    Args:
        parquet_files (list): List of paths to parquet files
        sql_query (str): SQL query to execute
        output_file (str, optional): Path to save JSON output. If None, prints to stdout.
    
    Returns:
        dict: Query results in dictionary format
    """
    try:
        # Validate all files exist before proceeding
        for file_path in parquet_files:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Parquet file not found: {file_path}")
        
        # Create a DuckDB connection
        con = duckdb.connect(database=':memory:')
        
        # In DuckDB, you can directly query parquet files without creating tables
        # We'll replace placeholders in the query with the actual file paths
        
        # Create a dictionary mapping table names to file paths
        table_mapping = {}
        for i, file_path in enumerate(parquet_files):
            table_name = f"table_{i}"
            table_mapping[table_name] = file_path
            print(f"Mapping '{file_path}' to reference '{table_name}'")
        
        # Replace table references in the query with read_parquet calls
        modified_query = sql_query
        for table_name, file_path in table_mapping.items():
            # Use regex pattern to match the table name as a whole word
            # This avoids replacing table_1 when looking for table_
            file_path_escaped = file_path.replace("'", "''")  # Escape single quotes
            modified_query = modified_query.replace(
                f"{table_name}", 
                f"read_parquet('{file_path_escaped}')"
            )
        
        print(f"Executing query: {modified_query}")
        
        # Execute the modified query
        result = con.execute(modified_query).fetchdf()
        
        # Convert result to JSON
        json_result = result.to_json(orient='records')
        parsed_result = json.loads(json_result)
        
        # Output the result
        if output_file:
            with open(output_file, 'w') as f:
                json.dump(parsed_result, f, indent=2)
            print(f"Results saved to {output_file}")
        else:
            print(json.dumps(parsed_result, indent=2))
        
        return parsed_result
    
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if 'con' in locals():
            con.close()

def main():
    """
    Main function to parse command line arguments and execute the query.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Query Parquet files using DuckDB and output as JSON')
    parser.add_argument('parquet_files', nargs='+', help='Paths to parquet files')
    parser.add_argument('--query', '-q', required=True, help='SQL query to execute')
    parser.add_argument('--output', '-o', help='Path to save JSON output (optional)')
    
    args = parser.parse_args()
    
    query_parquet_files(args.parquet_files, args.query, args.output)

if __name__ == "__main__":
    main()
