"""
ETL Agent Pipeline POC using LangGraph
Supports: CSV, JSON, Parquet, Excel files
No LLM required - uses rule-based schema inference
"""

import os
import duckdb
import pandas as pd
from pathlib import Path
from typing import TypedDict, Literal, Any
from langgraph.graph import StateGraph, END
import json

# Define the state for our ETL pipeline
class ETLState(TypedDict):
    file_path: str
    file_type: str
    raw_data: Any
    schema: dict
    table_name: str
    db_path: str
    status: str
    error: str
    row_count: int


class ETLAgent:
    """ETL Agent for automated data ingestion"""
    
    def __init__(self, db_path: str = "etl_database.duckdb"):
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self.workflow = self._build_workflow()
    
    def _build_workflow(self) -> StateGraph:
        """Build the LangGraph workflow"""
        workflow = StateGraph(ETLState)
        
        # Add nodes
        workflow.add_node("detect_file_type", self.detect_file_type)
        workflow.add_node("extract_data", self.extract_data)
        workflow.add_node("infer_schema", self.infer_schema)
        workflow.add_node("transform_data", self.transform_data)
        workflow.add_node("load_to_db", self.load_to_db)
        workflow.add_node("validate", self.validate)
        
        # Define edges
        workflow.set_entry_point("detect_file_type")
        workflow.add_edge("detect_file_type", "extract_data")
        workflow.add_edge("extract_data", "infer_schema")
        workflow.add_edge("infer_schema", "transform_data")
        workflow.add_edge("transform_data", "load_to_db")
        workflow.add_edge("load_to_db", "validate")
        workflow.add_edge("validate", END)
        
        return workflow.compile()
    
    def detect_file_type(self, state: ETLState) -> ETLState:
        """Detect the file type from extension"""
        print(f"📁 Detecting file type: {state['file_path']}")
        
        file_path = Path(state['file_path'])
        if not file_path.exists():
            state['status'] = 'error'
            state['error'] = f"File not found: {file_path}"
            return state
        
        extension = file_path.suffix.lower()
        file_type_map = {
            '.csv': 'csv',
            '.json': 'json',
            '.parquet': 'parquet',
            '.xlsx': 'excel',
            '.xls': 'excel'
        }
        
        state['file_type'] = file_type_map.get(extension, 'unknown')
        state['table_name'] = file_path.stem.lower().replace(' ', '_')
        state['db_path'] = self.db_path
        
        if state['file_type'] == 'unknown':
            state['status'] = 'error'
            state['error'] = f"Unsupported file type: {extension}"
        else:
            state['status'] = 'file_detected'
        
        print(f"✓ File type: {state['file_type']}, Table name: {state['table_name']}")
        return state
    
    def extract_data(self, state: ETLState) -> ETLState:
        """Extract data from the file"""
        print(f"📤 Extracting data from {state['file_type']} file...")
        
        try:
            file_path = state['file_path']
            file_type = state['file_type']
            
            if file_type == 'csv':
                df = pd.read_csv(file_path)
            elif file_type == 'json':
                df = pd.read_json(file_path)
            elif file_type == 'parquet':
                df = pd.read_parquet(file_path)
            elif file_type == 'excel':
                df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Cannot extract {file_type} files")
            
            state['raw_data'] = df
            state['row_count'] = len(df)
            state['status'] = 'data_extracted'
            print(f"✓ Extracted {len(df)} rows, {len(df.columns)} columns")
            
        except Exception as e:
            state['status'] = 'error'
            state['error'] = f"Extraction failed: {str(e)}"
            print(f"✗ {state['error']}")
        
        return state
    
    def infer_schema(self, state: ETLState) -> ETLState:
        """Infer schema from the data"""
        print(f"🔍 Inferring schema...")
        
        try:
            df = state['raw_data']
            schema = {}
            
            for col in df.columns:
                dtype = df[col].dtype
                null_count = df[col].isnull().sum()
                unique_count = df[col].nunique()
                
                # Map pandas dtypes to SQL types
                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = 'INTEGER'
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = 'DOUBLE'
                elif pd.api.types.is_bool_dtype(dtype):
                    sql_type = 'BOOLEAN'
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sql_type = 'TIMESTAMP'
                else:
                    sql_type = 'VARCHAR'
                
                schema[col] = {
                    'sql_type': sql_type,
                    'nullable': null_count > 0,
                    'unique_count': unique_count,
                    'null_count': null_count
                }
            
            state['schema'] = schema
            state['status'] = 'schema_inferred'
            print(f"✓ Schema inferred for {len(schema)} columns")
            
            # Print schema summary
            for col, info in schema.items():
                print(f"  - {col}: {info['sql_type']} "
                      f"(nulls: {info['null_count']}, unique: {info['unique_count']})")
        
        except Exception as e:
            state['status'] = 'error'
            state['error'] = f"Schema inference failed: {str(e)}"
            print(f"✗ {state['error']}")
        
        return state
    
    def transform_data(self, state: ETLState) -> ETLState:
        """Transform data (cleaning, normalization)"""
        print(f"🔄 Transforming data...")
        
        try:
            df = state['raw_data']
            
            # Clean column names: remove special chars, spaces
            df.columns = df.columns.str.strip().str.lower() \
                .str.replace(' ', '_').str.replace('[^a-z0-9_]', '', regex=True)
            
            # Handle date columns
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Try to parse as datetime
                    try:
                        parsed = pd.to_datetime(df[col], errors='coerce')
                        if parsed.notna().sum() > len(df) * 0.8:  # 80% valid dates
                            df[col] = parsed
                    except:
                        pass
            
            state['raw_data'] = df
            state['status'] = 'data_transformed'
            print(f"✓ Data transformed successfully")
            
        except Exception as e:
            state['status'] = 'error'
            state['error'] = f"Transformation failed: {str(e)}"
            print(f"✗ {state['error']}")
        
        return state
    
    def load_to_db(self, state: ETLState) -> ETLState:
        """Load data to DuckDB"""
        print(f"💾 Loading data to DuckDB table '{state['table_name']}'...")
        
        try:
            df = state['raw_data']
            table_name = state['table_name']
            
            # Drop table if exists
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Create table from dataframe
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            
            state['status'] = 'data_loaded'
            print(f"✓ Loaded {len(df)} rows to table '{table_name}'")
            
        except Exception as e:
            state['status'] = 'error'
            state['error'] = f"Load failed: {str(e)}"
            print(f"✗ {state['error']}")
        
        return state
    
    def validate(self, state: ETLState) -> ETLState:
        """Validate the loaded data"""
        print(f"✅ Validating data...")
        
        try:
            table_name = state['table_name']
            
            # Check row count
            result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            db_row_count = result[0]
            
            if db_row_count == state['row_count']:
                state['status'] = 'success'
                print(f"✓ Validation passed: {db_row_count} rows in database")
            else:
                state['status'] = 'warning'
                state['error'] = f"Row count mismatch: expected {state['row_count']}, got {db_row_count}"
                print(f"⚠ {state['error']}")
            
            # Show sample data
            print(f"\n📊 Sample data from '{table_name}':")
            sample = self.conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf()
            print(sample.to_string())
            
        except Exception as e:
            state['status'] = 'error'
            state['error'] = f"Validation failed: {str(e)}"
            print(f"✗ {state['error']}")
        
        return state
    
    def ingest_file(self, file_path: str) -> ETLState:
        """Main method to ingest a file"""
        print(f"\n{'='*60}")
        print(f"🚀 Starting ETL Pipeline for: {file_path}")
        print(f"{'='*60}\n")
        
        initial_state = ETLState(
            file_path=file_path,
            file_type="",
            raw_data=None,
            schema={},
            table_name="",
            db_path=self.db_path,
            status="",
            error="",
            row_count=0
        )
        
        # Run the workflow
        result = self.workflow.invoke(initial_state)
        
        print(f"\n{'='*60}")
        print(f"📋 Pipeline Status: {result['status'].upper()}")
        if result['error']:
            print(f"❌ Error: {result['error']}")
        print(f"{'='*60}\n")
        
        return result
    
    def list_tables(self):
        """List all tables in the database"""
        tables = self.conn.execute("SHOW TABLES").fetchall()
        return [table[0] for table in tables]
    
    def query(self, sql: str):
        """Execute a SQL query"""
        return self.conn.execute(sql).fetchdf()
    
    def close(self):
        """Close database connection"""
        self.conn.close()


# Example usage
if __name__ == "__main__":
    # Create sample data files for testing
    print("Creating sample data files for testing...\n")
    
    # Sample CSV
    sample_csv = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 40, 45],
        'salary': [50000.0, 60000.0, 70000.0, 80000.0, 90000.0],
        'hire_date': ['2020-01-15', '2019-03-20', '2021-06-10', '2018-11-05', '2022-02-28']
    })
    sample_csv.to_csv('sample_employees.csv', index=False)
    
    # Sample JSON
    sample_json = [
        {'product_id': 1, 'product_name': 'Laptop', 'price': 999.99, 'in_stock': True},
        {'product_id': 2, 'product_name': 'Mouse', 'price': 29.99, 'in_stock': True},
        {'product_id': 3, 'product_name': 'Keyboard', 'price': 79.99, 'in_stock': False}
    ]
    with open('sample_products.json', 'w') as f:
        json.dump(sample_json, f)
    
    # Initialize ETL Agent
    agent = ETLAgent(db_path="etl_demo.duckdb")
    
    # Ingest CSV file
    agent.ingest_file('sample_employees.csv')
    
    # Ingest JSON file
    agent.ingest_file('sample_products.json')
    
    # Show all tables
    print("\n📚 Tables in database:")
    tables = agent.list_tables()
    for table in tables:
        print(f"  - {table}")
    
    # Run some queries
    print("\n🔎 Sample queries:")
    print("\n1. Employees with salary > 60000:")
    result = agent.query("SELECT * FROM sample_employees WHERE salary > 60000")
    print(result)
    
    print("\n2. Products in stock:")
    result = agent.query("SELECT * FROM sample_products WHERE in_stock = true")
    print(result)
    
    # Cleanup
    agent.close()
    print("\n✅ ETL Pipeline POC completed successfully!")
