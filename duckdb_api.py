from flask import Flask, request, jsonify
import duckdb
import os
import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Database configuration
DB_FILE = "data.duckdb"
DB_PATH = os.path.join(os.getcwd(), DB_FILE)

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database and create tables if they don't exist"""
        try:
            conn = duckdb.connect(self.db_path)
            
            # Create a general data table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_records (
                    id INTEGER PRIMARY KEY,
                    key VARCHAR,
                    value TEXT,
                    data_type VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create an index on the key for faster lookups
            conn.execute("CREATE INDEX IF NOT EXISTS idx_key ON data_records(key)")
            
            conn.close()
            logger.info(f"Database initialized at {self.db_path}")
            
        except Exception as e:
            logger.error(f"Error initializing database: {str(e)}")
            raise
    
    def get_connection(self):
        """Get a database connection"""
        return duckdb.connect(self.db_path)

# Initialize database manager
db_manager = DatabaseManager(DB_PATH)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "database": DB_PATH,
        "timestamp": datetime.now().isoformat()
    })

@app.route('/data', methods=['POST'])
def save_data():
    """
    Save data to DuckDB
    Expected JSON format:
    {
        "key": "unique_identifier",
        "value": "any_value_or_json_object",
        "data_type": "string|number|json|boolean" (optional)
    }
    """
    try:
        # Get JSON data from request
        if not request.is_json:
            return jsonify({"error": "Request must be JSON"}), 400
        
        data = request.get_json()
        
        # Validate required fields
        if 'key' not in data or 'value' not in data:
            return jsonify({"error": "Both 'key' and 'value' are required"}), 400
        
        key = data['key']
        value = data['value']
        data_type = data.get('data_type', 'string')
        
        # Convert value to string if it's a complex object
        if isinstance(value, (dict, list)):
            value_str = json.dumps(value)
            data_type = 'json'
        else:
            value_str = str(value)
        
        # Connect to database and save data
        conn = db_manager.get_connection()
        
        # Check if key already exists
        existing = conn.execute(
            "SELECT id FROM data_records WHERE key = ?", [key]
        ).fetchone()
        
        if existing:
            # Update existing record
            conn.execute("""
                UPDATE data_records 
                SET value = ?, data_type = ?, updated_at = CURRENT_TIMESTAMP 
                WHERE key = ?
            """, [value_str, data_type, key])
            operation = "updated"
        else:
            # Insert new record
            conn.execute("""
                INSERT INTO data_records (key, value, data_type) 
                VALUES (?, ?, ?)
            """, [key, value_str, data_type])
            operation = "created"
        
        conn.close()
        
        return jsonify({
            "message": f"Data {operation} successfully",
            "key": key,
            "data_type": data_type
        }), 201 if operation == "created" else 200
        
    except Exception as e:
        logger.error(f"Error saving data: {str(e)}")
        return jsonify({"error": f"Failed to save data: {str(e)}"}), 500

@app.route('/data/<key>', methods=['GET'])
def get_data_by_key(key):
    """Retrieve data by key from DuckDB"""
    try:
        conn = db_manager.get_connection()
        
        result = conn.execute("""
            SELECT key, value, data_type, created_at, updated_at 
            FROM data_records 
            WHERE key = ?
        """, [key]).fetchone()
        
        conn.close()
        
        if not result:
            return jsonify({"error": f"No data found for key: {key}"}), 404
        
        key, value, data_type, created_at, updated_at = result
        
        # Parse JSON values back to objects
        if data_type == 'json':
            try:
                parsed_value = json.loads(value)
            except json.JSONDecodeError:
                parsed_value = value
        else:
            parsed_value = value
        
        return jsonify({
            "key": key,
            "value": parsed_value,
            "data_type": data_type,
            "created_at": created_at.isoformat() if created_at else None,
            "updated_at": updated_at.isoformat() if updated_at else None
        })
        
    except Exception as e:
        logger.error(f"Error retrieving data: {str(e)}")
        return jsonify({"error": f"Failed to retrieve data: {str(e)}"}), 500

@app.route('/data', methods=['GET'])
def get_all_data():
    """Retrieve all data from DuckDB with optional pagination"""
    try:
        # Get query parameters
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        
        conn = db_manager.get_connection()
        
        # Get total count
        total_count = conn.execute("SELECT COUNT(*) FROM data_records").fetchone()[0]
        
        # Get paginated results
        results = conn.execute("""
            SELECT key, value, data_type, created_at, updated_at 
            FROM data_records 
            ORDER BY updated_at DESC 
            LIMIT ? OFFSET ?
        """, [limit, offset]).fetchall()
        
        conn.close()
        
        # Format results
        data_list = []
        for row in results:
            key, value, data_type, created_at, updated_at = row
            
            # Parse JSON values back to objects
            if data_type == 'json':
                try:
                    parsed_value = json.loads(value)
                except json.JSONDecodeError:
                    parsed_value = value
            else:
                parsed_value = value
            
            data_list.append({
                "key": key,
                "value": parsed_value,
                "data_type": data_type,
                "created_at": created_at.isoformat() if created_at else None,
                "updated_at": updated_at.isoformat() if updated_at else None
            })
        
        return jsonify({
            "data": data_list,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": offset + limit < total_count
            }
        })
        
    except Exception as e:
        logger.error(f"Error retrieving all data: {str(e)}")
        return jsonify({"error": f"Failed to retrieve data: {str(e)}"}), 500

@app.route('/data/<key>', methods=['DELETE'])
def delete_data(key):
    """Delete data by key from DuckDB"""
    try:
        conn = db_manager.get_connection()
        
        # Check if key exists
        existing = conn.execute(
            "SELECT id FROM data_records WHERE key = ?", [key]
        ).fetchone()
        
        if not existing:
            conn.close()
            return jsonify({"error": f"No data found for key: {key}"}), 404
        
        # Delete the record
        conn.execute("DELETE FROM data_records WHERE key = ?", [key])
        conn.close()
        
        return jsonify({"message": f"Data with key '{key}' deleted successfully"})
        
    except Exception as e:
        logger.error(f"Error deleting data: {str(e)}")
        return jsonify({"error": f"Failed to delete data: {str(e)}"}), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Endpoint not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    print(f"Starting DuckDB API service...")
    print(f"Database file: {DB_PATH}")
    print(f"API endpoints:")
    print(f"  GET /health - Health check")
    print(f"  POST /data - Save data")
    print(f"  GET /data/<key> - Get data by key")
    print(f"  GET /data - Get all data (with pagination)")
    print(f"  DELETE /data/<key> - Delete data by key")
    
    app.run(host='0.0.0.0', port=5000, debug=True)