from flask import Flask, render_template_string, request, jsonify
import re
from dataclasses import dataclass, asdict
from typing import List, Optional

app = Flask(__name__)

@dataclass
class Field:
    level: int
    name: str
    pic: Optional[str]
    sql_type: str
    length: Optional[int]
    parent: Optional[str]

class CopybookParser:
    def __init__(self):
        self.fields = []
        self.parent_stack = []
    
    def parse(self, content: str) -> List[Field]:
        self.fields = []
        self.parent_stack = []
        
        for line in content.split('\n'):
            if line.strip().startswith('*') or not line.strip():
                continue
            
            pattern = r'^\s*(\d{2})\s+([A-Z0-9\-]+)(?:\s+(?:PIC|PICTURE)\s+([^\s.]+))?'
            match = re.match(pattern, line, re.IGNORECASE)
            
            if not match:
                continue
            
            level = int(match.group(1))
            name = match.group(2)
            pic = match.group(3)
            
            while self.parent_stack and self.parent_stack[-1][0] >= level:
                self.parent_stack.pop()
            
            parent = self.parent_stack[-1][1] if self.parent_stack else None
            
            if not pic:
                self.parent_stack.append((level, name))
                continue
            
            sql_type, length = self._parse_pic(pic)
            
            self.fields.append(Field(
                level=level,
                name=name,
                pic=pic,
                sql_type=sql_type,
                length=length,
                parent=parent
            ))
        
        return self.fields
    
    def _parse_pic(self, pic: str):
        pic = pic.upper().strip()
        
        # Decimal: 9(5)V99
        if 'V' in pic:
            m = re.search(r'9\((\d+)\)V9\((\d+)\)', pic)
            if m:
                p, s = int(m.group(1)), int(m.group(2))
                return f'DECIMAL({p+s},{s})', p+s
        
        # Integer: 9(n)
        m = re.search(r'9\((\d+)\)', pic)
        if m:
            length = int(m.group(1))
            return ('BIGINT' if length > 9 else 'INTEGER'), length
        
        # Character: X(n)
        m = re.search(r'[XA]\((\d+)\)', pic)
        if m:
            length = int(m.group(1))
            return f'VARCHAR({length})', length
        
        return 'VARCHAR(255)', 255
    
    def generate_ddl(self, table_name: str):
        ddl = f"CREATE TABLE bronze.{table_name} (\n"
        cols = []
        for f in self.fields:
            col = f"    {f.name.lower().replace('-', '_')} {f.sql_type}"
            cols.append(col)
        ddl += ",\n".join(cols) + "\n);"
        return ddl

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html>
<head>
    <title>Copybook Parser POC</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: Arial, sans-serif; background: #f5f5f5; }
        .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
        h1 { color: #333; margin-bottom: 20px; }
        .panel { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        h2 { color: #555; margin-bottom: 15px; font-size: 18px; }
        textarea { width: 100%; height: 300px; padding: 10px; border: 1px solid #ddd; border-radius: 4px; font-family: 'Courier New', monospace; font-size: 13px; }
        input[type="text"] { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; margin-bottom: 10px; }
        button { background: #007bff; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; font-size: 14px; }
        button:hover { background: #0056b3; }
        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }
        th { background: #f8f9fa; font-weight: 600; }
        tr:hover { background: #f8f9fa; }
        pre { background: #f8f9fa; padding: 15px; border-radius: 4px; overflow-x: auto; font-size: 13px; }
        .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
        @media (max-width: 768px) { .grid { grid-template-columns: 1fr; } }
    </style>
</head>
<body>
    <div class="container">
        <h1>📋 Copybook Parser - POC</h1>
        
        <div class="panel">
            <h2>Step 1: Paste Copybook Content</h2>
            <textarea id="copybook" placeholder="Paste your COBOL copybook here...">      * CUSTOMER RECORD
       01  CUSTOMER-RECORD.
           05  CUSTOMER-ID            PIC 9(10).
           05  CUSTOMER-NAME.
               10  FIRST-NAME         PIC X(30).
               10  LAST-NAME          PIC X(30).
           05  ACCOUNT-NUMBER         PIC 9(12).
           05  ACCOUNT-BALANCE        PIC S9(13)V99.
           05  EMAIL-ADDR             PIC X(50).
           05  SSN                    PIC 9(9).</textarea>
            <br><br>
            <input type="text" id="tableName" placeholder="Enter table name (e.g., customer)" value="customer">
            <button onclick="parseCopybook()">Parse & Generate</button>
        </div>
        
        <div class="grid">
            <div class="panel">
                <h2>Step 2: Field Metadata</h2>
                <div id="metadata">Click "Parse & Generate" to see metadata</div>
            </div>
            
            <div class="panel">
                <h2>Step 3: Generated DDL</h2>
                <div id="ddl">DDL will appear here</div>
            </div>
        </div>
    </div>
    
    <script>
        async function parseCopybook() {
            const copybook = document.getElementById('copybook').value;
            const tableName = document.getElementById('tableName').value;
            
            if (!copybook || !tableName) {
                alert('Please provide both copybook content and table name');
                return;
            }
            
            const response = await fetch('/parse', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({copybook, tableName})
            });
            
            const data = await response.json();
            
            // Display metadata table
            let metaHtml = '<table><tr><th>Level</th><th>Field Name</th><th>PIC</th><th>SQL Type</th><th>Length</th><th>Parent</th></tr>';
            data.fields.forEach(f => {
                metaHtml += `<tr>
                    <td>${f.level}</td>
                    <td>${f.name}</td>
                    <td>${f.pic || '-'}</td>
                    <td>${f.sql_type}</td>
                    <td>${f.length || '-'}</td>
                    <td>${f.parent || '-'}</td>
                </tr>`;
            });
            metaHtml += '</table>';
            document.getElementById('metadata').innerHTML = metaHtml;
            
            // Display DDL
            document.getElementById('ddl').innerHTML = `<pre>${data.ddl}</pre>`;
        }
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/parse', methods=['POST'])
def parse():
    data = request.get_json()
    copybook = data.get('copybook', '')
    table_name = data.get('tableName', 'table')
    
    parser = CopybookParser()
    fields = parser.parse(copybook)
    ddl = parser.generate_ddl(table_name)
    
    return jsonify({
        'fields': [asdict(f) for f in fields],
        'ddl': ddl
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)