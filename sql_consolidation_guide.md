# SQL Query Consolidation for Spark and AWS Deequ - Complete Guide

## Problem Statement

You have multiple SQL queries like:
- `SELECT col1,col2 FROM table1`
- `SELECT col3,col1 FROM table1` 
- `SELECT col5,col6 FROM table1`

You need to consolidate these into a single SQL query to pass to your AWS Deequ library Spark program, as Deequ only takes 1 DataFrame and 1 SQL as input.

## Solution Overview

We'll provide both AI and non-AI solutions for consolidating SQL queries efficiently, plus a comprehensive framework for handling complex scenarios.

---

## AI Solution Options

### Cloud-Based AI
```python
import openai

def consolidate_queries_ai(queries):
    prompt = f"""
    Consolidate these SQL queries into one efficient query that selects all unique columns:
    {chr(10).join(queries)}
    
    Requirements:
    - Remove duplicate column selections
    - Use the same table references
    - Optimize for Spark execution
    """
    # API call to get consolidated query
```

### Local AI Options
- **Ollama** with CodeLlama or similar models locally
- **Hugging Face Transformers** with SQL-focused models

---

## Non-AI Solution - Basic SQL Consolidator

### Simple Consolidator Implementation

```python
import re
from typing import List, Set, Dict, Tuple
import sqlparse
from sqlparse.sql import Statement, IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

class SQLConsolidator:
    def __init__(self):
        self.unique_columns = set()
        self.table_references = set()
        self.where_conditions = []
        self.join_conditions = []
    
    def parse_select_query(self, query: str) -> Dict:
        """Parse a SELECT query to extract columns and table info"""
        parsed = sqlparse.parse(query)[0]
        
        result = {
            'columns': [],
            'tables': [],
            'where_clause': None,
            'joins': []
        }
        
        # Find SELECT clause
        select_seen = False
        from_seen = False
        
        for token in parsed.flatten():
            if token.ttype is DML and token.value.upper() == 'SELECT':
                select_seen = True
                continue
            elif token.ttype is Keyword and token.value.upper() == 'FROM':
                select_seen = False
                from_seen = True
                continue
            elif token.ttype is Keyword and token.value.upper() in ('WHERE', 'GROUP', 'ORDER', 'HAVING'):
                from_seen = False
                continue
                
            if select_seen and token.value.strip() and token.value.strip() not in (',', ' '):
                if ',' in token.value:
                    cols = [col.strip() for col in token.value.split(',') if col.strip()]
                    result['columns'].extend(cols)
                else:
                    result['columns'].append(token.value.strip())
                    
            elif from_seen and token.value.strip() and token.value.strip() not in (',', ' '):
                result['tables'].append(token.value.strip())
        
        return result
    
    def extract_query_components(self, query: str) -> Dict:
        """Extract components from SQL query using regex (simpler approach)"""
        query = query.strip()
        
        # Extract SELECT columns
        select_pattern = r'SELECT\s+(.+?)\s+FROM'
        select_match = re.search(select_pattern, query, re.IGNORECASE | re.DOTALL)
        
        columns = []
        if select_match:
            col_string = select_match.group(1).strip()
            # Split by comma but handle potential aliases
            columns = [col.strip() for col in col_string.split(',')]
            columns = [col for col in columns if col]  # Remove empty strings
        
        # Extract table name
        table_pattern = r'FROM\s+(\w+)'
        table_match = re.search(table_pattern, query, re.IGNORECASE)
        table = table_match.group(1) if table_match else None
        
        # Extract WHERE clause if present
        where_pattern = r'WHERE\s+(.+?)(?:\s+ORDER|\s+GROUP|\s*$)'
        where_match = re.search(where_pattern, query, re.IGNORECASE | re.DOTALL)
        where_clause = where_match.group(1).strip() if where_match else None
        
        return {
            'columns': columns,
            'table': table,
            'where_clause': where_clause
        }
    
    def consolidate_queries(self, queries: List[str]) -> str:
        """Consolidate multiple SQL queries into one efficient query"""
        all_columns = set()
        tables = set()
        where_conditions = []
        
        for query in queries:
            components = self.extract_query_components(query)
            
            # Collect unique columns
            for col in components['columns']:
                # Clean column names (remove extra spaces, handle aliases)
                clean_col = col.strip()
                all_columns.add(clean_col)
            
            # Collect table references
            if components['table']:
                tables.add(components['table'])
            
            # Collect WHERE conditions
            if components['where_clause']:
                where_conditions.append(f"({components['where_clause']})")
        
        # Validate that all queries use the same table
        if len(tables) > 1:
            raise ValueError(f"Queries reference different tables: {tables}")
        elif len(tables) == 0:
            raise ValueError("No valid table found in queries")
        
        table_name = list(tables)[0]
        
        # Build consolidated query
        columns_str = ', '.join(sorted(all_columns))
        
        consolidated_query = f"SELECT {columns_str} FROM {table_name}"
        
        # Add WHERE conditions if any exist
        if where_conditions:
            # Combine WHERE conditions with OR (you might want AND depending on use case)
            where_str = ' OR '.join(where_conditions)
            consolidated_query += f" WHERE {where_str}"
        
        return consolidated_query
    
    def optimize_for_spark(self, query: str) -> str:
        """Apply Spark-specific optimizations"""
        # Add hints for Spark optimization
        optimized = query
        
        # You can add Spark-specific optimizations here:
        # - Add broadcast hints for small tables
        # - Optimize column order
        # - Add appropriate caching hints
        
        return optimized
```

### Usage Example

```python
def main():
    consolidator = SQLConsolidator()
    
    # Simple queries (WORKS WELL)
    simple_queries = [
        "SELECT col1, col2 FROM table1",
        "SELECT col3, col1 FROM table1", 
        "SELECT col5, col6 FROM table1",
        "SELECT col2, col7 FROM table1 WHERE id > 100"
    ]
    
    # Complex queries (PROBLEMATIC)
    complex_queries = [
        """SELECT t1.col1, 
                  t2.col2, 
                  CASE WHEN t1.status = 'active' THEN 1 ELSE 0 END as is_active,
                  SUM(t1.amount) OVER (PARTITION BY t1.category) as category_total
           FROM table1 t1 
           JOIN table2 t2 ON t1.id = t2.table1_id 
           WHERE t1.created_date >= '2023-01-01'""",
        
        """SELECT DISTINCT t1.col3,
                  AVG(t1.score) as avg_score,
                  COUNT(*) as record_count
           FROM table1 t1
           LEFT JOIN table3 t3 ON t1.region_id = t3.id
           WHERE t3.region_name IN ('North', 'South')
           GROUP BY t1.col3
           HAVING COUNT(*) > 10""",
        
        """SELECT col4, col5,
                  ROW_NUMBER() OVER (ORDER BY updated_date DESC) as rn
           FROM (
               SELECT col4, col5, updated_date
               FROM table1 
               WHERE status NOT IN ('deleted', 'archived')
           ) subquery"""
    ]
    
    try:
        consolidated = consolidator.consolidate_queries(simple_queries)
        optimized = consolidator.optimize_for_spark(consolidated)
        
        print("Original queries:")
        for i, query in enumerate(simple_queries, 1):
            print(f"{i}. {query}")
        
        print(f"\nConsolidated query:")
        print(consolidated)
        
        print(f"\nSpark-optimized query:")
        print(optimized)
        
    except Exception as e:
        print(f"Error: {e}")
```

---

## Advanced Non-AI Solution for Complex Queries

### Enhanced SQL Consolidator

```python
import re
import sqlparse
from sqlparse.sql import Statement, IdentifierList, Identifier, Function
from sqlparse.tokens import Keyword, DML, Punctuation
from typing import List, Set, Dict, Tuple, Optional
from dataclasses import dataclass
from enum import Enum

class QueryComplexity(Enum):
    SIMPLE = "simple"
    MODERATE = "moderate" 
    COMPLEX = "complex"
    VERY_COMPLEX = "very_complex"

@dataclass
class QueryComponents:
    columns: List[str]
    computed_columns: List[str]  # Expressions, functions, etc.
    tables: List[str]
    joins: List[str]
    where_conditions: List[str]
    group_by: List[str]
    having_conditions: List[str]
    order_by: List[str]
    window_functions: List[str]
    subqueries: List[str]
    complexity: QueryComplexity

class AdvancedSQLConsolidator:
    def __init__(self):
        self.supported_functions = {
            'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'COALESCE', 'CASE',
            'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD'
        }
        
    def analyze_query_complexity(self, query: str) -> QueryComplexity:
        """Determine the complexity level of the query"""
        query_upper = query.upper()
        
        complexity_indicators = {
            'subqueries': len(re.findall(r'\(.*?SELECT.*?\)', query_upper, re.DOTALL)),
            'joins': len(re.findall(r'\b(?:INNER|LEFT|RIGHT|FULL|CROSS)\s+JOIN\b', query_upper)),
            'window_functions': len(re.findall(r'OVER\s*\(', query_upper)),
            'aggregates': len(re.findall(r'\b(?:SUM|COUNT|AVG|MIN|MAX)\s*\(', query_upper)),
            'case_statements': len(re.findall(r'\bCASE\b.*?\bEND\b', query_upper, re.DOTALL)),
            'group_by': 1 if 'GROUP BY' in query_upper else 0,
            'having': 1 if 'HAVING' in query_upper else 0
        }
        
        total_score = sum(complexity_indicators.values())
        
        if total_score == 0:
            return QueryComplexity.SIMPLE
        elif total_score <= 2:
            return QueryComplexity.MODERATE
        elif total_score <= 5:
            return QueryComplexity.COMPLEX
        else:
            return QueryComplexity.VERY_COMPLEX

    def can_consolidate_queries(self, query_components: List[QueryComponents]) -> Tuple[bool, str]:
        """Determine if queries can be safely consolidated"""
        
        # Check 1: Different complexity levels
        complexities = [qc.complexity for qc in query_components]
        if any(c in [QueryComplexity.COMPLEX, QueryComplexity.VERY_COMPLEX] for c in complexities):
            return False, "Contains complex queries with joins, subqueries, or window functions"
        
        # Check 2: Different base tables
        all_tables = set()
        for qc in query_components:
            all_tables.update(qc.tables)
        if len(all_tables) > 1:
            return False, f"Queries reference different tables: {all_tables}"
        
        # Check 3: Presence of aggregations or grouping
        has_aggregations = any(qc.group_by or qc.having_conditions for qc in query_components)
        if has_aggregations:
            return False, "Contains GROUP BY or HAVING clauses that cannot be merged"
        
        # Check 4: Window functions
        has_window_functions = any(qc.window_functions for qc in query_components)
        if has_window_functions:
            return False, "Contains window functions that require separate processing"
        
        # Check 5: Subqueries
        has_subqueries = any(qc.subqueries for qc in query_components)
        if has_subqueries:
            return False, "Contains subqueries that cannot be easily merged"
        
        return True, "Queries can be consolidated"

    def provide_alternative_approaches(self, queries: List[str]) -> Dict[str, str]:
        """Provide alternative approaches for complex queries that can't be consolidated"""
        approaches = {}
        
        # Approach 1: Union-based consolidation
        approaches['union_approach'] = self._create_union_query(queries)
        
        # Approach 2: CTE-based approach
        approaches['cte_approach'] = self._create_cte_query(queries)
        
        # Approach 3: Materialized view approach
        approaches['materialized_view'] = self._suggest_materialized_view(queries)
        
        return approaches

    def _create_union_query(self, queries: List[str]) -> str:
        """Create a UNION ALL query for incompatible queries"""
        # Add a query_source column to identify which original query each row came from
        numbered_queries = []
        for i, query in enumerate(queries, 1):
            modified = re.sub(r'SELECT', f'SELECT {i} as query_source, ', query, flags=re.IGNORECASE)
            numbered_queries.append(f"({modified})")
        
        return " UNION ALL ".join(numbered_queries)

    def _create_cte_query(self, queries: List[str]) -> str:
        """Create a CTE-based consolidated query"""
        ctes = []
        for i, query in enumerate(queries, 1):
            cte_name = f"q{i}"
            ctes.append(f"{cte_name} AS ({query})")
        
        # Create final SELECT that unions all CTEs
        cte_selects = [f"SELECT * FROM q{i}" for i in range(1, len(queries) + 1)]
        
        return f"WITH {', '.join(ctes)} {' UNION ALL '.join(cte_selects)}"
```

---

## Limitations of Non-AI Solutions

### What Complex Queries Cannot Be Handled:

1. **Different JOINs**: Queries with different JOIN conditions
2. **Window Functions**: ROW_NUMBER(), RANK(), etc.
3. **Subqueries**: Nested SELECT statements
4. **Different WHERE logic**: Complex business logic differences
5. **Aggregations**: GROUP BY, HAVING clauses
6. **Different table aliases**: Same table with different aliases and complex references

---

## Strategic Decision: UNION ALL vs Multiple DataFrames

**Question**: Should we use UNION ALL fallback or modify Spark program for multiple DataFrames?

**Answer**: **Modify your Spark program to handle different DataFrames is better.**

### UNION ALL Problems:
- **Schema conflicts** - Complex queries often have different column types/names
- **Performance penalty** - Forces Spark to process all data even if you only need specific columns
- **Memory overhead** - Combines all data unnecessarily 
- **Lost optimization** - Spark can't optimize individual query patterns
- **Difficult debugging** - Hard to trace issues back to original queries

### Multiple DataFrames Benefits:
- **Parallel processing** - Spark can execute queries simultaneously
- **Optimized execution plans** - Each query gets its own optimal plan
- **Memory efficiency** - Only loads needed data per query
- **Better caching** - Can cache individual results strategically
- **Cleaner code** - Easier to maintain and debug

### For Deequ Specifically:
```python
# Instead of forcing one DataFrame, do this:
for i, query in enumerate(complex_queries):
    df = spark.sql(query)
    check = Check(spark, CheckLevel.Warning, f"Check_{i}")
    result = VerificationSuite(spark).onData(df).addCheck(check).run()
    # Process individual results
```

---

## Smart DataFrames Strategy

**Don't treat every SQL as a separate DataFrame** - that would go against Spark's design principles.

### Spark's Optimal Approach:

**Batch similar queries together** based on:
1. **Same base tables** - Queries hitting the same source data
2. **Similar complexity** - Simple selects vs complex analytics
3. **Similar execution patterns** - Similar WHERE clauses, partitioning keys
4. **Logical groupings** - Related business logic

### Smart Batching Strategy:

```python
def group_queries_intelligently(queries):
    groups = {
        'simple_table1': [],      # Simple selects from table1
        'simple_table2': [],      # Simple selects from table2  
        'aggregations_table1': [], # GROUP BY queries on table1
        'joins_complex': [],       # Multi-table joins
        'window_functions': []     # Analytical queries
    }
    
    # Group by pattern similarity
    for query in queries:
        group_key = classify_query(query)
        groups[group_key].append(query)
    
    return groups
```

### Best Practice for Your Use Case:

1. **Consolidate within logical groups** (3-5 similar queries per DataFrame)
2. **Keep complex queries separate** if they have different execution patterns
3. **Share DataFrames** when queries use the same base data with different filters
4. **Use broadcast joins** for small lookup tables across multiple queries

**The goal**: Balance Spark efficiency with logical separation - typically 5-10 DataFrames total, not 50+ individual ones.

---

## Full-Proof Approach for Query Grouping

### Phase 1: Query Classification and Analysis

1. **Parse each SQL query** to extract:
   - Base tables referenced
   - Join patterns (if any)
   - Aggregation functions used
   - Window functions present
   - WHERE clause complexity
   - Column selectivity (how many columns vs total available)

2. **Assign complexity score** to each query:
   - Simple: Basic SELECT with simple WHERE
   - Moderate: Single aggregations, basic joins
   - Complex: Multiple joins, window functions, subqueries

### Phase 2: Grouping Logic (Hierarchical Approach)

**Primary Grouping** (by data access pattern):
- Group by **primary table** first
- Separate **single-table** vs **multi-table** queries
- Isolate queries with **different partition keys**

**Secondary Grouping** (by execution pattern):
- **Read-heavy group**: Simple SELECTs that can be consolidated
- **Aggregation group**: Queries with GROUP BY that hit same partitions  
- **Join group**: Multi-table queries with similar join patterns
- **Analytics group**: Window functions and complex calculations
- **Filter-heavy group**: Queries with expensive WHERE clauses

**Final Grouping** (by resource requirements):
- **Memory-light**: Simple column selections
- **Memory-heavy**: Large aggregations or wide result sets
- **CPU-intensive**: Complex calculations or regex operations

### Phase 3: DataFrame Creation Decision Matrix

**Create Single Consolidated DataFrame When:**
- All queries hit the same table with simple SELECT
- Similar WHERE clause patterns (can be OR'ed together)
- Column overlap > 60%
- No aggregations or same aggregation pattern
- Total estimated result size < 1GB

**Create Separate DataFrames When:**
- Different base tables
- Different join patterns
- Mix of aggregated vs non-aggregated results
- Different partitioning requirements
- One query is significantly more expensive than others

**Create Batched DataFrames When:**
- Queries share 80%+ of the same base data
- Similar complexity but different business logic
- Can benefit from shared table scans
- Result schemas are compatible

### Phase 4: Execution Strategy

**DataFrame Creation Timing:**
1. **Eager creation**: For small, frequently-used base tables
2. **Lazy creation**: For large tables - create DataFrame only when needed
3. **Cached creation**: For base tables used by multiple groups

**Resource Management:**
- Limit to **5-8 concurrent DataFrames** maximum
- **Cache base DataFrames** that are reused across groups
- **Persist intermediate results** if used by multiple downstream queries
- **Unpersist** DataFrames after all dependent queries complete

### Phase 5: Optimization Rules

**Memory Optimization:**
- Process **memory-heavy groups first** when cluster is fresh
- **Spill lighter queries** to later execution if resources are constrained

**I/O Optimization:**
- **Batch queries by partition keys** to minimize table scans
- **Combine compatible filters** to push down to storage layer

**Execution Optimization:**
- **Parallelize independent groups**
- **Sequence dependent groups** (where one uses output of another)
- **Use broadcast joins** for small dimension tables across multiple groups

### Key Success Metrics:

- **Data locality**: Minimize cross-node shuffles
- **Resource utilization**: Keep CPU/memory usage steady
- **Execution time**: Total time should be less than individual execution
- **Memory efficiency**: Peak memory usage should be manageable

---

## Shell Script + Configuration-Driven Approach

### Overview

Create a smart, configuration-driven system that automatically groups SQL queries, creates optimal DataFrames, and handles fallbacks - all orchestrated by shell scripts without code changes.

### Phase 1: Create Query Configuration Files

**Structure your queries in JSON/YAML configs:**

```bash
# queries_config/
├── table1_simple.json
├── table1_aggregations.json  
├── multi_table_joins.json
└── analytics_queries.json
```

**Each config file contains:**
- Query definitions
- Grouping hints
- Resource requirements
- Fallback options
- Deequ check definitions

### Phase 2: Pre-Processing Script (Query Analyzer)

**Create `analyze_queries.py`:**
- Reads all query config files
- Analyzes and groups queries using your consolidation logic
- Generates execution plan JSON
- Estimates resource requirements
- Outputs optimized execution strategy

**Shell script calls:**
```bash
# Generate execution plan
python analyze_queries.py --config-dir queries_config/ --output execution_plan.json

# Check if analysis succeeded
if [ $? -ne 0 ]; then
    echo "Query analysis failed, using fallback strategy"
    cp fallback_execution_plan.json execution_plan.json
fi
```

### Phase 3: Dynamic Spark Program Structure

**Make your Spark program config-driven:**
- Reads `execution_plan.json` at startup
- Creates DataFrames based on the plan
- Executes Deequ checks per group
- Handles fallback scenarios dynamically

**Key components:**
- **DataFrame Factory**: Creates DFs based on config
- **Execution Orchestrator**: Manages execution flow
- **Fallback Handler**: Switches strategies on failure
- **Resource Monitor**: Tracks memory/performance

### Phase 4: Intelligent Shell Script Orchestration

```bash
#!/bin/bash
# smart_spark_execution.sh

# Step 1: Environment setup
export SPARK_CONF_DIR=/path/to/spark-conf
export MAX_MEMORY="8g"
export MAX_CORES="4"

# Step 2: Query analysis and planning
echo "Analyzing queries..."
python analyze_queries.py \
    --config-dir queries_config/ \
    --output execution_plan.json \
    --max-memory $MAX_MEMORY \
    --max-cores $MAX_CORES

# Step 3: Validate execution plan
if [ ! -f "execution_plan.json" ]; then
    echo "Using emergency fallback plan"
    cp emergency_fallback.json execution_plan.json
fi

# Step 4: Dynamic Spark submission
EXECUTION_STRATEGY=$(jq -r '.strategy' execution_plan.json)
ESTIMATED_MEMORY=$(jq -r '.estimated_memory' execution_plan.json)
PARALLELISM=$(jq -r '.parallelism' execution_plan.json)

echo "Execution strategy: $EXECUTION_STRATEGY"
echo "Memory requirement: $ESTIMATED_MEMORY"

# Step 5: Adaptive Spark configuration
spark-submit \
    --master local[$PARALLELISM] \
    --driver-memory $ESTIMATED_MEMORY \
    --executor-memory $ESTIMATED_MEMORY \
    --conf "spark.sql.adaptive.enabled=true" \
    --conf "spark.sql.adaptive.coalescePartitions.enabled=true" \
    dynamic_spark_program.py \
    --execution-plan execution_plan.json \
    --fallback-enabled true
```

---

## Sample Input/Output for Complete Workflow

### 1. Query Configs → Analyzer

**Input: `queries_config/table1_simple.json`**
```json
{
  "queries": [
    {
      "id": "q1",
      "sql": "SELECT col1, col2 FROM table1 WHERE status = 'active'",
      "description": "Active records basic info",
      "estimated_rows": 10000
    },
    {
      "id": "q2", 
      "sql": "SELECT col3, col1 FROM table1 WHERE created_date > '2023-01-01'",
      "description": "Recent records",
      "estimated_rows": 15000
    }
  ],
  "table_metadata": {
    "primary_table": "table1",
    "partition_key": "date_partition",
    "total_columns": 20,
    "avg_row_size_kb": 2
  }
}
```

**Input: `queries_config/table1_complex.json`**
```json
{
  "queries": [
    {
      "id": "q5",
      "sql": "SELECT t1.col1, COUNT(*) OVER (PARTITION BY t1.region) as region_count FROM table1 t1 JOIN table2 t2 ON t1.id = t2.ref_id WHERE t1.amount > 1000",
      "description": "Regional analysis with window function",
      "estimated_rows": 50000
    }
  ],
  "resource_hints": {
    "memory_intensive": true,
    "requires_shuffle": true
  }
}
```

### 2. Analyzer → Execution Plan

**Command:**
```bash
python query_analyzer.py --config-dir queries_config/ --output execution_plan.json --max-memory 8g
```

**Output: `execution_plan.json`**
```json
{
  "strategy": "mixed_execution",
  "estimated_memory": "6g",
  "parallelism": 4,
  "total_queries": 3,
  "execution_groups": [
    {
      "group_id": "simple_table1_consolidated", 
      "execution_type": "consolidated",
      "queries": ["q1", "q2"],
      "consolidated_sql": "SELECT col1, col2, col3 FROM table1 WHERE (status = 'active') OR (created_date > '2023-01-01')",
      "estimated_memory": "2g",
      "estimated_runtime_sec": 45,
      "priority": 1,
      "deequ_checks": [
        {"column": "col1", "check": "isComplete"},
        {"column": "col2", "check": "hasDataType('string')"}
      ],
      "fallback": {
        "type": "individual_execution",
        "queries": ["q1", "q2"]
      }
    },
    {
      "group_id": "complex_table1_individual",
      "execution_type": "individual", 
      "queries": ["q5"],
      "estimated_memory": "4g",
      "estimated_runtime_sec": 180,
      "priority": 2,
      "reason_for_individual": "contains window functions and joins",
      "deequ_checks": [
        {"column": "region_count", "check": "isPositive"}
      ],
      "fallback": {
        "type": "skip",
        "reason": "too_resource_intensive"
      }
    }
  ],
  "fallback_strategy": "sequential_individual",
  "resource_constraints": {
    "max_concurrent_groups": 2,
    "memory_threshold_percent": 80
  }
}
```

### 3. Execution Plan → Spark Program

**Command:**
```bash
spark-submit dynamic_spark_executor.py --execution-plan execution_plan.json
```

**Spark Program Internal Processing:**
```python
# What the Spark program does internally:

# For group "simple_table1_consolidated":
df1 = spark.sql("SELECT col1, col2, col3 FROM table1 WHERE (status = 'active') OR (created_date > '2023-01-01')")
# Runs Deequ checks on df1

# For group "complex_table1_individual": 
df2 = spark.sql("SELECT t1.col1, COUNT(*) OVER (PARTITION BY t1.region) as region_count FROM table1 t1 JOIN table2 t2 ON t1.id = t2.ref_id WHERE t1.amount > 1000")
# Runs Deequ checks on df2
```

### 4. Spark Program → Deequ Results

**Output: `deequ_results.json`**
```json
{
  "execution_summary": {
    "total_groups_processed": 2,
    "successful_groups": 2,
    "failed_groups": 0,
    "fallbacks_triggered": 0,
    "total_runtime_sec": 225
  },
  "results": [
    {
      "group_id": "simple_table1_consolidated",
      "execution_type": "consolidated",
      "dataframe_rows": 25000,
      "dataframe_cols": 3,
      "deequ_results": {
        "check_status": "Success",
        "checks": [
          {"column": "col1", "check": "isComplete", "status": "passed", "completeness": 0.98},
          {"column": "col2", "check": "hasDataType('string')", "status": "passed"}
        ]
      },
      "execution_time_sec": 45,
      "memory_used_mb": 1800
    },
    {
      "group_id": "complex_table1_individual", 
      "execution_type": "individual",
      "queries_processed": ["q5"],
      "dataframe_rows": 50000,
      "dataframe_cols": 2,
      "deequ_results": {
        "check_status": "Warning",
        "checks": [
          {"column": "region_count", "check": "isPositive", "status": "warning", "violations": 5}
        ]
      },
      "execution_time_sec": 180,
      "memory_used_mb": 3200
    }
  ]
}
```

### 5. Shell Script Console Output

**Console Output During Execution:**
```bash
$ ./smart_spark_execution.sh prod

[2024-08-21 10:15:30] Analyzing queries in queries_config/...
[2024-08-21 10:15:32] Found 3 queries across 2 config files
[2024-08-21 10:15:33] ✅ Generated execution plan: mixed_execution strategy
[2024-08-21 10:15:33] 📊 Plan: 2 queries consolidated, 1 individual
[2024-08-21 10:15:33] 💾 Estimated memory: 6g (within 8g limit)

[2024-08-21 10:15:35] 🚀 Starting Spark execution...
[2024-08-21 10:15:40] ⚡ Group 1/2: simple_table1_consolidated (Priority 1)
[2024-08-21 10:16:25] ✅ Group 1 completed: 25K rows, 45sec
[2024-08-21 10:16:25] ⚡ Group 2/2: complex_table1_individual (Priority 2)  
[2024-08-21 10:19:05] ⚠️  Group 2 completed: 50K rows, 180sec (warnings detected)

[2024-08-21 10:19:05] 📈 Execution Summary:
- Total runtime: 225 seconds
- Peak memory: 3.2GB / 8GB
- Fallbacks triggered: 0
- Data quality: 2 passed, 1 warning

[2024-08-21 10:19:05] 📄 Results saved to: deequ_results.json
[2024-08-21 10:19:05] ✅ Execution completed successfully
```

**Error Scenario Output:**
```bash
[2024-08-21 10:20:15] ❌ Memory pressure detected: 85% usage
[2024-08-21 10:20:15] 🔄 Triggering fallback: switching to sequential individual execution
[2024-08-21 10:20:16] ♻️  Restarting with fallback execution plan...
[2024-08-21 10:23:45] ✅ Fallback execution completed: 380 seconds total
```

---

## Complete Solution Summary

**What we're doing:** Creating a smart, configuration-driven system that automatically groups SQL queries, creates optimal DataFrames, and handles fallbacks - all orchestrated by shell scripts without code changes.

### Core Components to Build

#### 1. **SQL Consolidator/Analyzer** (Enhanced from before)
- **File:** `query_analyzer.py`
- **Purpose:** Analyzes queries, groups them intelligently, generates execution plans
- **Input:** Query config files
- **Output:** `execution_plan.json` with grouping strategy

#### 2. **Query Configuration Files**
- **Structure:** `queries_config/table1_simple.json`, `table2_complex.json`, etc.
- **Content:** Query definitions + metadata (complexity hints, resource needs)

#### 3. **Dynamic Spark Program** 
- **File:** `dynamic_spark_executor.py`
- **Purpose:** Generic executor that reads execution plan and creates DataFrames accordingly
- **Features:** Handles consolidated queries, individual queries, and fallbacks

#### 4. **Orchestration Shell Script**
- **File:** `smart_spark_execution.sh`
- **Purpose:** Runs analyzer → submits Spark job → monitors → handles failures
- **Features:** Adaptive resource allocation, fallback triggers

#### 5. **Execution Plan Schema**
- **File:** `execution_plan.json` (generated)
- **Content:** Which queries to consolidate, which to run individually, fallback strategies

### Workflow:
```bash
Query Configs → Analyzer → Execution Plan → Spark Program → Deequ Results
     ↓              ↓           ↓              ↓           ↓
   JSON files   Groups &    DataFrame      Adaptive    Data Quality
                Optimizes   Creation       Execution     Reports
```

### Key Benefits:
- **Add new queries:** Just update config files, no code changes
- **Automatic optimization:** System decides best DataFrame strategy
- **Robust fallbacks:** Handles memory/complexity issues automatically
- **Shell-script driven:** Fits your current deployment approach

**You still need the SQL consolidator** - but now it's part of a larger intelligent system that knows when NOT to consolidate and what to do instead.

---

## Integration with AWS Deequ

Here's how to use the consolidated query with Deequ:

```python
from pyspark.sql import SparkSession
from pydeequ import Check, CheckLevel, VerificationSuite

# Use the consolidator
consolidator = SQLConsolidator()
queries = ["SELECT col1,col2 FROM table1", "SELECT col3,col1 FROM table1"]
consolidated_sql = consolidator.consolidate_queries(queries)

# Create Spark DataFrame
spark = SparkSession.builder.appName("DeequValidation").getOrCreate()
df = spark.sql(consolidated_sql)

# Use with Deequ
check = Check(spark, CheckLevel.Warning, "Data Quality Check")
result = VerificationSuite(spark).onData(df).addCheck(check).run()
```

---

## Alternative Libraries for Non-AI Approach

### 1. **SQLGlot** - Advanced SQL parser and transpiler:
```python
import sqlglot

def consolidate_with_sqlglot(queries):
    parsed_queries = [sqlglot.parse_one(query) for query in queries]
    # Extract and merge SELECT clauses
```

### 2. **Mo-SQL-Parsing** - Mozilla's SQL parser:
```python
from moz_sql_parser import parse

def consolidate_with_moz(queries):
    parsed = [parse(query) for query in queries]
    # Merge parsed structures
```

---

## Monitoring and Adaptive Fallback

### Add monitoring to your shell script:

```bash
# Monitor execution progress
SPARK_PID=$!
sleep 5

while kill -0 $SPARK_PID 2>/dev/null; do
    # Check memory usage
    MEMORY_USAGE=$(ps -p $SPARK_PID -o %mem --no-headers)
    if (( $(echo "$MEMORY_USAGE > 80" | bc -l) )); then
        echo "High memory usage detected: $MEMORY_USAGE%"
        # Could trigger adaptive scaling or kill and restart with different config
    fi
    sleep 10
done

wait $SPARK_PID
EXIT_CODE=$?

# Handle different exit scenarios
case $EXIT_CODE in
    0) echo "Execution completed successfully" ;;
    1) echo "Out of memory error, retrying with individual execution"
       retry_with_fallback ;;
    2) echo "Query timeout, skipping complex queries"
       retry_with_simplified_plan ;;
    *) echo "Unknown error, using emergency mode"
       emergency_execution ;;
esac
```

---

## Configuration Management

### Environment-specific configs:

```bash
# config/
├── dev/
│   ├── spark_conf.properties
│   └── resource_limits.json
├── staging/
│   └── ...
└── prod/
    └── ...

# Load environment-specific settings
ENV=${1:-dev}
source config/${ENV}/spark_conf.properties
```

---

## Final Recommendations

### For Simple Queries (70-80% of cases):
- Use the **basic SQL consolidator**
- Consolidate 3-5 similar queries per DataFrame
- Apply Spark-specific optimizations

### For Complex Queries:
- Use the **advanced analyzer** to detect complexity
- Group by execution patterns, not just table names
- Fall back to individual DataFrames for very complex queries
- Use **UNION ALL** only as last resort

### For Production Deployment:
- Implement the **configuration-driven approach**
- Use **shell script orchestration** for robustness
- Add **monitoring and fallback strategies**
- Test with your actual query patterns

### Architecture Benefits:
1. **Zero code changes** for new query patterns
2. **Automatic optimization** based on current queries  
3. **Robust fallback handling** without manual intervention
4. **Environment adaptability** (dev/staging/prod)
5. **Monitoring and alerting** built into the workflow
6. **Resource-aware execution** based on cluster capacity

**You just run:** `./smart_spark_execution.sh prod` and everything adapts dynamically!

---

## Getting Started

1. **Start with the basic SQL consolidator** for your simple queries
2. **Identify your complex query patterns** and test consolidation feasibility
3. **Implement the configuration structure** for your specific queries
4. **Build the shell script orchestration** incrementally
5. **Add monitoring and fallback strategies** as you scale

This approach gives you a production-ready system that handles both simple consolidation and complex query scenarios while maintaining Spark efficiency and Deequ compatibility.