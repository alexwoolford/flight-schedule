#!/usr/bin/env python3
"""
Fix DateTime queries after reloading graph with proper DateTime objects.
This script updates all queries to use native DateTime operations instead of epoch conversions.

Run this AFTER:
1. Manually clearing the database
2. Reloading with the fixed load_with_parallel_spark.py
3. Confirming datetime test passes
"""

import re
from pathlib import Path

def fix_datetime_queries():
    """Update all Cypher queries to use native DateTime operations"""
    
    files_to_fix = [
        "flight_search_demo.py",
        "README.md",
        "AGENTS.md"
    ]
    
    # Patterns to replace epoch conversions with native operations
    replacements = [
        # Date comparisons
        (r'date\(datetime\(\{epochmillis: ([^}]+) / 1000\}\)\)', r'date(\1)'),
        
        # Hour/time access
        (r'datetime\(\{epochmillis: ([^}]+) / 1000\}\)\.hour', r'\1.hour'),
        
        # DateTime assignments
        (r'datetime\(\{epochmillis: ([^}]+) / 1000\}\) AS (\w+)', r'\1 AS \2'),
        
        # Duration calculations (keep these as-is, they work with native DateTime)
        # duration.between() works with both epoch and native DateTime
    ]
    
    print("🔧 Fixing DateTime queries...")
    
    for file_path in files_to_fix:
        path = Path(file_path)
        if path.exists():
            print(f"   📝 Fixing {file_path}...")
            
            with open(path, 'r') as f:
                content = f.read()
            
            original_content = content
            
            # Apply all replacements
            for pattern, replacement in replacements:
                content = re.sub(pattern, replacement, content)
            
            if content != original_content:
                with open(path, 'w') as f:
                    f.write(content)
                print(f"      ✅ Updated {file_path}")
            else:
                print(f"      ℹ️  No changes needed in {file_path}")
        else:
            print(f"      ⚠️  File not found: {file_path}")
    
    print("\n✅ DateTime query fixes complete!")
    print("\nNext steps:")
    print("1. Clear database manually: MATCH (n) DETACH DELETE n")
    print("2. Reload data: python load_with_parallel_spark.py") 
    print("3. Test DateTime types: pytest tests/test_flight_search_unit.py::TestBasicFunctionality::test_datetime_types_in_graph -v")
    print("4. Run demo: python flight_search_demo.py")

if __name__ == "__main__":
    fix_datetime_queries()