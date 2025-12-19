#!/usr/bin/env python3
import re
import glob
import os

def fix_create_bundle_syntax(content):
    """Remove the 5th parameter (default value) from CREATE BUNDLE field definitions"""
    # Pattern: {"field", "TYPE", bool, bool, "default_value"}
    # Replace with: {"field", "TYPE", bool, bool}
    
    pattern = r'\{"([^"]+)",\s*"([^"]+)",\s*(true|false),\s*(true|false),\s*"[^"]*"\}'
    replacement = r'{"\1", "\2", \3, \4}'
    
    content = re.sub(pattern, replacement, content)
    
    # Also handle numeric defaults: {"field", "TYPE", bool, bool, number}
    pattern2 = r'\{"([^"]+)",\s*"([^"]+)",\s*(true|false),\s*(true|false),\s*[0-9.]+\}'
    replacement2 = r'{"\1", "\2", \3, \4}'
    
    content = re.sub(pattern2, replacement2, content)
    
    return content

# Find all test Go files
test_files = glob.glob('src/cmd/tests/**/*.go', recursive=True)

print(f"Found {len(test_files)} test files\n")

updated_count = 0
for filepath in test_files:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            original = f.read()
        
        updated = fix_create_bundle_syntax(original)
        
        if updated != original:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(updated)
            print(f"✓ Updated: {filepath}")
            updated_count += 1
        
    except Exception as e:
        print(f"✗ Error processing {filepath}: {e}")

print(f"\n✓ Complete: Updated {updated_count} of {len(test_files)} files")
