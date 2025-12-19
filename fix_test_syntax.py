#!/usr/bin/env python3
"""
Script to fix SyndrQL syntax in test files:
1. CREATE BUNDLE: Remove 5th parameter (default value) -> 4 parameters
2. ADD DOCUMENT: Change from multi-object to single-object format
"""

import re
import os
import glob

def fix_create_bundle(content):
    """Fix CREATE BUNDLE syntax from 5 params to 4 params"""
    # Pattern: {"field", "TYPE", bool, bool, "default"} -> {"field", "TYPE", bool, bool}
    # We need to remove the 5th parameter (default value)
    pattern = r'\{"([^"]+)",\s*"([^"]+)",\s*(true|false),\s*(true|false),\s*"[^"]*"\}'
    replacement = r'{"\1", "\2", \3, \4}'
    return re.sub(pattern, replacement, content)

def fix_add_document(content):
    """Fix ADD DOCUMENT syntax from multi-object to single-object format"""
    # This is more complex - we need to find ADD DOCUMENT commands and convert them
    # Pattern: WITH ({X}, {Y}, {Z}) -> WITH ({X, Y, Z})
    
    # Find all ADD DOCUMENT ... WITH ( patterns
    lines = content.split('\n')
    result = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Check if this line contains ADD DOCUMENT TO BUNDLE
        if 'ADD DOCUMENT TO BUNDLE' in line and 'WITH' in line:
            # Try to capture the entire command (might span multiple lines)
            command_lines = [line]
            j = i + 1
            
            # Keep adding lines until we find the closing semicolon or reach a reasonable limit
            while j < len(lines) and j < i + 10:
                command_lines.append(lines[j])
                if ');' in lines[j]:
                    break
                j += 1
            
            # Join the command lines
            full_command = '\n'.join(command_lines)
            
            # Check if it's the old format (multiple objects with {"key"=value})
            if re.search(r'WITH\s*\(\s*\{"[^"]+"\s*=', full_command):
                # Extract field assignments
                # Pattern: {"key"=value} or {"key"="value"}
                field_pattern = r'\{"([^"]+)"\s*=\s*([^,}]+)\}'
                matches = re.findall(field_pattern, full_command)
                
                if matches:
                    # Build new format: {key=value, key2=value2}
                    new_fields = ', '.join([f'{key}={value.strip()}' for key, value in matches])
                    
                    # Replace the old WITH clause with new format
                    # Find the WITH ( ... ) part
                    with_pattern = r'WITH\s*\([^)]+\)'
                    new_with = f'WITH ({{{new_fields}}})'
                    full_command = re.sub(with_pattern, new_with, full_command, count=1)
                
                # Add the fixed command
                result.extend(full_command.split('\n'))
                i = j + 1
                continue
        
        result.append(line)
        i += 1
    
    return '\n'.join(result)

def process_file(filepath):
    """Process a single test file"""
    print(f"Processing: {filepath}")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply fixes
        content = fix_create_bundle(content)
        content = fix_add_document(content)
        
        # Only write if something changed
        if content != original_content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"  ✓ Updated: {filepath}")
            return True
        else:
            print(f"  - No changes needed: {filepath}")
            return False
            
    except Exception as e:
        print(f"  ✗ Error processing {filepath}: {e}")
        return False

def main():
    """Main function to process all test files"""
    test_dir = '/Users/danstrohschein/Documents/CodeProjects/golang/SyndrDB/src/cmd/tests'
    
    # Find all Go test files
    pattern = os.path.join(test_dir, '**', '*test*.go')
    test_files = glob.glob(pattern, recursive=True)
    
    print(f"Found {len(test_files)} test files\n")
    
    updated_count = 0
    for filepath in sorted(test_files):
        if process_file(filepath):
            updated_count += 1
    
    print(f"\n✓ Complete: Updated {updated_count} of {len(test_files)} files")

if __name__ == '__main__':
    main()
