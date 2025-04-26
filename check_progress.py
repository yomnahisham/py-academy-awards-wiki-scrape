#!/usr/bin/env python3
"""
Check the progress of fix_remaining.py
"""

import os
import time
import subprocess
import sys

# Define colors for better output
class Colors:
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

def check_running():
    """Check if fix_remaining.py is running"""
    result = subprocess.run("ps aux | grep -i fix_remaining.py | grep -v grep | grep -v check_progress.py", 
                          shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return len(result.stdout) > 0

def get_interim_file_stats():
    """Get stats on the interim file"""
    interim_file = 'data/persons_fixed.csv.interim'
    if not os.path.exists(interim_file):
        return {"exists": False}
    
    file_size = os.path.getsize(interim_file)
    mod_time = os.path.getmtime(interim_file)
    mod_time_str = time.strftime('%H:%M:%S', time.localtime(mod_time))
    
    with open(interim_file, 'r') as f:
        line_count = sum(1 for _ in f)
    
    return {
        "exists": True,
        "size": file_size,
        "modified": mod_time_str,
        "lines": line_count - 1  # Subtract header
    }

def get_log_progress():
    """Get progress from the log file"""
    log_file = 'fix_persons.log'
    if not os.path.exists(log_file):
        return {"exists": False}
    
    # Get the last 100 lines from the log file
    result = subprocess.run(f"tail -100 {log_file}", shell=True, 
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    # Look for progress lines
    progress_line = None
    chunk_line = None
    
    for line in reversed(result.stdout.splitlines()):
        if "Processed " in line and "%" in line and "Updated:" in line:
            progress_line = line
            break
    
    for line in reversed(result.stdout.splitlines()):
        if "Processing chunk " in line:
            chunk_line = line
            break
    
    return {
        "exists": True,
        "progress_line": progress_line,
        "chunk_line": chunk_line
    }

def get_output_file_stats():
    """Get stats on the output file"""
    output_file = 'data/persons_fixed.csv'
    if not os.path.exists(output_file):
        return {"exists": False}
    
    file_size = os.path.getsize(output_file)
    mod_time = os.path.getmtime(output_file)
    mod_time_str = time.strftime('%H:%M:%S', time.localtime(mod_time))
    
    with open(output_file, 'r') as f:
        line_count = sum(1 for _ in f)
    
    return {
        "exists": True,
        "size": file_size,
        "modified": mod_time_str,
        "lines": line_count - 1  # Subtract header
    }

def display_progress():
    """Display progress information"""
    is_running = check_running()
    interim_stats = get_interim_file_stats()
    log_progress = get_log_progress()
    output_stats = get_output_file_stats()
    
    print(f"\n{Colors.BLUE}=== fix_remaining.py Progress Check ==={Colors.RESET}")
    
    # Check if process is running
    if is_running:
        print(f"{Colors.GREEN}✓ Process is running{Colors.RESET}")
    else:
        print(f"{Colors.RED}✗ Process is NOT running{Colors.RESET}")
        
        # If process isn't running, check if output file exists as an indicator of completion
        if output_stats["exists"]:
            print(f"{Colors.GREEN}✓ Output file exists - process may have completed successfully{Colors.RESET}")
        else:
            print(f"{Colors.RED}✗ Output file doesn't exist - process may have failed{Colors.RESET}")
    
    # Show progress from log if available
    if log_progress["exists"]:
        if log_progress["progress_line"]:
            print(f"\n{Colors.YELLOW}Progress:{Colors.RESET} {log_progress['progress_line']}")
        if log_progress["chunk_line"]:
            print(f"{Colors.YELLOW}Current chunk:{Colors.RESET} {log_progress['chunk_line']}")
    
    # Show interim file stats if available
    if interim_stats["exists"]:
        print(f"\n{Colors.YELLOW}Interim file:{Colors.RESET}")
        print(f"  - Last modified: {interim_stats['modified']}")
        print(f"  - Size: {interim_stats['size'] / 1024:.2f} KB")
        print(f"  - Records: {interim_stats['lines']}")
    else:
        print(f"\n{Colors.YELLOW}Interim file:{Colors.RESET} Not found")
    
    # Show output file stats if available
    if output_stats["exists"]:
        print(f"\n{Colors.YELLOW}Output file:{Colors.RESET}")
        print(f"  - Last modified: {output_stats['modified']}")
        print(f"  - Size: {output_stats['size'] / 1024:.2f} KB")
        print(f"  - Records: {output_stats['lines']}")
    else:
        print(f"\n{Colors.YELLOW}Output file:{Colors.RESET} Not found yet")
    
    print(f"\n{Colors.BLUE}==================================={Colors.RESET}\n")

if __name__ == "__main__":
    display_progress() 