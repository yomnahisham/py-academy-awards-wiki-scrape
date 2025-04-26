#!/usr/bin/env python3
import os
import time
import sys

OUTPUT_FILE = 'data/persons_fixed.csv'

def monitor_output():
    """Monitor the output file for changes"""
    print(f"Monitoring {OUTPUT_FILE} for changes...")
    
    last_size = 0
    last_modified = 0
    
    if os.path.exists(OUTPUT_FILE):
        last_size = os.path.getsize(OUTPUT_FILE)
        last_modified = os.path.getmtime(OUTPUT_FILE)
        print(f"Initial size: {last_size} bytes")
        print(f"Last modified: {time.ctime(last_modified)}")
        
        # Show first 5 and last 5 lines of the file
        print("\nFile preview:")
        os.system(f"head -5 {OUTPUT_FILE}")
        print("...")
        os.system(f"tail -5 {OUTPUT_FILE}")
    else:
        print(f"File does not exist yet: {OUTPUT_FILE}")
    
    print("\nWaiting for changes (press Ctrl+C to stop)...")
    
    try:
        while True:
            time.sleep(5)  # Check every 5 seconds
            
            if os.path.exists(OUTPUT_FILE):
                current_size = os.path.getsize(OUTPUT_FILE)
                current_modified = os.path.getmtime(OUTPUT_FILE)
                
                if current_size != last_size:
                    print(f"\n[{time.ctime()}] File size changed: {last_size} -> {current_size} bytes (+{current_size - last_size} bytes)")
                    last_size = current_size
                
                if current_modified != last_modified:
                    print(f"[{time.ctime()}] File was modified")
                    last_modified = current_modified
                    
                    # Show the last 5 lines of the file
                    print("Last 5 lines:")
                    os.system(f"tail -5 {OUTPUT_FILE}")
                    
                    # Count the number of rows
                    with open(OUTPUT_FILE, 'r') as f:
                        row_count = sum(1 for _ in f)
                    print(f"Total rows: {row_count} (including header)")
            else:
                if last_size > 0:
                    print(f"\n[{time.ctime()}] Warning: File has been deleted!")
                    last_size = 0
                    last_modified = 0
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    monitor_output() 