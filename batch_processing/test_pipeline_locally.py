"""
Local testing script for the batch data lake pipeline
Run this script to test the entire pipeline locally before deploying to Airflow
"""
import os
import sys
import subprocess
from datetime import datetime

def run_spark_job(script_path):
    """
    Run a Spark job locally
    """
    print(f"\n{'='*60}")
    print(f"Running: {script_path}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run([
            sys.executable, script_path
        ], capture_output=True, text=True, check=True)
        
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
            
        print(f"✅ Successfully completed: {script_path}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running {script_path}:")
        print(f"Return code: {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return False

def cleanup_data_directories():
    """
    Clean up data directories before testing
    """
    import shutil
    
    data_dirs = ["data/landing", "data/bronze", "data/silver", "data/gold"]
    
    for data_dir in data_dirs:
        if os.path.exists(data_dir):
            print(f"Cleaning up directory: {data_dir}")
            shutil.rmtree(data_dir)

def main():
    """
    Main function to test the entire pipeline
    """
    print("🚀 Starting local pipeline testing...")
    print(f"Test started at: {datetime.now()}")
    
    # Clean up previous runs
    cleanup_data_directories()
    
    # Define the pipeline steps
    pipeline_steps = [
        "batch_processing/landing_to_bronze.py",
        "batch_processing/bronze_to_silver.py",
        "batch_processing/silver_to_gold.py"
    ]
    
    # Run each step
    success_count = 0
    for step in pipeline_steps:
        if run_spark_job(step):
            success_count += 1
        else:
            print(f"❌ Pipeline failed at step: {step}")
            break
    
    # Summary
    print(f"\n{'='*60}")
    print("PIPELINE TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Total steps: {len(pipeline_steps)}")
    print(f"Successful steps: {success_count}")
    print(f"Test completed at: {datetime.now()}")
    
    if success_count == len(pipeline_steps):
        print("🎉 All pipeline steps completed successfully!")
        
        # Show final results
        print("\n📊 Final data structure:")
        for root, dirs, files in os.walk("data"):
            level = root.replace("data", "").count(os.sep)
            indent = " " * 2 * level
            print(f"{indent}{os.path.basename(root)}/")
            subindent = " " * 2 * (level + 1)
            for file in files:
                print(f"{subindent}{file}")
    else:
        print("❌ Pipeline testing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()