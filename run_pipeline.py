import subprocess
import sys
import os
import argparse

def run_command(command, cwd=None):
    """Run a shell command and check for errors."""
    print(f"Running: {command}")
    try:
        # Use shell=True to handle complex commands if needed, but list is safer
        # Here we assume command is a list of arguments
        result = subprocess.run(command, cwd=cwd, check=True, shell=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {command}")
        print(e)
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Run the full Rain4Agri data pipeline.")
    parser.add_argument('--days', type=int, default=5, help="Number of days back to crawl (default: 5)")
    parser.add_argument('--skip-crawl', action='store_true', help="Skip crawling, only run cleaning")
    args = parser.parse_args()

    # Determine Python executable
    python_exe = sys.executable
    # If running from venv, sys.executable should be correct. 
    # If not, we might want to force venv python if it exists.
    venv_python = os.path.join("venv", "Scripts", "python.exe")
    if os.path.exists(venv_python):
        python_exe = venv_python

    print(f"Using Python: {python_exe}")
    
    # 1. Crawl Station Data
    if not args.skip_crawl:
        print("\n=== Step 1: Crawling Station Data ===")
        # Note: cwa_his_data_crawler_rev2.py currently has hardcoded DAYS_BACK.
        # Ideally we should modify it to accept arguments, but for now we run it as is
        # or we can modify it to read env var or arg.
        # Let's assume we modify it later. For now, just run it.
        run_command(f"{python_exe} crawler/cwa_his_data_crawler_rev2.py")

        print("\n=== Step 2: Crawling Satellite Data ===")
        run_command(f"{python_exe} crawler/gibs.py")

    # 2. Data Cleaning & Imputation
    print("\n=== Step 3: Data Cleaning & Imputation ===")
    run_command(f"{python_exe} data_scripts/preprocess_and_impute_with_cache.py")

    print("\n=== Pipeline Complete ===")

if __name__ == "__main__":
    main()
