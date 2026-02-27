import subprocess
import sys
import os
import time

def run_script(script_name):
    print(f"[Run] {script_name}...")
    try:
        # Use the same python interpreter
        result = subprocess.run([sys.executable, script_name], 
                                capture_output=True, 
                                text=True, 
                                check=True,
                                cwd=os.path.dirname(os.path.abspath(__file__)))
        print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"[Error] {script_name}:")
        print(e.stderr)
        return False

def main():
    scripts = [
        "A_export_users.py",
        "B_export_user_type.py",
        "C_export_stocks.py",
        "D_export_price_snapshots.py",
        "E_export_analysis_signals.py",
        "F_export_recommendations.py",
        "G_export_newsletters.py",
        "H_export_stock_fundamentals.py",
        "I_export_investor_trends.py"
    ]
    
    start_time = time.time()
    print("=== Starting Full Outbound Sync (Parallel) ===")
    
    from concurrent.futures import ThreadPoolExecutor
    
    success_count = 0
    with ThreadPoolExecutor(max_workers=len(scripts)) as executor:
        results = list(executor.map(run_script, scripts))
        success_count = sum(results)
            
    end_time = time.time()
    print(f"=== Sync Completed in {end_time - start_time:.2f}s ===")
    print(f"DONE: Successfully ran {success_count}/{len(scripts)} scripts.")

if __name__ == "__main__":
    main()
