import os
import sys
from main import app, run_idle_alphabetical_scraping

# Ensure we are in the correct directory context
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def manual_run():
    print("Starting manual idle scraping run...")
    with app.app_context():
        try:
            run_idle_alphabetical_scraping()
            print("Idle scraping run completed successfully.")
        except Exception as e:
            print(f"Error executing idle scraping: {e}")

if __name__ == "__main__":
    manual_run()
