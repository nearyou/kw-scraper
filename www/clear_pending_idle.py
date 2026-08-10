import sys
import os

# Get the absolute path of the current directory (www)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory (kw-scraper)
parent_dir = os.path.dirname(current_dir)

# Add parent directory to sys.path so we can import 'www' as a package
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    # Import everything from the 'www' package to ensure we use the SAME instance
    # of SQLAlchemy that is initialized in main.py
    from www.main import app, db
    from www.models import IdleTasks

    with app.app_context():
        # Count first
        pending_count = IdleTasks.query.filter_by(status='pending').count()
        if pending_count > 0:
            print(f"Found {pending_count} pending idle tasks. Deleting them...")
            IdleTasks.query.filter_by(status='pending').delete()
            db.session.commit()
            print("Successfully cleared pending queue.")
        else:
            print("No pending idle tasks found. Queue is clean.")

except Exception as e:
    print(f"Error executing cleanup: {e}")
    import traceback
    traceback.print_exc()
