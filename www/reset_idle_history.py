import sys
import os

# Get the absolute path of the current directory (www)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Get the parent directory (kw-scraper)
parent_dir = os.path.dirname(current_dir)

# Add parent directory to sys.path
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    from www.main import app, db
    from www.models import IdleTasks, IdleStatus

    with app.app_context():
        # Reset IdleStatus
        print("Resetting IdleStatus...")
        status = IdleStatus.get_singleton()
        status.active = False
        status.current_code = None
        status.current_number = None
        status.empty_streak = 0
        db.session.commit()

        # Clear ALL IdleTasks
        count = IdleTasks.query.count()
        if count > 0:
            print(f"Deleting {count} idle task records (history and pending)...")
            IdleTasks.query.delete()
            db.session.commit()
            print("Successfully cleared all idle tasks. Scraper will restart from 'Informacje' max.")
        else:
            print("IdleTasks table is already empty.")

except Exception as e:
    print(f"Error resetting idle history: {e}")
    import traceback
    traceback.print_exc()
