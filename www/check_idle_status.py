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
    from www.main import app, db
    from www.models import IdleTasks, IdleStatus, Informacje
    from sqlalchemy import func

    with app.app_context():
        print("=== Idle Scraper Diagnostic ===")
        
        # 1. Check IdleStatus
        status = IdleStatus.get_singleton()
        print(f"Global Idle Status: Active={status.active}, Code={status.current_code}, Number={status.current_number}, Streak={status.empty_streak}")
        
        # 2. Check Pending/In-Progress Tasks
        pending = IdleTasks.query.filter_by(status='pending').all()
        in_progress = IdleTasks.query.filter_by(status='in_progress').all()
        
        print(f"\nPending Tasks: {len(pending)}")
        for t in pending[:5]:
            print(f" - [{t.id}] {t.kod_wydzialu}: {t.start_from}-{t.end_at} (Created: {t.created_at})")
            
        print(f"\nIn-Progress Tasks: {len(in_progress)}")
        for t in in_progress:
            print(f" - [{t.id}] {t.kod_wydzialu}: {t.start_from}-{t.end_at} (Started: {t.started_at})")
            
        # 3. Check Sticky Logic (Last attempted ranges)
        if status.current_code:
            code = status.current_code
            print(f"\nCurrent 'Sticky' Department: {code}")
            
            # Max from Informacje
            max_info = db.session.query(func.max(Informacje.ksiega)).filter(Informacje.ksiega.like(f"{code}-%")).scalar()
            print(f" - Max in Informacje: {max_info}")
            
            # Max from IdleTasks
            max_idle = db.session.query(func.max(IdleTasks.end_at)).filter_by(kod_wydzialu=code).scalar()
            print(f" - Max Attempted in IdleTasks: {max_idle}")

except Exception as e:
    print(f"Error executing diagnostic: {e}")
    import traceback
    traceback.print_exc()
