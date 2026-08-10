import os
import sys

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from www.main import app, db
from www.models import Proxy

if __name__ == "__main__":
    with app.app_context():
        print("Resetting ALL proxy states to available (in_use=False)...")
        try:
            count = Proxy.query.update({Proxy.in_use: False})
            db.session.commit()
            print(f"✅ Successfully reset {count} proxies.")
        except Exception as e:
            print(f"❌ Error resetting proxies: {e}")
            db.session.rollback()
