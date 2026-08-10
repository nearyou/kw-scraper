import os
import sys
import requests
import random
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker


# Add parent directory to path to import models
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

# Add parent directory to path to import models
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(parent_dir)

# Manual .env loading to avoid dependency
env_path = os.path.join(parent_dir, '.env')
if os.path.exists(env_path):
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip().strip("'").strip('"')

from www.models import Proxy

def main():
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        print("Error: DATABASE_URL not found in environment variables.")
        return

    print(f"Connecting to database...")
    engine = create_engine(db_url)
    Session = scoped_session(sessionmaker(bind=engine))
    session = Session()

    try:
        # Get all proxies
        proxies = session.query(Proxy).all()
        if not proxies:
            print("No proxies found in database.")
            return

        # Pick a random proxy
        proxy = random.choice(proxies)
        print(f"Selected proxy: {proxy.host}:{proxy.port} (User: {proxy.username})")

        # Prepare Splash request
        splash_url = 'http://localhost:8050/execute'
        
        # Load Lua script
        lua_path = os.path.join(parent_dir, 'scraping_functions', 'debug_extractor.lua')
        try:
            with open(lua_path, 'r', encoding='utf-8') as f:
                lua_script = f.read()
        except FileNotFoundError:
            print(f"Error: Could not find lua script at {lua_path}")
            return

        # Prepare proxy string
        if proxy.is_direct:
            proxy_url = None # Splash handles no proxy if we don't pass it, or we might need to handle direct differently
            # Usually direct means no proxy argument to splash
            print("Using direct connection (no proxy)")
        else:
            proxy_url = f"http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
            print(f"Using proxy URL: {proxy_url}")

        payload = {
            'lua_source': lua_script,
            'timeout': 90,
            'proxy_url': proxy_url if proxy_url else ''
        }
        
        # If proxy provided, pass it to Lua args as well just in case, 
        # but primarily Splash uses the 'proxy' argument in requests/or request_headers if configured in lua.
        # However, standard Splash /execute endpoint accepts 'proxy' parameter to set default proxy.
        # But in our custom Lua, we might not be using the global proxy unless we set it.
        # Let's check how the original scraper was doing it.
        # Original scraper seemed to use `proxies` dict in python `requests` calls to Splash? 
        # No, scraper.py lines 740+ shows it uses `proxies` dict to connect TO Splash (if splash is behind proxy? No)
        # Wait, scraper.py line 737: `proxy_string = ...`. 
        # Then `extract_data_native` uses python requests.
        # `extract_data_with_splash` (which I didn't read fully but logically exists) likely passes proxy info to Splash.
        # Let's simple pass it as an argument 'proxy_url' to Lua and let Lua handle or Splash handle.
        # Actually, in /execute, you can pass 'proxy' argument.
        
        if proxy_url:
            payload['proxy'] = proxy_url

        print("Sending request to Splash...")
        try:
            response = requests.post(splash_url, json=payload, timeout=100)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Error communicating with Splash: {e}")
            return

        if 'success' in data and data['success'] == '1':
            print("SUCCESS: The proxy works and the search page loaded correctly.")
        else:
            print("FAILURE: The search page did not load correctly.")
            code = data.get('code', 'unknown')
            print(f"Error code: {code}")
            
            if 'html' in data:
                filename = f"debug_failure_{proxy.host}_{int(time.time())}.html"
                filepath = os.path.join(parent_dir, filename)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(data['html'])
                print(f"Saved HTML dump to: {filepath}")
            else:
                print("No HTML content returned in response.")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()
