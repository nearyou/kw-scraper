import os
from flask import Flask
from www.models import db
from dotenv import load_dotenv
from logging_config import configure_logging

load_dotenv()
configure_logging(service="database-init", capture_streams=True)

app = Flask(__name__)
# Ensure DATABASE_URL is set
db_url = os.getenv('DATABASE_URL')
if not db_url:
    print("DATABASE_URL not set!")
    exit(1)

app.config['SQLALCHEMY_DATABASE_URI'] = db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)

with app.app_context():
    # The full URL contains credentials and must not be stored in logs.
    print("Connecting to configured database")
    print("Creating tables...")
    try:
        db.create_all()
        print("Tables created successfully.")
    except Exception as e:
        print(f"Error creating tables: {e}")
