#!/usr/bin/env python
from flask import Flask
from flask_migrate import Migrate
from www.main import app, db
import os
# Initialize Flask-Migrate
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MIGRATIONS_DIR = os.path.join(BASE_DIR, "migrations")

migrate = Migrate(app, db, directory=MIGRATIONS_DIR)

# The Flask CLI will automatically add the 'db' command when Flask-Migrate is initialized
# No need for Flask-Script anymore as it's deprecated

if __name__ == '__main__':
    # This script is now mostly for backwards compatibility
    # The preferred way to run commands is now using 'flask db <command>'
    print("This script is deprecated. Please use 'flask db <command>' instead.")
    print("Example: flask db init")
    print("Example: flask db migrate -m 'Initial migration'")
    print("Example: flask db upgrade")
