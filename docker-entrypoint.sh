#!/bin/sh
set -eu

# Do not start the application until both durable dependencies are reachable.
# The Python helper retries authentication as well as the network connection.
python /app/scripts/wait_for_dependencies.py

# Replacing the shell is important: SIGTERM can now reach the real service and
# give it time to finish or requeue in-flight work before Docker stops it.
exec "$@"
