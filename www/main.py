import os
import time
import subprocess
import psutil
import signal
import json
import tempfile
import gc  # Import garbage collector module
import threading
from flask import Flask, request, render_template, redirect, url_for, session, send_from_directory, send_file, make_response, jsonify, flash
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from flask_migrate import Migrate
from functools import wraps
import secrets
import string

# Fix imports to work in both direct execution and module import scenarios
import sys
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

try:
    # Try direct import first (for Celery worker)
    from www.models import db, Informacje, Wlasciciele, Notatki, Status, User, ScrapingProgress, TaskStatus, TaskQueue, UserPrefixPermission, get_user_by_login, get_user, Egzekucje, Hipoteki, Spadki, Dziedziczenia, Darowizny
except ImportError:
    # If that fails, try relative import (for direct script execution)
    from models import db, Informacje, Wlasciciele, Notatki, Status, User, ScrapingProgress, TaskStatus, TaskQueue, UserPrefixPermission, get_user_by_login, get_user, Egzekucje, Hipoteki, Spadki, Dziedziczenia, Darowizny

from celery import Celery, chain
from celery.backends.base import DisabledBackend
from celery.result import AsyncResult, current_app
from dotenv import load_dotenv
from sqlalchemy import and_, not_, exists
from celery.exceptions import MaxRetriesExceededError

import urllib, re, random
import tenacity  # Make sure we import tenacity for RetryError handling
from unidecode import unidecode

import sentry_sdk

load_dotenv()

# Import the cleaned, refactored kwscraper functions
from kwscraper import run_scraper, setup_scraper, download_worker, processing_done_event, cleanup_local_folder
from helper import get_formatted_book_number, get_control_digit

from department_codes import DEPARTMENT_CODES
from kwparser import parse_directory

# Configuration for workers
DOWNLOAD_WORKERS = int(os.getenv('DOWNLOAD_WORKERS', '1'))  # Default to 1 download worker
PARSING_WORKERS = int(os.getenv('PARSING_WORKERS', '1'))    # Default to 1 parsing worker
IDLE_SCRAPE_ENABLED = os.getenv('IDLE_SCRAPE_ENABLED', 'true').lower() in ('true', '1', 'yes', 'y')
IDLE_EMPTY_STREAK_LIMIT = int(os.getenv('IDLE_EMPTY_STREAK_LIMIT', '40000'))

# Maintenance mode check interval (in seconds)
MAINTENANCE_CHECK_INTERVAL = int(os.getenv('MAINTENANCE_CHECK_INTERVAL', '21600'))  # 6 hours default

sentry_sdk.init(
    dsn=os.getenv('SENTRY_DSN'),
    traces_sample_rate=1.0,
    _experiments={
        "continuous_profiling_auto_start": True,
    },
)

app = Flask(__name__)

DATABASE_URL = os.getenv('DATABASE_URL')
print(f"DEBUG - Actual DATABASE_URL being used: {DATABASE_URL}")
app.config['SQLALCHEMY_DATABASE_URI'] = f'{DATABASE_URL}?sslmode=disable'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    'pool_size': 10,           # Reduced from 20 to prevent connection exhaustion
    'max_overflow': 5,         # Reduced from 10
    'pool_timeout': 30,        # Connection acquisition timeout
    'pool_recycle': 300,       # 5 min (was 30 min) - more aggressive recycling
    'pool_pre_ping': True,     # KEY FIX: Verify connection is alive before using
}

db.init_app(app)
migrate = Migrate(app, db, directory='../migrations')

# Celery Configuration - read from environment variable for Docker compatibility
app.config['CELERY_BROKER_URL'] = os.getenv('CELERY_BROKER_URL', 'amqp://guest@localhost//')
app.config['CELERY_RESULT_BACKEND'] = 'rpc://'  # Use RPC backend for task results

# Initialize Celery after all configurations are set
def make_celery(app):
    celery = Celery(
        app.import_name,
        broker=app.config['CELERY_BROKER_URL'],
        backend=app.config['CELERY_RESULT_BACKEND']
    )

    # Explicitly set configurations after initialization
    celery.conf.update(
        broker_url=app.config['CELERY_BROKER_URL'],
        result_backend=app.config['CELERY_RESULT_BACKEND'],
        task_ignore_result=True,  # Don't store task results
        worker_prefetch_multiplier=1,  # Process one task at a time
        task_acks_late=True,  # Acknowledge tasks after they're completed
        task_track_started=False,  # Don't track when tasks are started
        task_send_sent_event=False,  # Don't send events when tasks are sent
        task_create_missing_queues=True,  # Create queues if they don't exist
        task_always_eager=False,  # Never execute tasks eagerly
        worker_disable_rate_limits=True,  # Disable rate limits
        broker_connection_retry=True,  # Retry connecting to broker if connection fails
        broker_connection_retry_on_startup=True,  # Retry connecting to broker on startup
        task_default_queue='new_tasks',  # Use a different queue for new tasks
        broker_transport_options={'visibility_timeout': 2592000},  # 30 days in seconds
        task_time_limit=None,  # No time limit on tasks
        task_reject_on_worker_lost=True,  # Requeue tasks if worker is lost
        task_acks_on_failure_or_timeout=False,  # Don't ack failed tasks
        broker_connection_max_retries=10,  # Retry connecting to broker up to 10 times
        broker_connection_timeout=10  # 10 second connection timeout
    )

    # Task class with Flask app context
    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery

# Create Celery instance after Flask app initialization
celery = make_celery(app)

# ---------------------------------------------------------------------------
# Cached Celery control RPC (inspect) — prevent 504s from blocking every HTTP
# request on synchronous inspector.active()/ping() calls. Celery control RPCs
# block for up to 1s per worker and are invoked on virtually every page render
# (context processor + /zadania). Under load / when the broker is saturated
# these calls exhaust the small gunicorn thread pool (2 workers x 2 threads)
# and produce 504 Gateway Time-out. Cache results for a short TTL instead.
# ---------------------------------------------------------------------------
_CELERY_INSPECT_CACHE = {'data': None, 'ts': 0.0, 'lock': threading.Lock()}
_CELERY_INSPECT_TTL = float(os.getenv('CELERY_INSPECT_TTL', '10'))


def get_celery_inspect_cached(timeout=1.0):
    """Return TTL-cached {'active': ..., 'ping': ...} from Celery inspect.

    Safe to call on every request. First call within the TTL window performs
    the blocking RPC; subsequent calls return the cached dict.
    """
    now = time.time()
    with _CELERY_INSPECT_CACHE['lock']:
        cached = _CELERY_INSPECT_CACHE['data']
        if cached is not None and (now - _CELERY_INSPECT_CACHE['ts']) < _CELERY_INSPECT_TTL:
            return cached
    # Perform the RPC outside the lock so concurrent requests don't serialize
    try:
        inspector = celery.control.inspect(timeout=timeout)
        active = inspector.active() or {}
        ping = inspector.ping() or {}
        result = {'active': active, 'ping': ping}
    except Exception as e:
        print(f"Celery inspect failed: {e}")
        result = {'active': {}, 'ping': {}}
    with _CELERY_INSPECT_CACHE['lock']:
        _CELERY_INSPECT_CACHE['data'] = result
        _CELERY_INSPECT_CACHE['ts'] = time.time()
    return result


def get_active_task_count_cached():
    """Count currently active Celery tasks using the cached inspect result."""
    data = get_celery_inspect_cached()
    count = 0
    for worker_tasks in data['active'].values():
        count += len(worker_tasks)
    return count

# Helper function to require admin access
def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not current_user.is_authenticated:
            return jsonify({'error': 'Authentication required'}), 401
        if not hasattr(current_user, 'is_admin') or not current_user.is_admin:
            return jsonify({'error': 'Admin privileges required'}), 403
        return f(*args, **kwargs)
    return decorated_function

@celery.task(name="scraping.scrape_task", bind=True, max_retries=3,
             acks_late=False, time_limit=3600, soft_time_limit=3300)
def scrape_task(self, department_code, start_from=0, end_at=999999, resume_from_progress=False):
    import concurrent.futures

    last_scraped_book = None
    task_status = TaskStatus.query.filter_by(task_id=self.request.id).first()

    try:
        # Set the task status to 'started' immediately
        if task_status:
            task_status.status = 'started'
            db.session.commit()
            print(f"Task {self.request.id} for {department_code} marked as started")
        # Note: This function is deprecated in favor of the new TaskQueue system
        # Progress tracking is now handled via TaskQueue.books_processed and ScrapingProgress.task_id
        print(f"Starting scraping for {department_code} from book {start_from}")

        # Initialize the scraper
        scraper = setup_scraper()

        try:
            # Reset the processing done event to False
            processing_done_event.clear()

            # Use the configurable worker counts
            print(f"Starting scraper with {DOWNLOAD_WORKERS} download workers")

            # Start download workers - parsing happens within each download worker now
            with concurrent.futures.ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as download_executor:
                # Bounded in-flight sliding window to avoid OOM when ranges are huge
                # (previously all futures for 0..999999 were submitted at once)
                IN_FLIGHT_LIMIT = int(os.getenv('IN_FLIGHT_LIMIT', max(12, DOWNLOAD_WORKERS * 2)))
                pending = {}
                book_iter = iter(range(start_from, end_at + 1))

                def _submit_next():
                    try:
                        n = next(book_iter)
                    except StopIteration:
                        return None
                    f = download_executor.submit(download_worker, scraper, department_code, n)
                    pending[f] = n
                    return f

                # Seed initial window
                for _ in range(IN_FLIGHT_LIMIT):
                    if _submit_next() is None:
                        break

                completed_books = []
                for future in concurrent.futures.as_completed(list(pending.keys())):
                    book_num = pending.pop(future)
                    try:
                        result = future.result()
                        if result:  # If download was successful
                            completed_books.append(book_num)
                            if not last_scraped_book or book_num > last_scraped_book:
                                last_scraped_book = book_num
                    except Exception as e:
                        print(f"Download failed for book {book_num}: {e}")

                    # Refill the window
                    _submit_next()

                # Signal that processing is done
                processing_done_event.set()

        finally:
            # Always clean up resources
            if scraper:
                scraper.close()
            gc.collect()  # Free up memory
        # Update ScrapingProgress table - Note: This should use task-specific progress
        # This function is deprecated in favor of TaskQueue system
        print(f"Scraping completed successfully for department {department_code}, last book: {last_scraped_book}")

    except Exception as e:        # Save progress before stopping due to error - Note: This should use task-specific progress
        # This function is deprecated in favor of TaskQueue system
        print(f"Error during scraping for department {department_code}: {str(e)}")

        # Retry the task if the max retries have not been exceeded
        try:
            self.retry(countdown=3, args=[department_code, start_from, end_at, True])
        except MaxRetriesExceededError:
            print(f"Max retries exceeded for department {department_code}. Ending the task.")

            # Avoid marking tasks as 'failed' — mark as 'stopped' for manual review
            if task_status:
                task_status.status = 'stopped'
                db.session.commit()

        raise


@celery.task(name="scraping.queue_manager", bind=True, max_retries=None,
             acks_late=False, reject_on_worker_lost=False)
def queue_manager_task(self):
    """
    Task queue manager that picks the next task from the queue and processes it.
    This task runs indefinitely, checking for new tasks and processing them.
    Only one queue manager should be running at a time.
    """
    print("Task queue manager started")
    # Singleton lock to ensure only one queue manager runs
    import fasteners
    lockfile = os.path.join(tempfile.gettempdir(), 'kw_queue_manager.lock')
    lock = fasteners.InterProcessLock(lockfile)
    if not lock.acquire(blocking=False):
        print("Another queue manager is already running. Exiting this instance.")
        return

    try:
        with app.app_context():
            # Mark all 'stopping' tasks as 'stopped'
            stopping_tasks = TaskQueue.query.filter_by(status='stopping').all()
            for task in stopping_tasks:
                print(f"Marking task {task.id} as stopped (was stopping)")
                task.status = 'stopped'
            # Mark all 'in_progress' tasks as 'pending'
            in_progress_tasks = TaskQueue.query.filter_by(status='in_progress').all()
            for task in in_progress_tasks:
                print(f"Marking task {task.id} as pending (was in_progress)")
                task.status = 'pending'
            # Also reset any stuck idle tasks to pending so they can be picked up again
            try:
                from www.models import IdleTasks, IdleStatus
                stuck_idle = IdleTasks.query.filter_by(status='in_progress').all()
                for it in stuck_idle:
                    print(f"Re-queueing stuck idle task {it.id} ({it.kod_wydzialu}) from in_progress -> pending")
                    it.status = 'pending'
                    it.started_at = None
                # Ensure idle status isn't falsely marked active on startup
                try:
                    idle_status = IdleStatus.get_singleton()
                    idle_status.active = False
                except Exception:
                    pass
            except Exception as e:
                print(f"Idle tasks reset error: {e}")
            db.session.commit()

        while True:
            with app.app_context():
                # Check maintenance mode and handle periodic health checks
                from www.models import IdleStatus
                import datetime
                
                idle_status = IdleStatus.get_singleton()
                
                # If in maintenance mode, check if it's time for a health check
                if idle_status.maintenance_mode:
                    current_time = datetime.datetime.utcnow()
                    if (idle_status.last_maintenance_check is None or 
                        (current_time - idle_status.last_maintenance_check).total_seconds() >= MAINTENANCE_CHECK_INTERVAL):
                        
                        print("Performing maintenance mode health check...")
                        try:
                            scraper = setup_scraper()
                            site_healthy = scraper.check_site_health()
                            scraper.close()
                            
                            if site_healthy:
                                print("Site is back online! Exiting maintenance mode.")
                                idle_status.maintenance_mode = False
                                idle_status.last_maintenance_check = current_time
                                db.session.commit()
                            else:
                                print("Site still in maintenance mode.")
                                idle_status.last_maintenance_check = current_time
                                db.session.commit()
                                
                        except Exception as e:
                            print(f"Error during health check: {e}")
                            idle_status.last_maintenance_check = current_time
                            db.session.commit()
                    
                    # If still in maintenance mode, skip all scraping
                    if idle_status.maintenance_mode:
                        print("Maintenance mode active - skipping all scraping activities")
                        time.sleep(60)  # Wait 1 minute before next check
                        continue
                
                # Get the next pending task from the queue
                next_task = TaskQueue.get_next_pending_task()
                
                # Check if there are any stopping tasks that need to be marked as stopped
                stopping_tasks = TaskQueue.query.filter_by(status='stopping').all()
                if stopping_tasks:
                    print(f"Found {len(stopping_tasks)} stopping tasks, waiting for them to complete...")
                    time.sleep(5)  # Give stopping tasks time to finish
                    continue
                
                if next_task:
                    print(f"Found pending task: {next_task}")
                    # Mark the task as in progress
                    next_task.status = 'in_progress'
                    next_task.books_total = next_task.end_at - next_task.start_from + 1
                    db.session.commit()
                    try:
                        # Use a single pool of DOWNLOAD_WORKERS for this task
                        # All workers share the same book queue for this task
                        scrape_task_direct(
                            next_task.id,
                            next_task.department_code, 
                            next_task.start_from, 
                            next_task.end_at
                        )
                        # The task status is already updated inside scrape_task_direct
                        print(f"Task {next_task.id} processing completed")
                    except Exception as e:
                        print(f"Error during task {next_task.id}: {str(e)}")
                        # Get fresh task instance in case of error
                        error_task = TaskQueue.query.get(next_task.id)
                        if error_task:
                            # Do not mark regular queue tasks as 'failed' automatically
                            error_task.status = 'stopped'
                            db.session.commit()
                else:
                    # No regular tasks - check for idle tasks
                    from www.models import IdleTasks
                    next_idle_task = IdleTasks.get_next_pending_task()
                    if next_idle_task:
                        print(f"Found pending idle task: {next_idle_task}")
                        try:
                            scrape_idle_task(next_idle_task.id)
                        except Exception as e:
                            print(f"Error during idle task {next_idle_task.id}: {str(e)}")
                    else:
                        # No tasks at all - try to create more idle tasks if enabled
                        if IDLE_SCRAPE_ENABLED:
                            # Rate limit log message - only once per minute
                            import time as time_module
                            _cache_attr = '_last_idle_log'
                            _last = getattr(queue_manager_task, _cache_attr, 0)
                            _now = time_module.time()
                            if _now - _last > 60:
                                print("No pending tasks or idle tasks. Running idle alphabetical scraping...")
                                setattr(queue_manager_task, _cache_attr, _now)
                            try:
                                run_idle_alphabetical_scraping()
                            except Exception as e:
                                print(f"Idle scraping error: {e}")
                            # Sleep after checking idle tasks to prevent CPU spinning
                            time.sleep(10)
                        else:
                            print("No pending tasks found, sleeping for 10 seconds...")
                            time.sleep(10)
                    gc.collect()  # Free up memory
    finally:
        lock.release()

# Automatically start the queue manager when Celery starts
@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    print("Setting up automatic queue manager...")
    # Only start the queue manager if not already running
    inspector = celery.control.inspect(timeout=5.0)
    active_tasks = inspector.active() or {}
    queue_manager_running = False
    for worker_name, tasks in active_tasks.items():
        for task in tasks:
            if task.get('name') == 'scraping.queue_manager':
                queue_manager_running = True
                break
    if not queue_manager_running:
        print("Starting queue manager automatically...")
        queue_manager_task.apply_async()
    else:
        print("Queue manager already running")


def scrape_task_direct(task_id, department_code, start_from=0, end_at=999999):
    """
    Direct function to handle scraping without using Celery task.
    This allows the queue manager to control the task lifecycle.

    Behavior change: this function will keep retrying on unexpected errors
    and will not mark regular queue tasks as 'stopped' or 'failed' unless
    the user explicitly requests a stop (status -> 'stopping').
    """
    import concurrent.futures
    import time
    import collections
    import threading
    from pathlib import Path
    import shutil

    # Store task_id instead of the task object to avoid session issues
    last_scraped_book = None
    task = TaskQueue.query.get(task_id)  # This task is only used in the main thread
    stopping = False  # Flag to indicate we're in the process of stopping

    # Batch processing variables - separate counters for UI updates vs DB commits
    db_update_batch_size = 50  # Commit to database every 50 books (reduce contention)
    ui_update_interval = 5  # Update UI metrics every 5 books (frequent feedback)
    batch_counter = 0
    ui_counter = 0
    processing_results = collections.defaultdict(int)  # To track success, failures, etc.
    
    # Thread local storage to keep track of worker IDs
    thread_local = threading.local()
    # Worker ID counter and lock
    worker_id_counter = 0
    worker_id_lock = threading.Lock()

    # Main loop: keep trying until user requests stop or we finish the range
    while True:
        print(f"Processing task {task_id} for {department_code}")
        print(f"Starting from book {start_from} to {end_at}")
        print(f"{task.books_processed} books already processed")

        # Calculate start_from based on books_processed and scraping_progress
        if task.books_processed > 0:
            resume_from = task.start_from + task.books_processed
            print(f"Resuming from task's previous progress: book {resume_from}")
            start_from = resume_from
        elif task.scraping_progress and task.scraping_progress.last_kw:
            try:
                last_scraped_book = int(task.scraping_progress.last_kw)
                if last_scraped_book + 1 > start_from:
                    start_from = last_scraped_book + 1
                    print(f"Resuming scraping for task {task_id} from book {start_from} (from ScrapingProgress)")
            except ValueError:
                print(f"Invalid last_kw value in ScrapingProgress: {task.scraping_progress.last_kw}")
        else:
            print(f"Starting scraping for {department_code} from book {start_from}")

        # Check if task is already stopped or stopping before starting
        db.session.refresh(task)
        if task.status in ['stopped']:
            print(f"Task {task_id} is already marked as {task.status}. Exiting.")
            return

        try:
            # Initialize per-thread scrapers (each worker gets its own to avoid contention)
            scrapers_cache = {}  # Thread-safe cache of scraper instances per thread
            scrapers_lock = threading.Lock()
            
            def get_thread_scraper():
                """Get or create a scraper instance for the current thread"""
                thread_id = threading.get_ident()
                # Check cache first without lock (fast path)
                if thread_id in scrapers_cache:
                    return scrapers_cache[thread_id]
                
                # Only lock when creating new scraper (slow path)
                with scrapers_lock:
                    # Double-check in case another thread created it while we waited
                    if thread_id not in scrapers_cache:
                        scrapers_cache[thread_id] = setup_scraper()
                        print(f"Created new scraper instance for thread {thread_id}")
                    return scrapers_cache[thread_id]
            
            processing_done_event.clear()
            print(f"Starting scraper with {DOWNLOAD_WORKERS} download workers")
            book_range = list(range(start_from, end_at + 1))
            total_books = len(book_range)

            # Process books in parallel without waiting for all to complete
            with concurrent.futures.ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
                # Function to process books and handle database updates
                def process_book_with_callback(book_number):
                    nonlocal batch_counter, last_scraped_book, worker_id_counter, ui_counter
                    
                    # Create a Flask application context for this thread
                    with app.app_context():
                        # Check for stopping status at the beginning of each book processing
                        current_task = TaskQueue.query.get(task_id)
                        if current_task and current_task.status == 'stopping':
                            print(f"Worker detected stop signal before processing book: {book_number}")
                            return False  # Signal to stop processing
                        
                        # Assign a stable worker ID if not already assigned to this thread.
                        # Use process id + a bounded thread ident to avoid an ever-growing counter
                        if not hasattr(thread_local, 'worker_id'):
                            try:
                                pid = os.getpid()
                                tid = threading.get_ident() % 10000
                                thread_local.worker_id = f"{pid}-{tid}"
                            except Exception:
                                # Fallback to a simple thread ident string
                                thread_local.worker_id = str(threading.get_ident())
                        
                        # Log which worker is processing which book
                        print(f"Worker {thread_local.worker_id} processing book: {book_number}")
                        
                        # Get this thread's dedicated scraper instance
                        thread_scraper = get_thread_scraper()
                        
                        # Attempt download and record per-result metrics; ensure
                        # every attempt counts toward `books_processed` and
                        # `last_scraped_book` regardless of outcome.
                        result = False
                        try:
                            result = download_worker(thread_scraper, department_code, book_number)
                        except Exception as e:
                            print(f"Worker {thread_local.worker_id} exception for book {book_number}: {e}")
                            with lock:
                                processing_results['error'] += 1

                        # Check for stopping status after processing each book
                        current_task = TaskQueue.query.get(task_id)
                        if current_task and current_task.status == 'stopping':
                            print(f"Worker {thread_local.worker_id} detected stop signal after processing book: {book_number}")
                            return False  # Signal to stop processing

                        # Success-specific handling (success counters)
                        if result:
                            print(f"Worker {thread_local.worker_id} successfully processed book: {book_number}")
                            with lock:
                                processing_results['success'] += 1
                        else:
                            print(f"Worker {thread_local.worker_id} failed to process book: {book_number}")
                            with lock:
                                processing_results['failed'] += 1

                        # Always count the attempt toward progress (books_processed)
                        current_batch_count = 0
                        current_last_book = None
                        with lock:
                            # Track attempts separately as well
                            processing_results['attempts'] += 1

                            # Update last_scraped_book based on this book (attempt)
                            if not last_scraped_book or book_number > last_scraped_book:
                                last_scraped_book = book_number

                            batch_counter += 1
                            ui_counter += 1

                            # Reserve and reset batch_counter atomically if threshold reached
                            if batch_counter >= db_update_batch_size:
                                current_batch_count = batch_counter
                                current_last_book = last_scraped_book
                                batch_counter = 0

                        # Frequent UI updates without DB commits (lightweight)
                        if ui_counter >= ui_update_interval:
                            current_task = TaskQueue.query.get(task_id)
                            if current_task:
                                if current_task.status == 'stopping':
                                    print(f"Worker {thread_local.worker_id} detected stop signal after book: {book_number}")
                                    return False
                            with lock:
                                ui_counter = 0

                        # Database commits for reserved batch (outside the lock)
                        if current_batch_count > 0:
                            current_task = TaskQueue.query.get(task_id)
                            if current_task:
                                current_task.books_processed += current_batch_count
                                current_task.last_scraped_book = str(current_last_book)
                                db.session.commit()
                                print(f"Database updated: {current_batch_count} books committed, last book: {current_last_book}")
                            else:
                                print(f"WARNING: Worker {thread_local.worker_id} could not find task {task_id}")

                        return True  # Continue processing
                
                # Create a lock for thread safety
                lock = threading.Lock()

                # Bounded in-flight (sliding-window) submission to avoid creating
                # very large numbers of pending futures at once. This prevents
                # memory/connection exhaustion when processing large ranges.
                IN_FLIGHT_LIMIT = int(os.getenv('IN_FLIGHT_LIMIT', max(12, DOWNLOAD_WORKERS * 2)))

                pending = {}
                completed_futures = set()

                # Helper to submit next book if any remain
                book_iter = iter(book_range)
                def submit_next():
                    try:
                        b = next(book_iter)
                    except StopIteration:
                        return None
                    f = executor.submit(process_book_with_callback, b)
                    pending[f] = b
                    return f

                # Seed initial window
                try:
                    for _ in range(IN_FLIGHT_LIMIT):
                        if submit_next() is None:
                            break
                except Exception:
                    pass

                # As futures complete, submit new ones to keep the window filled
                for future in concurrent.futures.as_completed(list(pending.keys())):
                    book = pending.pop(future)
                    completed_futures.add(future)

                    try:
                        # If the result is False, we should stop processing
                        if not future.result():
                            stopping = True
                            print(f"Stop signal received while processing book {book}, initiating shutdown...")
                            # Cancel all remaining pending futures
                            for remaining_future in list(pending.keys()):
                                remaining_future.cancel()
                            break
                    except Exception as e:
                        print(f"Error in future for book {book}: {str(e)}")

                    # Also check database for stopping status periodically
                    if len(completed_futures) % 5 == 0:  # Check every 5 completed tasks
                        current_task = TaskQueue.query.get(task_id)
                        if current_task and current_task.status == 'stopping':
                            stopping = True
                            print(f"Stop signal detected from database, cancelling {len(pending)} remaining tasks...")
                            for remaining_future in list(pending.keys()):
                                remaining_future.cancel()
                            break

                    # Try to submit another to keep the window full
                    if not stopping:
                        submit_next()

                # After main loop, cancel any still-pending futures
                if not stopping:
                    for remaining_future in list(pending.keys()):
                        try:
                            # Wait briefly for completion
                            remaining_future.cancel()
                        except Exception:
                            pass
                
                # Update database with any remaining books in the batch (reserve under lock first)
                final_batch = 0
                with lock:
                    if batch_counter > 0:
                        final_batch = batch_counter
                        batch_counter = 0

                if final_batch > 0:
                    # Get a fresh task object for final update
                    final_task = TaskQueue.query.get(task_id)
                    if final_task:
                        final_task.books_processed += final_batch
                        final_task.last_scraped_book = str(last_scraped_book) if last_scraped_book is not None else None
                        db.session.commit()
                        print(f"Final database update: {final_batch} remaining books committed")

            processing_done_event.set()
        finally:
            # Close all thread-specific scrapers
            for thread_id, scraper_instance in scrapers_cache.items():
                try:
                    scraper_instance.close()
                    print(f"Closed scraper for thread {thread_id}")
                except Exception as e:
                    print(f"Error closing scraper for thread {thread_id}: {e}")
            gc.collect()  # Free up memory

        # Update ScrapingProgress table (per-task)
        if last_scraped_book is not None:
            if task.scraping_progress:
                task.scraping_progress.last_kw = str(last_scraped_book)
            else:
                progress = ScrapingProgress(task_id=task.id, last_kw=str(last_scraped_book))
                db.session.add(progress)
            db.session.commit()

        # Check final status of the task - get a fresh instance
        final_task = TaskQueue.query.get(task_id)
        if final_task:
            if stopping or final_task.status == 'stopping':
                print(f"Task {task_id} was stopped, progress saved. Last book: {last_scraped_book}")
                final_task.status = 'stopped'
                db.session.commit()
                break
            elif last_scraped_book is not None and last_scraped_book >= end_at:
                print(f"Scraping completed successfully for department {department_code}, last book: {last_scraped_book}")
                final_task.status = 'completed'
                db.session.commit()
                break
            else:
                # Do not mark as stopped/failed automatically. Save progress and retry.
                print(f"Task ended prematurely for department {department_code}, last book: {last_scraped_book}. Will retry until user stops the task.")
                db.session.commit()

        # Print summary of processing results
        print(f"Processing summary: Success: {processing_results['success']}, Failed: {processing_results['failed']}, Errors: {processing_results['error']}")

        # If we reach here without break, sleep briefly and retry the loop
        time.sleep(5)
        # Refresh task object before next iteration
        task = TaskQueue.query.get(task_id)
        # If task has been set to stopping by user, loop will check at top and exit
        continue


def scrape_idle_task(idle_task_id):
    """Process an IdleTasks entry representing a range of books for a single court.

    Uses a single shared scraper instance and a ThreadPoolExecutor to mimic regular
    scraping concurrency (DOWNLOAD_WORKERS). Stops early when consecutive empty
    results reach IDLE_EMPTY_STREAK_LIMIT or when regular tasks appear.
    """
    from www.models import IdleTasks, IdleStatus
    from concurrent.futures import ThreadPoolExecutor, as_completed

    idle_task = IdleTasks.query.get(idle_task_id)
    if not idle_task:
        print(f"Idle task {idle_task_id} not found")
        return False

    if idle_task.status != 'pending':
        print(f"Idle task {idle_task_id} is not pending (status: {idle_task.status})")
        return False

    # Mark started
    idle_task.mark_started()
    kod_wydzialu = idle_task.kod_wydzialu
    # Resume from last_processed+1 if available, otherwise from configured start_from
    start_from_cfg = int(getattr(idle_task, 'start_from', 0))
    last_proc_val = int(idle_task.last_processed) + 1 if idle_task.last_processed is not None else None
    start_from = max(start_from_cfg, last_proc_val) if last_proc_val is not None else start_from_cfg
    end_at = int(getattr(idle_task, 'end_at', 999999))

    print(f"Idle range task {idle_task_id}: processing {kod_wydzialu} {start_from}-{end_at}")

    idle_status = IdleStatus.get_singleton()
    # Mark idle status as active while this idle task runs
    try:
        idle_status.enabled = IDLE_SCRAPE_ENABLED
        idle_status.active = True
        idle_status.current_code = kod_wydzialu
        idle_status.current_number = start_from
        idle_status.empty_streak = getattr(idle_status, 'empty_streak', 0) or 0
        db.session.commit()
    except Exception:
        db.session.rollback()

    # Shared scraper instance used for initial cookie seeding; individual threads
    # will create their own scraper instances to avoid contention.
    scraper = setup_scraper()
    scrapers_cache = {}

    def get_thread_scraper():
        tid = threading.get_ident()
        if tid not in scrapers_cache:
            try:
                scrapers_cache[tid] = setup_scraper()
            except Exception as e:
                # Fall back to shared scraper if creation fails
                scrapers_cache[tid] = scraper
        return scrapers_cache[tid]

    try:
        # Use single continuous loop structure similar to scrape_task_direct
        # Allow independent idle worker count via env var
        workers = int(os.getenv('IDLE_DOWNLOAD_WORKERS', DOWNLOAD_WORKERS))
        workers = max(1, workers)
        consecutive_empty = getattr(idle_status, 'empty_streak', 0)

        # Worker wrapper that ensures each thread uses its own scraper
        def download_wrapper(num):
            thread_scraper = get_thread_scraper()
            return download_worker(thread_scraper, kod_wydzialu, num)

        # Use a bounded in-flight sliding window submission similar to main scraping
        IN_FLIGHT_LIMIT = int(os.getenv('IN_FLIGHT_LIMIT', max(12, workers * 2)))

        pending = {}
        completed_futures = set()
        
        # Generator for the full range
        book_range = range(start_from, end_at + 1)
        book_iter = iter(book_range)

        def submit_next():
            try:
                n = next(book_iter)
            except StopIteration:
                return None
            f = executor.submit(download_wrapper, n)
            pending[f] = n
            return f

        preempted_by_regular = False
        stopping = False

        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Seed initial window
            try:
                for _ in range(IN_FLIGHT_LIMIT):
                    if submit_next() is None:
                        break
            except Exception:
                pass

            # Main processing loop
            for future in as_completed(list(pending.keys())):
                num = pending.pop(future)
                
                # Check for Regular Tasks Preemption (check periodically e.g. every task completion)
                # This is efficient enough as db query is fast, but we can throttle if needed.
                # Here we check every time to be responsive.
                if not preempted_by_regular and not stopping:
                     try:
                        # Check strictly for pending/in_progress regular tasks
                        if TaskQueue.query.filter(TaskQueue.status.in_(['pending', 'in_progress'])).count() > 0:
                            print(f"Idle range {idle_task_id}: Aborting because regular tasks appeared")
                            preempted_by_regular = True
                            stopping = True
                            # Cancel remaining
                            for remaining_future in list(pending.keys()):
                                remaining_future.cancel()
                     except Exception as e:
                         print(f"Idle preemption check error: {e}")

                book_number_formatted = get_formatted_book_number(str(num))
                control_digit = str(get_control_digit(kod_wydzialu, book_number_formatted))
                full_book_id = f"{kod_wydzialu}-{book_number_formatted}-{control_digit}"

                try:
                    res = future.result()
                except Exception as e:
                    print(f"Idle range {idle_task_id}: Error for {full_book_id}: {e}")
                    res = False

                if res:
                    print(f"Idle range {idle_task_id}: Downloaded {full_book_id}")
                    consecutive_empty = 0
                else:
                    consecutive_empty += 1
                    print(f"Idle range {idle_task_id}: Empty for {full_book_id} (streak {consecutive_empty})")

                # Persist idle_status updates
                try:
                    idle_status.empty_streak = consecutive_empty
                    idle_status.current_code = kod_wydzialu
                    idle_status.current_number = num
                    db.session.commit()
                except Exception:
                    db.session.rollback()

                # Persist idle task progress so we can resume where we left off
                try:
                    idle_task.update_progress(num)
                except Exception:
                    pass

                if consecutive_empty >= IDLE_EMPTY_STREAK_LIMIT:
                    print(f"Idle range {idle_task_id}: Hit empty limit ({IDLE_EMPTY_STREAK_LIMIT}) for court {kod_wydzialu}")
                    stopping = True
                    for remaining_future in list(pending.keys()):
                         remaining_future.cancel()
                    break

                # Refill window if not stopping
                if not stopping:
                    submit_next()

            # End of loop

        if preempted_by_regular:
            try:
                # Return to pending so it can resume later
                idle_task.status = 'pending'
                db.session.commit()
                print(f"Idle range {idle_task_id}: Preempted by regular tasks, set back to pending")
            except Exception:
                db.session.rollback()
        else:
            idle_task.mark_completed()
            print(f"Idle range {idle_task_id}: Completed for {kod_wydzialu}")
        return True

    except Exception as e:
        print(f"Idle range {idle_task_id}: Error during processing: {e}")
        try:
            idle_task.mark_failed(str(e))
        except Exception:
            pass
        return False

    finally:
        try:
            # Mark idle status inactive after finishing/preemption
            try:
                idle_status.active = False
                db.session.commit()
            except Exception:
                db.session.rollback()
            # Close the shared scraper AND every per-thread scraper to avoid leaks
            if scraper:
                try:
                    scraper.close()
                except Exception:
                    pass
            for scraper_instance in scrapers_cache.values():
                try:
                    if scraper_instance is not scraper:
                        scraper_instance.close()
                except Exception:
                    pass
        except Exception:
            pass


def run_idle_alphabetical_scraping():
    """
    Simplified idle scraping that uses separate IdleTasks table.
    
    For each court code alphabetically:
    1. Find the highest existing book number in DB
    2. Create idle tasks starting from (max+1) for individual book downloads
    3. Creates enough tasks to keep the queue busy but not overwhelm it
    
    This creates idle tasks that show up in the queue UI alongside regular tasks.
    """
    # Safety: if any regular task is in progress or pending, do not run idle scraping
    blocking = TaskQueue.query.filter(TaskQueue.status.in_(['pending', 'in_progress'])).all()
    if blocking:
        detail = ', '.join(f"{t.id}:{t.status}" for t in blocking)
        print(f"Idle scraping skipped: regular tasks present => {detail}")
        return

    # Load department codes from JSON
    codes_path = os.path.join(os.path.dirname(__file__), 'codes.json')
    try:
        with open(codes_path, encoding='utf-8') as f:
            codes_data = json.load(f)
            department_codes = sorted([c['code'] for c in codes_data if 'code' in c], key=lambda x: x)
    except Exception:
        # Fallback to department_codes module
        department_codes = sorted(DEPARTMENT_CODES)

    # Update idle status
    try:
        from www.models import IdleStatus, IdleTasks
        idle_status = IdleStatus.get_singleton()
        idle_status.enabled = IDLE_SCRAPE_ENABLED
        idle_status.active = True
        idle_status.current_code = None
        idle_status.current_number = None
        idle_status.empty_streak = 0
        db.session.commit()
    except Exception as e:
        print(f"Idle: Failed to initialize IdleStatus: {e}")
        db.session.rollback()
        idle_status = None

    # Check if there are already pending idle tasks
    pending_idle_tasks = IdleTasks.query.filter(IdleTasks.status.in_(['pending','in_progress'])).count()
    if pending_idle_tasks > 0:
        print(f"Idle scraping: {pending_idle_tasks} pending/in-progress idle tasks already exist. Skipping.")
        return

    # OPTIMIZATION: Fast exit if all courts have been exhausted (all have end_at = 999999)
    # Check if there are any courts that could still have work
    from sqlalchemy import func
    all_exhausted_count = db.session.query(func.count(func.distinct(IdleTasks.kod_wydzialu))).filter(
        IdleTasks.end_at >= 999999
    ).scalar() or 0
    
    total_courts = len(department_codes)
    if all_exhausted_count >= total_courts:
        # All courts have been attempted up to max - no point iterating
        # Only log once every 5 minutes to avoid spam
        import time
        cache_key = '_idle_exhausted_log_time'
        last_log = getattr(run_idle_alphabetical_scraping, cache_key, 0)
        now = time.time()
        if now - last_log > 300:  # 5 minutes
            print(f"Idle scraping: All {total_courts} courts exhausted (end_at >= 999999). Skipping iteration.")
            setattr(run_idle_alphabetical_scraping, cache_key, now)
        return

    try:
        tasks_created = 0
        tasks_created = 0
        max_tasks_per_run = 1  # Create only ONE task at a time to "stick" to the current department
        
        for code in department_codes:
            # Abort if a regular task arrives
            if TaskQueue.query.filter(TaskQueue.status.in_(['pending', 'in_progress'])).count() > 0:
                print("Idle scraping aborted: new regular tasks detected.")
                break

            # Find highest existing book number for this court
            from sqlalchemy import func
            max_ksiega = db.session.query(func.max(Informacje.ksiega)).filter(Informacje.ksiega.like(f"{code}-%")).scalar()
            
            # Also find the highest 'end_at' from completed IdleTasks to skip empty ranges
            max_idle_attempt = db.session.query(func.max(IdleTasks.end_at)).filter_by(kod_wydzialu=code).scalar()

            last_book_num = 0

            if max_ksiega:
                try:
                    # Format: CODE-XXXXXXXX-D
                    parts = max_ksiega.split('-')
                    last_book_num = int(parts[1])
                except Exception:
                    pass
            
            if max_idle_attempt:
                try:
                    last_book_num = max(last_book_num, int(max_idle_attempt))
                except Exception:
                    pass
            
            next_number = last_book_num + 1

            if next_number > 999999:
                print(f"Idle: {code} already at or beyond max number. Skipping.")
                continue

            # Check if there are already idle tasks for this code
            existing_tasks = IdleTasks.query.filter_by(kod_wydzialu=code, status='pending').first()
            if existing_tasks:
                continue  # Skip this code, already has pending tasks

            print(f"Idle: Creating range task for {code} starting from book {next_number}")

            # Update idle status
            if idle_status:
                try:
                    idle_status.current_code = code
                    idle_status.current_number = next_number
                    idle_status.empty_streak = 0
                    db.session.commit()
                except Exception as e:
                    print(f"Idle: Failed to update IdleStatus for {code}: {e}")
                    db.session.rollback()

            # Create one IdleTasks entry representing a large range to be processed like a regular task
            # We'll limit the per-range size to a reasonable window to avoid too-large single tasks
            range_size = min(50000, 999999 - next_number + 1)
            end_at = min(999999, next_number + range_size - 1)

            idle_task = IdleTasks(
                kod_wydzialu=code,
                start_from=next_number,
                end_at=end_at,
                status='pending'
            )
            db.session.add(idle_task)
            db.session.commit()
            tasks_created += 1
            print(f"Idle: Created range task for {code} from {next_number} to {end_at}")
            
            # Stop if we've created enough tasks for this run
            if tasks_created >= max_tasks_per_run:
                break
            
    finally:
        if idle_status:
            try:
                idle_status.active = False
                db.session.commit()
            except Exception as e:
                print(f"Idle: Failed to deactivate IdleStatus: {e}")
                db.session.rollback()


# Health check endpoint for Docker and monitoring
@app.route('/health')
def health_check():
    """Health check endpoint for Docker container monitoring.
    Returns 200 if database is accessible, 503 otherwise.
    """
    from sqlalchemy import text
    try:
        db.session.execute(text('SELECT 1'))
        db.session.commit()  # Release the connection back to pool
        return jsonify({
            'status': 'healthy',
            'database': 'connected'
        }), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({
            'status': 'unhealthy',
            'database': 'disconnected',
            'error': str(e)
        }), 503


@app.route('/tasks', methods=['GET'])
@login_required
@admin_required
def list_tasks():
    # Fetch tasks from the database, ordered by creation date with pagination
    page = request.args.get('page', 1, type=int)
    per_page = 10
    pagination = TaskStatus.query.order_by(TaskStatus.date_created.desc()).paginate(page=page, per_page=per_page)
    
    # Check if the backend is available before attempting to fetch live task statuses
    if isinstance(celery.backend, DisabledBackend):
        # We'll just use the database status if backend is disabled
        pass
    else:
        # Update task statuses in the database based on their live status
        try:
            # Attempt to retrieve the task using AsyncResult with celery instance
            for task_status in pagination.items:
                try:
                    task = AsyncResult(task_status.task_id, app=celery)
                    live_status = task.status
                    if task_status.status != live_status:
                        task_status.status = live_status
                        db.session.add(task_status)
                except Exception as e:
                    print(f"Error getting task status: {e}")

            db.session.commit()
        except Exception as e:
            print(f"Error updating live task statuses: {e}")

    return render_template('tasks.html', tasks=pagination)


@app.route('/start_celery_workers', methods=['POST'])
@login_required
@admin_required
def start_celery_workers():
    """Start Celery workers as a background process"""
    try:
        # Check if we have a Python virtual environment
        venv_python = None
        if 'VIRTUAL_ENV' in os.environ:
            venv_path = os.environ['VIRTUAL_ENV']
            if os.path.exists(os.path.join(venv_path, 'bin', 'python')):  # Unix
                venv_python = os.path.join(venv_path, 'bin', 'python')
            elif os.path.exists(os.path.join(venv_path, 'Scripts', 'python.exe')):  # Windows
                venv_python = os.path.join(venv_path, 'Scripts', 'python.exe')
        
        # Determine Python executable
        python_exe = venv_python if venv_python else 'python'
        
        # Path to current directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(current_dir)
        
        # Command to start Celery workers. Read desired concurrency/pool from env.
        CELERY_CONCURRENCY = os.getenv('CELERY_CONCURRENCY', '4')
        CELERY_POOL = os.getenv('CELERY_POOL', 'prefork')
        CELERY_PREFETCH = os.getenv('CELERY_PREFETCH_MULTIPLIER', None)

        # Base command
        cmd = [
            python_exe,
            '-m', 'celery',
            '-A', 'www.main.celery',
            'worker',
            '--loglevel=info',
            '--detach'
        ]

        # Append concurrency and pool
        if CELERY_CONCURRENCY:
            cmd.extend(['--concurrency', str(CELERY_CONCURRENCY)])
        if CELERY_POOL:
            cmd.extend(['--pool', str(CELERY_POOL)])
        if CELERY_PREFETCH:
            cmd.extend(['--prefetch-multiplier', str(CELERY_PREFETCH)])
        
        # Start the process
        process = subprocess.Popen(
            cmd,
            cwd=parent_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True  # Detach from parent process
        )
        
        # Check if process started successfully
        return_code = process.poll()
        if return_code is not None and return_code != 0:
            stderr = process.stderr.read().decode('utf-8')
            return jsonify({'success': False, 'message': f"Error starting workers: {stderr}"})
            
        # Wait a moment to allow workers to start up
        time.sleep(2)
        
        # Check if workers are now active (bounded timeout so POST can't hang)
        inspector = celery.control.inspect(timeout=5.0)
        active_workers = inspector.ping() or {}
        
        if active_workers:
            return jsonify({'success': True, 'worker_count': len(active_workers)})
        else:
            return jsonify({'success': False, 'message': "Workery uruchomione, ale nie odpowiadają"})
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.route('/restart_celery_workers', methods=['POST'])
@login_required
@admin_required
def restart_celery_workers():
    """Restart all Celery workers"""
    try:
        # First, get a list of running Celery worker processes
        celery_procs = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info['cmdline']
                if cmdline and 'celery' in ' '.join(cmdline).lower() and 'worker' in ' '.join(cmdline).lower():
                    celery_procs.append(proc)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        
        # Stop all running worker processes
        for proc in celery_procs:
            try:
                proc.terminate()
            except psutil.NoSuchProcess:
                pass
        
        # Give processes time to terminate gracefully
        time.sleep(2)
        
        # Force kill any remaining processes
        for proc in celery_procs:
            try:
                if proc.is_running():
                    proc.kill()
            except psutil.NoSuchProcess:
                pass
        
        # Now start new workers by calling our start_celery_workers function
        start_response = start_celery_workers()
        start_data = start_response.get_json()
        
        if start_data.get('success', False):
            return jsonify({'success': True, 'message': f"Zrestartowano {start_data.get('worker_count', 0)} workerów"})
        else:
            return jsonify({'success': False, 'message': start_data.get('message', 'Unknown error')})
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})


@app.context_processor
def inject_active_task_count():
    # NOTE: previously called inspector.active() on EVERY page render — a
    # blocking Celery control RPC that exhausted gunicorn threads → 504.
    # Now sourced from the TTL-cached inspect result.
    try:
        active_task_count = get_active_task_count_cached()
    except Exception:
        active_task_count = 0
    return {"active_task_count": active_task_count}


app.secret_key = os.getenv('APP_SECRET')

auth = LoginManager()
auth.init_app(app)

@auth.user_loader
def load_user(user_id):
    return get_user(int(user_id))

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('index'))

@app.route('/', methods=['GET', 'POST'])
def index():
    error = session.get('error')
    session['error'] = ''
    
    if request.method == 'POST':
        login = request.form['login']
        password = request.form['password']
        user = get_user_by_login(login)
        if user:
            if user.check_password(password):
                login_user(user)
                return redirect(url_for('book_list'))
            else:
                # Nieprawidłowe hasło
                session['error'] = "Nieprawidłowe hasło!"
                return redirect(url_for('index'))
        else:
            # Nie znaleziono takiego użytkownika
            session['error'] = "Nie odnaleziono użytkownika!"
            return redirect(url_for('index'))
    else:
        # Sprawdzamy czy nie jest już autoryzowany
        if current_user.is_authenticated:
            return redirect(url_for('book_list'))
        else:
            return render_template('login.html', error=error)

def get_data(page, per_page, filters=None, parameters=None, order_by=None):
    query = db.session.query(Informacje)
    offset = (page - 1) * per_page
    
    # Apply prefix-based access control for non-admin users
    if not current_user.is_admin:
        allowed_prefixes = current_user.get_allowed_prefixes()
        if allowed_prefixes is not None:  # None means admin access (all prefixes)
            if not allowed_prefixes:  # Empty list means no access
                query = query.filter(False)  # Return no results
            else:
                # Filter by allowed prefixes in the ksiega field
                prefix_conditions = []
                for prefix in allowed_prefixes:
                    prefix_conditions.append(Informacje.ksiega.like(f'{prefix}-%'))
                from sqlalchemy import or_
                query = query.filter(or_(*prefix_conditions))

    # Apply multiple filters
    if filters:
        for filter_item in filters:
            filter_type = filter_item.get('filter')
            search = filter_item.get('search', '').strip()
            statement = filter_item.get('statement')
            
            if not search or not filter_type:
                continue
                
            search_upper = search.upper()

            if filter_type == "ksiega":
                query = query.filter(Informacje.ksiega.ilike(f'%{search}%'))
            elif filter_type == "id":
                try:
                    query = query.filter(Informacje.id == int(search))
                except ValueError:
                    query = query.filter(False)
            elif filter_type == "kwota_hipoteki":
                query = query.filter(Informacje.kwota_hipoteki.ilike(f'%{search}%'))
            elif filter_type == "identyfikator":
                query = query.filter(Informacje.identyfikator.ilike(f'%{search}%'))
            elif filter_type == "typ":
                # Map hardcoded categories to database types
                if search == "księga gruntowa":
                    gruntowa_types = [
                        'NIERUCHOMOŚĆ GRUNTOWA',
                        'GRUNT ODDANY W UŻYTKOWANIE WIECZYSTE',
                        'GRUNT ODDANY W UŻYTKOWANIE WIECZYSTE, BUDYNEK I URZĄDZENIE STANOWIĄCE ODRĘBNĄ NIERUCHOMOŚĆ',
                        'GRUNT ODDANY W UŻYTKOWANIE WIECZYSTE I BUDYNEK STANOWIĄCY ODRĘBNĄ NIERUCHOMOŚĆ',
                        'GRUNT ODDANY W UŻYTKOWANIE WIECZYSTE I URZĄDZENIE STANOWIĄCE ODRĘBNĄ NIERUCHOMOŚĆ',
                        'BUDYNEK STANOWIĄCY ODRĘBNĄ NIERUCHOMOŚĆ'
                    ]
                    query = query.filter(Informacje.typ.in_(gruntowa_types))
                elif search == "księga lokalowa":
                    lokalowa_types = [
                        'LOKAL STANOWIĄCY ODRĘBNĄ NIERUCHOMOŚĆ',
                        'PRAWO DO DOMU JEDNORODZINNEGO W SPÓŁDZIELNI MIESZKANIOWEJ',
                        'SPÓŁDZIELCZE PRAWO DO LOKALU UŻYTKOWEGO',
                        'SPÓŁDZIELCZE WŁASNOŚCIOWE PRAWO DO LOKALU',
                        'WŁASNOŚCIOWE SPÓŁDZIELCZE PRAWO DO LOKALU MIESZKALNEGO'
                    ]
                    query = query.filter(Informacje.typ.in_(lokalowa_types))
                else:
                    query = query.filter(Informacje.typ.ilike(f'%{search}%'))
            elif filter_type == "lokalizacja":
                query = query.filter(
                    (Informacje.ulica.ilike(f'%{search}%')) |
                    (Informacje.adres.ilike(f'%{search}%')) |
                    (Informacje.polozenie.ilike(f'%{search}%'))
                )
            elif filter_type == "pesel":
                # Filter by PESEL number using EXISTS to avoid building large IN subqueries
                normalized_search = search.replace(' ', '').replace('-', '')
                query = query.filter(
                    exists().where(
                        and_(Wlasciciele.ksiega == Informacje.ksiega,
                             Wlasciciele.pesel.ilike(f'%{normalized_search}%'))
                    )
                )
            elif filter_type in ("wlasciciel", "nazwa_wlasciciela", "imie_nazwisko"):
                # Unified owner filter: match company name, first name, last name or PESEL.
                # Backwards-compatible: accept legacy filter names 'nazwa_wlasciciela' and 'imie_nazwisko'.
                from sqlalchemy import func, or_
                token = search.strip()
                tokens = token.split()
                normalized_no_space = token.replace(' ', '').lower()

                if len(tokens) == 1:
                    t = tokens[0].lower().replace(' ', '')
                    query = query.filter(
                        exists().where(
                            and_(Wlasciciele.ksiega == Informacje.ksiega,
                                 or_(
                                     func.lower(func.replace(Wlasciciele.nazwa, ' ', '')).ilike(f'%{t}%'),
                                     func.lower(func.replace(Wlasciciele.imie, ' ', '')).ilike(f'%{t}%'),
                                     func.lower(func.replace(Wlasciciele.nazwisko, ' ', '')).ilike(f'%{t}%'),
                                     Wlasciciele.pesel.ilike(f'%{token}%')
                                 )
                            )
                        )
                    )
                else:
                    first = tokens[0].lower().replace(' ', '')
                    last = tokens[-1].lower().replace(' ', '')
                    query = query.filter(
                        exists().where(
                            and_(Wlasciciele.ksiega == Informacje.ksiega,
                                 or_(
                                     # company name contains whole phrase
                                     func.lower(func.replace(Wlasciciele.nazwa, ' ', '')).ilike(f'%{normalized_no_space}%'),
                                     # person matches first+last or swapped
                                     and_(
                                         func.lower(func.replace(Wlasciciele.imie, ' ', '')).ilike(f'%{first}%'),
                                         func.lower(func.replace(Wlasciciele.nazwisko, ' ', '')).ilike(f'%{last}%')
                                     ),
                                     and_(
                                         func.lower(func.replace(Wlasciciele.imie, ' ', '')).ilike(f'%{last}%'),
                                         func.lower(func.replace(Wlasciciele.nazwisko, ' ', '')).ilike(f'%{first}%')
                                     )
                                 )
                            )
                        )
                    )
            elif filter_type == "ilosc_wlascicieli" and statement:
                try:
                    query = query.filter(
                        Informacje.ksiega.in_(
                            db.session.query(Wlasciciele.ksiega)
                            .group_by(Wlasciciele.ksiega)
                            .having(db.func.count(Wlasciciele.ksiega).op(statement)(int(search)))
                        )
                    )
                except ValueError:
                    pass

    if parameters:
        for param, value in parameters.items():
            if value == "on":
                if param.startswith("+"):
                    real_value = int(param.split("+")[1])
                    query = query.filter(
                        Informacje.ksiega.in_(
                            db.session.query(Wlasciciele.ksiega).filter(
                                Wlasciciele.wiek.between(real_value, real_value + 10)
                            )
                        )
                    )
                else:
                    if param != "hipoteka":
                        query = query.filter(getattr(Informacje, param) == 1)
                    else:
                        query = query.filter(Informacje.kwota_hipoteki != 'Brak')

    # --- NEW: Egzekucja/Hipoteka date range filtering ---
    egzekucja_from = parse_date(parameters.get('egzekucja_date_from'))
    egzekucja_to = parse_date(parameters.get('egzekucja_date_to'))
    hipoteka_from = parse_date(parameters.get('hipoteka_date_from'))
    hipoteka_to = parse_date(parameters.get('hipoteka_date_to'))

    if parameters.get('egzekucja') == "on" and (egzekucja_from or egzekucja_to):
        # Use EXISTS correlated subquery to ask whether an Egzekucje row exists for this Informacje.ksiega
        conditions = [Egzekucje.ksiega == Informacje.ksiega]
        if egzekucja_from:
            conditions.append(Egzekucje.data_wszczecia >= egzekucja_from)
        if egzekucja_to:
            conditions.append(Egzekucje.data_wszczecia <= egzekucja_to)
        query = query.filter(exists().where(and_(*conditions)))
    if parameters.get('hipoteka') == "on" and (hipoteka_from or hipoteka_to):
        conditions = [Hipoteki.ksiega == Informacje.ksiega]
        if hipoteka_from:
            conditions.append(Hipoteki.data_wydania >= hipoteka_from)
        if hipoteka_to:
            conditions.append(Hipoteki.data_wydania <= hipoteka_to)
        query = query.filter(exists().where(and_(*conditions)))

    # Darowizna, Dziedziczenie, Spadek date ranges
    darowizna_from = parse_date(parameters.get('darowizna_date_from'))
    darowizna_to = parse_date(parameters.get('darowizna_date_to'))
    dziedziczenie_from = parse_date(parameters.get('dziedziczenie_date_from'))
    dziedziczenie_to = parse_date(parameters.get('dziedziczenie_date_to'))
    spadek_from = parse_date(parameters.get('spadek_date_from'))
    spadek_to = parse_date(parameters.get('spadek_date_to'))

    if parameters.get('darowizna') == "on" and (darowizna_from or darowizna_to):
        conditions = [Darowizny.ksiega == Informacje.ksiega]
        if darowizna_from:
            conditions.append(Darowizny.data_umowy >= darowizna_from)
        if darowizna_to:
            conditions.append(Darowizny.data_umowy <= darowizna_to)
        query = query.filter(exists().where(and_(*conditions)))

    if parameters.get('dziedziczenie') == "on" and (dziedziczenie_from or dziedziczenie_to):
        conditions = [Dziedziczenia.ksiega == Informacje.ksiega]
        if dziedziczenie_from:
            conditions.append(Dziedziczenia.data_orzeczenia >= dziedziczenie_from)
        if dziedziczenie_to:
            conditions.append(Dziedziczenia.data_orzeczenia <= dziedziczenie_to)
        query = query.filter(exists().where(and_(*conditions)))

    if parameters.get('spadek') == "on" and (spadek_from or spadek_to):
        conditions = [Spadki.ksiega == Informacje.ksiega]
        if spadek_from:
            conditions.append(Spadki.data_orzeczenia >= spadek_from)
        if spadek_to:
            conditions.append(Spadki.data_orzeczenia <= spadek_to)
        query = query.filter(exists().where(and_(*conditions)))

    # Validate order_by values against an allowlist to avoid errors and abuse
    ALLOWED_SORT_COLUMNS = {'id', 'ksiega', 'kwota_hipoteki', 'obszar_calej', 'adres', 'typ'}
    if order_by:
        for column in order_by:
            if column in ALLOWED_SORT_COLUMNS:
                query = query.order_by(getattr(Informacje, column).asc())
    else:
        query = query.order_by(Informacje.ksiega.asc())

    total_count = query.count()
    data = query.limit(per_page).offset(offset).all()
    total_pages = (total_count // per_page) + (1 if total_count % per_page else 0)

    return [data, total_pages, total_count]



def get_powiat(a):
    if a != "Brak":
        b = a.split(', ')
        if len(b) == 2: # Powiat i miasto
            return b[0]
        else:
            return "Brak"
    else:
        return "Brak"

def get_miasto(a):
    if a != "Brak":
        b = a.split(', ')
        if len(b) == 1: # Tylko miasto
            return b[0]
        if len(b) == 2: # Powiat i miasto
            return b[1]
        if len(b) == 3:
            return b[2]
        if len(b) == 4: # Tylko miasto
            return b[3]
        if len(b) == 5: # Tylko miasto i część miasta
            return b[3] + " " + b[4]
    else:
        return "Brak"
    
def get_wlasciciele(kw):
    return Wlasciciele.query.filter_by(ksiega=kw).count()


def get_udzialy(kw):
    udzialy = Wlasciciele.query.filter_by(ksiega=kw).all()
    udzial_list = [row.udzial for row in udzialy]
    if not udzial_list or udzial_list[0] == "Brak":
        return "Brak"
    return " <b>,</b> ".join(udzial_list)


def is_notatka(kw):
    notatka = Notatki.query.filter_by(ksiega=kw).first()
    return bool(notatka and notatka.notatka)


def search_oldperson(kw, age):
    max_age = db.session.query(db.func.max(Wlasciciele.wiek)).filter_by(ksiega=kw).scalar()
    return max_age is not None and age <= max_age <= age + 10

    
def get_status(kw):
    status = Status.query.filter_by(ksiega=kw).first()
    if status:
        return {
            'w_pozysku': ["W pozysku", "text-bg-primary"],
            'otwarte': ["Otwarte", "text-bg-success"],
            'analiza': ["Analiza", "text-bg-warning"],
            'odrzucone': ["Odrzucone", "text-bg-danger"],
            'pozyskane': ["Pozyskane", "text-bg-info text-white"]
        }.get(status.stan, ["Analiza", "text-bg-warning"])
    return ["Analiza", "text-bg-warning"]


def is_hipoteka(kw):
    if kw == "Brak":
        return False
    else:
        return True
    
def get_data_kw(kw):
    return Informacje.query.filter_by(ksiega=kw).first()


def get_lista_wlascicieli(kw, dedupe=False):
    """Return list of owners for a księga. If dedupe=True, remove duplicate owners.

    Deduplication strategy:
    - For company owners (czy_firma == True) dedupe by normalized `nazwa` (lower, spaces removed).
    - For individuals with a valid PESEL (not 'Brak') dedupe by PESEL.
    - For individuals without PESEL dedupe by normalized fullname (imie + nazwisko).

    We preserve ordering from the DB (ordered by age desc in callers) so the first seen
    entry is kept (usually the most complete/oldest).
    """
    rows = Wlasciciele.query.filter_by(ksiega=kw).order_by(Wlasciciele.wiek.desc()).all()
    if not dedupe:
        return rows

    seen = set()
    result = []
    for r in rows:
        if r.czy_firma:
            key = ('firma', (r.nazwa or '').strip().lower().replace(' ', ''))
        else:
            pesel = (r.pesel or '').strip()
            if pesel and pesel != 'Brak':
                key = ('pesel', pesel)
            else:
                name = ((r.imie or '') + ' ' + (r.nazwisko or '')).strip().lower().replace(' ', '')
                key = ('person', name)

        if key not in seen:
            seen.add(key)
            result.append(r)

    return result


@app.route('/api/set_status/<kw>', methods=['POST'])
@login_required
def set_status(kw):
    data = request.get_json()
    status = data.get('status')
    current_status = Status.query.filter_by(ksiega=kw).first()
    
    if current_status:
        current_status.stan = status
    else:
        new_status = Status(ksiega=kw, stan=status)
        db.session.add(new_status)
    
    db.session.commit()
    return "Status został zmieniony", 200

    
@app.route('/api/get_status/<kw>', methods=['GET'])
@login_required
def get_status_inside(kw):
    status = Status.query.filter_by(ksiega=kw).first()
    return status.stan if status else "analiza"


@app.route('/api/get_note/<kw>', methods=['GET'])
@login_required
def get_note(kw):
    note = Notatki.query.filter_by(ksiega=kw).first()
    return note.notatka if note else ""


@app.route('/api/note/<kw>', methods=['POST'])
@login_required
def save_note(kw):
    try:
        data = request.get_json()
        note = data.get('note')
        ksiega = data.get('ksiega')

        current_note = Notatki.query.filter_by(ksiega=ksiega).first()
        if current_note:
            current_note.notatka = note
        else:
            new_note = Notatki(ksiega=ksiega, notatka=note)
            db.session.add(new_note)

        db.session.commit()
        return "Notatka została zapisana", 200
    except Exception as e:
        return "Wystąpił błąd przy dodawaniu notatki", 500

    
def dynamic_search(kw, term, dzial):
    dirn = os.path.join(app.root_path, '..', 'ekw')
    file_path = os.path.join(dirn, kw, dzial)
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                if term in line:
                    return True
        return False

def delete_old_tasks():
    tasks_to_delete = TaskStatus.query.filter(
        not_(TaskStatus.status.in_(['started', 'pending']))  # Exclude 'started' and 'pending' tasks
    ).order_by(TaskStatus.date_created.desc())  # Order by most recent tasks

    # Fetch all but the 10 most recent tasks
    tasks_to_delete = tasks_to_delete.offset(10).all()

    # Delete the selected tasks from the database
    if tasks_to_delete:
        for task in tasks_to_delete:
            db.session.delete(task)
        db.session.commit()
    
    return True



@app.route('/api/kw/<path:filename>')
@login_required
def serve_files(filename):
    base_dir = os.path.abspath(os.path.join(app.root_path, '..', 'ekw'))

    # Guard against path traversal: resolve and confirm the file stays inside base_dir
    file_path = os.path.realpath(os.path.join(base_dir, filename))
    if not os.path.commonpath([base_dir, file_path]) == base_dir:
        return "Forbidden", 403

    print(f"Ścieżka do podglądu księgi: {file_path}")
    
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        modified_content = content + '\n<link rel="stylesheet" href="/static/kw.css">'
        
        response = make_response(modified_content)
        response.headers['Content-Type'] = 'text/html'
        
        return response
    else:
        print("File not found:", file_path)
        return "File not found", 404

@app.route('/kw')
@login_required
def kw():
    ksiega = request.args.get('ksiega')
    data = get_data_kw(ksiega)
    # Allow caller to disable deduplication by passing dedupe=0 in query params
    dedupe_param = request.args.get('dedupe', '1')
    dedupe = False if dedupe_param in ('0', 'false', 'False') else True

    lista_wlascicieli_raw = get_lista_wlascicieli(ksiega)
    lista_wlascicieli = get_lista_wlascicieli(ksiega, dedupe=dedupe)

    return render_template('display.html', ksiega=ksiega, data=data, get_wlasciciele=get_wlasciciele,
                           lista_wlascicieli=lista_wlascicieli, lista_wlascicieli_raw=lista_wlascicieli_raw,
                           dedupe=dedupe, get_miasto=get_miasto)

def format_hipoteka(kwota):
    try:
        if kwota is None:
            return "Brak"
        if kwota == "Brak":
            return kwota
        s = str(kwota).replace("\xa0", " ").strip()
        if not s:
            return kwota
        m = re.match(r'([0-9\s,\.]+)\s*(.*)', s)
        if not m:
            return kwota
        num_part = m.group(1).replace(" ", "")
        waluta = (m.group(2) or '').strip()
        # Ensure there is at least one digit
        if not re.search(r'\d', num_part):
            return kwota
        # Normalize number: keep only last separator as decimal
        seps = [i for i, c in enumerate(num_part) if c in ',.']
        if seps:
            last = seps[-1]
            digits_only = re.sub(r'[,.]', '', num_part)
            # If last separator was not the last character, treat as decimal point
            if last < len(num_part) - 1:
                decimals = len(num_part) - last - 1
                if decimals < len(digits_only):
                    normalized = digits_only[:-decimals] + '.' + digits_only[-decimals:]
                else:
                    normalized = '0.' + digits_only.zfill(decimals)
            else:
                normalized = digits_only
        else:
            normalized = num_part
        value = float(normalized)
        return f"{value:,} {waluta}".strip()
    except Exception:
        # On any parsing error, return original value to avoid 500
        return kwota

@app.template_filter('update_query_params')
def update_query_params(query_params, **new_params):
    params = query_params.copy()
    for key, value in new_params.items():
        params[key] = value
    return urllib.parse.urlencode(params)


@app.route('/list')
@login_required
def book_list():
    page = request.args.get('page', 1, type=int)
    records_per_page = request.args.get('limit', 10, type=int)
    order_by = request.args.getlist('order_by')
    
    # Handle multiple filters
    filters = []
    filter_count = 0
    
    # Count how many filters we have
    while f'filter_{filter_count}' in request.args:
        filter_item = {
            'filter': request.args.get(f'filter_{filter_count}'),
            'search': request.args.get(f's_{filter_count}', ''),
            'statement': request.args.get(f'statement_{filter_count}')
        }
        if filter_item['filter'] and filter_item['search']:
            filters.append(filter_item)
        filter_count += 1
    
    # Backward compatibility: handle single filter
    if not filters:
        get_filter = request.args.get('filter')
        search = request.args.get('s', '')
        statement = request.args.get('statement', None, type=str)
        
        if get_filter and search:
            filters.append({
                'filter': get_filter,
                'search': search,
                'statement': statement
            })
    
    parameters = {
        'hipoteka': request.args.get('hipoteka', ""),
        'spadek': request.args.get('spadek', ""),
        'dziedziczenie': request.args.get('dziedziczenie', ""),
        'darowizna': request.args.get('darowizna', ""),
        'egzekucja': request.args.get('egzekucja', ""),
        '+90': request.args.get('plus90', ""),
        '+80': request.args.get('plus80', ""),
        '+70': request.args.get('plus70', ""),
        # Add date range parameters for egzekucja and hipoteka
    'egzekucja_date_from': request.args.get('egzekucja_date_from', ""),
    'egzekucja_date_to': request.args.get('egzekucja_date_to', ""),
    'hipoteka_date_from': request.args.get('hipoteka_date_from', ""),
    'hipoteka_date_to': request.args.get('hipoteka_date_to', ""),
    # Add date range parameters for darowizna, dziedziczenie and spadek
    'darowizna_date_from': request.args.get('darowizna_date_from', ""),
    'darowizna_date_to': request.args.get('darowizna_date_to', ""),
    'dziedziczenie_date_from': request.args.get('dziedziczenie_date_from', ""),
    'dziedziczenie_date_to': request.args.get('dziedziczenie_date_to', ""),
    'spadek_date_from': request.args.get('spadek_date_from', ""),
    'spadek_date_to': request.args.get('spadek_date_to', "")
    }

    database = get_data(page, records_per_page, filters, parameters, order_by)

    department_code = 'PO1P'
    start_from = 0
    end_at = 999999

    total_pages = database[1]
    data = database[0]
    total_records = database[2]
    found_records = len(data)

    # Prefetch related aggregates/maps for visible rows to avoid per-row DB queries in template
    ksiega_list = [r.ksiega for r in data]
    if ksiega_list:
        # owners count per ksiega
        owners_counts = dict(
            db.session.query(Wlasciciele.ksiega, db.func.count(Wlasciciele.id))
            .filter(Wlasciciele.ksiega.in_(ksiega_list))
            .group_by(Wlasciciele.ksiega)
            .all()
        )

        # owners shares aggregated as comma-separated string (Postgres string_agg)
        try:
            udzials = dict(
                db.session.query(
                    Wlasciciele.ksiega,
                    db.func.string_agg(Wlasciciele.udzial, ', ').label('udzialy')
                )
                .filter(Wlasciciele.ksiega.in_(ksiega_list))
                .group_by(Wlasciciele.ksiega)
                .all()
            )
        except Exception:
            # Fallback to python-side join if DB doesn't support string_agg
            udzials = {}
            rows = db.session.query(Wlasciciele.ksiega, Wlasciciele.udzial).filter(Wlasciciele.ksiega.in_(ksiega_list)).all()
            tmp = {}
            for k, u in rows:
                tmp.setdefault(k, []).append(u)
            for k, arr in tmp.items():
                udzials[k] = ', '.join(arr)

        # status_map
        status_map = dict(db.session.query(Status.ksiega, Status.stan).filter(Status.ksiega.in_(ksiega_list)).all())

        # notatki set
        notatki_set = set(r[0] for r in db.session.query(Notatki.ksiega).filter(Notatki.ksiega.in_(ksiega_list)).all())

        # max_age per ksiega
        max_age_map = dict(
            db.session.query(Wlasciciele.ksiega, db.func.max(Wlasciciele.wiek))
            .filter(Wlasciciele.ksiega.in_(ksiega_list))
            .group_by(Wlasciciele.ksiega)
            .all()
        )
    else:
        owners_counts = {}
        udzials = {}
        status_map = {}
        notatki_set = set()
        max_age_map = {}
    
    start_page = max(page - 2, 1)
    end_page = min(page + 2, total_pages)
    if start_page == 1:
        end_page = min(5, total_pages)
    elif end_page == total_pages:
        start_page = max(1, total_pages)

    return render_template('list.html', found_records=found_records, 
                           # removed per-row DB helpers; use maps instead
                           udzials=udzials, owners_counts=owners_counts, status_map=status_map,
                           notatki_set=notatki_set, max_age_map=max_age_map,
                           dynamic_search=dynamic_search, records_per_page=records_per_page, current_page=page,
                           data=data, total_pages=total_pages, start_page=start_page,
                           end_page=end_page, get_miasto=get_miasto, get_powiat=get_powiat,
                           is_hipoteka=is_hipoteka, format_hipoteka=format_hipoteka,
                           filters=filters, filter_query=filters[0]['filter'] if filters else None,
                           total_records=total_records, department_code=department_code, 
                           start_from=start_from, end_at=end_at)


@app.route('/home')
@login_required
def home():
    return render_template('home.html')


@app.route('/start_scraping', methods=['POST'])
@login_required
@admin_required
def start_scraping():
    active_task_count = get_active_task_count_cached()

    if active_task_count >= 2:
        return jsonify({"error": "Only 2 tasks at a time allowed."}), 403
    
    department_code = request.form['department_code']
    start_from = int(request.form.get('start_from', 0))
    end_at = int(request.form.get('end_at', 999999))

    task = scrape_task.apply_async(args=[department_code, start_from, end_at])

    new_task_status = TaskStatus(
        task_id=task.id,
        department_code=department_code,
        status='pending'
    )
    db.session.add(new_task_status)

    db.session.commit()

    return f"Scraper aktywny dla kodu {department_code}", 200


@app.route('/start_all_scraping', methods=['POST'])
@login_required
@admin_required
def start_all_scraping():

    active_task_count = get_active_task_count_cached()

    if active_task_count >= 2:
        return jsonify({"error": "Only 2 tasks at a time allowed."}), 403

    start_from = 0
    end_at = 999999 
    
    for code in DEPARTMENT_CODES:
        task = scrape_task.apply_async(args=[code, start_from, end_at])

        new_task_status = TaskStatus(
            task_id=task.id,
            department_code=code,
            status='pending'
        )
        db.session.add(new_task_status)
    
    db.session.commit()
    
    return f"Scraper aktywny dla wszystkich kodów", 200


@app.route('/zadania', methods=['GET', 'POST'])
@login_required
@admin_required
def zadania():
    # Load department codes (from codes.json if available, fallback to DEPARTMENT_CODES)
    codes_path = os.path.join(os.path.dirname(__file__), 'codes.json')
    try:
        with open(codes_path, encoding='utf-8') as f:
            department_codes = json.load(f)
    except Exception:
        department_codes = DEPARTMENT_CODES

    page = request.args.get('page', 1, type=int)
    per_page = 10

    # Get tasks from the TaskQueue table
    # Newest first (by id desc); secondary order by status and then priority for readability
    pagination = TaskQueue.query.order_by(
        TaskQueue.id.desc(),
        TaskQueue.status.in_(['pending', 'in_progress']).desc(),
        TaskQueue.priority.desc()
    ).paginate(page=page, per_page=per_page)

    # Get current in-progress task
    current_task = TaskQueue.query.filter_by(status='in_progress').first()

    # Calculate task statistics
    pending_count = TaskQueue.query.filter_by(status='pending').count()
    in_progress_count = TaskQueue.query.filter_by(status='in_progress').count()
    completed_count = TaskQueue.query.filter_by(status='completed').count()
    failed_count = TaskQueue.query.filter_by(status='failed').count()
    
    # Get idle task statistics
    from www.models import IdleTasks
    idle_pending_count = IdleTasks.query.filter_by(status='pending').count()
    idle_in_progress_count = IdleTasks.query.filter_by(status='in_progress').count()
    idle_completed_count = IdleTasks.query.filter_by(status='completed').count()
    idle_failed_count = IdleTasks.query.filter_by(status='failed').count()
    
    # Get some recent idle tasks to show in the template
    idle_tasks = IdleTasks.query.order_by(
        IdleTasks.id.desc()
    ).limit(5).all()

    # Check if queue manager is running — use TTL-cached inspect to avoid
    # 3 blocking RPCs (active + ping + active) per page load that caused 504s.
    inspect_data = get_celery_inspect_cached()
    active_tasks = inspect_data['active'] or {}
    queue_manager_running = False
    for worker_name, tasks in active_tasks.items():
        for task in tasks:
            if task.get('name') == 'scraping.queue_manager':
                queue_manager_running = True
                break

    # For backward compatibility, also get worker information (from same cache)
    workers_list = []
    try:
        active_workers = inspect_data['ping'] or {}
        active_tasks_by_worker = inspect_data['active'] or {}
        for worker_name in active_workers.keys():
            worker_tasks = active_tasks_by_worker.get(worker_name, [])
            workers_list.append({
                "worker_name": worker_name,
                "status": "Available",
                "active_tasks": worker_tasks,
                "task_count": len(worker_tasks)
            })
    except Exception as e:
        print(f"Error connecting to workers: {str(e)}")

    # Build idle info for slim status bar
    try:
        from www.models import IdleStatus
        idle_status = IdleStatus.get_singleton()
        idle_info = {
            'enabled': IDLE_SCRAPE_ENABLED,
            'active': bool(getattr(idle_status, 'active', False)),
            'code': getattr(idle_status, 'current_code', None),
            'number': getattr(idle_status, 'current_number', None),
            'empty_streak': getattr(idle_status, 'empty_streak', 0),
        }

        # Compute last_processed for display: prefer an in-progress idle task, else use max last_processed
        try:
            last_proc = None
            if idle_info['code']:
                from www.models import IdleTasks
                inprog = IdleTasks.query.filter_by(kod_wydzialu=idle_info['code'], status='in_progress').first()
                if inprog and inprog.last_processed is not None:
                    last_proc = inprog.last_processed
                else:
                    max_lp = db.session.query(db.func.max(IdleTasks.last_processed)).filter(IdleTasks.kod_wydzialu == idle_info['code']).scalar()
                    if max_lp is not None:
                        last_proc = int(max_lp)
            idle_info['last_processed'] = last_proc
        except Exception:
            idle_info['last_processed'] = None
    except Exception:
        idle_info = {
            'enabled': IDLE_SCRAPE_ENABLED,
            'active': False,
            'code': None,
            'number': None,
            'empty_streak': 0,
        }

    # Add downloaded count for idle info if a code is present
    if idle_info.get('code'):
        try:
            # Query Informacje count for this department code
            count = db.session.query(db.func.count(Informacje.id)).filter(
                Informacje.ksiega.like(f"{idle_info['code']}-%")
            ).scalar() or 0
            idle_info['downloaded_count'] = count
        except Exception as e:
            print(f"Error counting downloaded books for idle info: {e}")
            idle_info['downloaded_count'] = 0
    else:
        idle_info['downloaded_count'] = 0

    # Compute number of found books for the current in-progress task so UI can show found vs not-found
    found_count = None
    if current_task:
        try:
            # Use SQL to extract the numeric part from ksiega: split_part(ksiega, '-', 2)
            # and cast to integer to count how many existing Informacje.ksiega entries fall into the task's range
            found_count = db.session.query(db.func.count(Informacje.id)).filter(
                Informacje.ksiega.like(f"{current_task.department_code}-%"),
                db.cast(db.func.split_part(Informacje.ksiega, '-', 2), db.Integer).between(current_task.start_from, current_task.end_at)
            ).scalar() or 0
        except Exception:
            # Fallback: fetch matching ksiega values and count in Python (safer if DB lacks split_part)
            try:
                rows = db.session.query(Informacje.ksiega).filter(Informacje.ksiega.like(f"{current_task.department_code}-%")).all()
                cnt = 0
                for (ks,) in rows:
                    try:
                        parts = ks.split('-')
                        num = int(parts[1])
                        if current_task.start_from <= num <= current_task.end_at:
                            cnt += 1
                    except Exception:
                        continue
                found_count = cnt
            except Exception:
                found_count = 0

    return render_template(
        'task_queue.html',
        tasks=pagination,
        current_task=current_task,
        found_count=found_count,
        department_codes=department_codes,
        pending_count=pending_count,
        in_progress_count=in_progress_count,
        completed_count=completed_count,
        failed_count=failed_count,
        queue_manager_running=queue_manager_running,
        workers=workers_list,
        total_workers=len(workers_list),
        idle_info=idle_info,
        idle_tasks=idle_tasks,
        idle_pending_count=idle_pending_count,
        idle_in_progress_count=idle_in_progress_count,
        idle_completed_count=idle_completed_count,
        idle_failed_count=idle_failed_count
    )

# Redirect /task_queue to /zadania for backward compatibility
@app.route('/task_queue', methods=['GET'])
@login_required
@admin_required
def task_queue_redirect():
    return redirect(url_for('zadania'))


@app.route('/add_to_queue', methods=['POST'])
@login_required
@admin_required
def add_to_queue():
    """Add a new scraping task to the queue"""
    department_code = request.form['department_code']
    start_from = int(request.form.get('start_from', 0))
    end_at = int(request.form.get('end_at', 999999))
    priority = int(request.form.get('priority', 0))
    
    # Create a new task in the queue
    new_task = TaskQueue(
        department_code=department_code,
        start_from=start_from,
        end_at=end_at,
        priority=priority,
        status='pending',
        books_total=end_at - start_from + 1,
        books_processed=0
    )
    
    db.session.add(new_task)
    db.session.commit()
    
    return redirect(url_for('zadania'))

@app.route('/start_queue_manager', methods=['POST'])
@login_required
@admin_required
def start_queue_manager():
    """Start the queue manager task if it's not already running"""
    # timeout=1.0 bounds the blocking control RPC so this POST can't hang >120s
    inspector = celery.control.inspect(timeout=1.0)
    
    # First check if Celery workers are running
    try:
        active_workers = inspector.ping() or {}
        if not active_workers:
            flash("Error: No Celery workers are running. Please start Celery workers first.", "error")
            return redirect(url_for('zadania'))
    except Exception as e:
        flash(f"Error connecting to Celery: {str(e)}", "error")
        return redirect(url_for('zadania'))
    
    # Then check if queue manager is already running
    try:
        active_tasks = inspector.active() or {}
        queue_manager_running = False
        for worker_name, tasks in active_tasks.items():
            for task in tasks:
                if task.get('name') == 'scraping.queue_manager':
                    queue_manager_running = True
                    break
        
        if queue_manager_running:
            flash("Queue manager is already running.", "info")
        else:
            # Start the queue manager
            try:
                task = queue_manager_task.apply_async()
                flash(f"Queue manager started successfully with task ID: {task.id}", "success")
                # Wait a brief moment to allow the task to start
                time.sleep(1)
            except Exception as e:
                flash(f"Failed to start queue manager: {str(e)}", "error")
    except Exception as e:
        flash(f"Error checking task status: {str(e)}", "error")
    
    return redirect(url_for('zadania'))

@app.route('/cancel_queued_task/<int:task_id>', methods=['POST'])
@login_required
@admin_required
def cancel_queued_task(task_id):
    """Cancel a pending task in the queue"""
    task = TaskQueue.query.get_or_404(task_id)
    
    if task.status == 'pending':
        task.status = 'cancelled'
        db.session.commit()
    
    return redirect(url_for('zadania'))

@app.route('/requeue_task/<int:task_id>', methods=['POST'])
@login_required
@admin_required
def requeue_task(task_id):
    """Requeue a failed, completed, or stopped task by setting status to pending"""
    task = TaskQueue.query.get_or_404(task_id)
    if task.status in ['failed', 'completed', 'cancelled', 'stopped']:
        if task.status == 'completed':
            # Reset progress for completed tasks
            task.books_processed = 0
            task.last_scraped_book = None
            # Delete associated ScrapingProgress entry if exists
            if task.scraping_progress:
                db.session.delete(task.scraping_progress)
        # For incomplete tasks, keep progress and ScrapingProgress
        task.status = 'pending'
        db.session.commit()
        flash(f"Task {task_id} has been requeued.", "success")
    return redirect(url_for('zadania'))

@app.route('/stop_queued_task/<int:task_id>', methods=['POST'])
@login_required
@admin_required
def stop_queued_task(task_id):
    """Stop an in-progress task in the queue"""
    task = TaskQueue.query.get_or_404(task_id)
    
    if task.status == 'in_progress':
        # Mark the task as stopping - it will change to stopped when the worker actually stops
        task.status = 'stopping'
        db.session.commit()
        flash(f"Task {task_id} is stopping. Progress will continue to be saved until the task fully stops.", "warning")
    
    return redirect(url_for('zadania'))

@app.route('/delete_queued_task/<int:task_id>', methods=['POST'])
@login_required
@admin_required
def delete_queued_task(task_id):
    """Delete a task from the queue if it's stopped, completed, failed, or cancelled"""
    task = TaskQueue.query.get_or_404(task_id)
    
    # Only allow deletion of tasks that are not running
    if task.status in ['stopped', 'completed', 'failed', 'cancelled']:
        db.session.delete(task)
        db.session.commit()
        flash(f"Task {task_id} has been deleted.", "success")
    else:
        flash(f"Cannot delete task {task_id} because it is {task.status}.", "warning")
    
    return redirect(url_for('zadania'))


@app.route('/proxies', methods=['GET'])
@login_required
@admin_required
def proxy_list():
    """Display the list of proxies with options to add and remove"""
    # Initialize scraper to access proxy methods
    scraper = setup_scraper()

    # Check for initialization error
    if getattr(scraper, "initialization_error", None):
        flash(f"Proxy system error: {scraper.initialization_error}", "danger")
        return render_template(
            'proxy_list.html',
            proxies=[],
            direct_count=0,
            proxy_count=0,
            test_mode=False,
            error_message=scraper.initialization_error
        )

    proxies = scraper.get_all_proxies()
    direct_connections = [p for p in proxies if p.is_direct]
    real_proxies = [p for p in proxies if not p.is_direct]
    test_mode = os.getenv('TEST_MODE', '').lower() in ('true', '1', 'yes', 'y')
    return render_template(
        'proxy_list.html',
        proxies=proxies,
        direct_count=len(direct_connections),
        proxy_count=len(real_proxies),
        test_mode=test_mode
    )


@app.route('/add_proxy', methods=['POST'])
@login_required
@admin_required
def add_proxy():
    """Add a new proxy to the database"""
    # Get form data
    host = request.form.get('host', '').strip()
    port = request.form.get('port', '').strip()
    username = request.form.get('username', '').strip() or None
    password = request.form.get('password', '').strip() or None
    
    # Validate required fields
    if not host or not port:
        flash("Host and port are required.", "danger")
        return redirect(url_for('proxy_list'))
    
    # Initialize scraper
    scraper = setup_scraper()
    
    # Add the proxy
    result = scraper.add_proxy(host, port, username, password)
    
    if result['success'] == '1':
        flash(f"Proxy {host}:{port} added successfully.", "success")
    else:
        flash(f"Failed to add proxy: {result.get('code', 'unknown error')}", "danger")
    
    return redirect(url_for('proxy_list'))

@app.route('/delete_proxy/<proxy_id>', methods=['POST'])
@login_required
@admin_required
def delete_proxy(proxy_id):
    """Delete a proxy from the database"""
    # Initialize scraper
    scraper = setup_scraper()
    
    # Delete the proxy
    result = scraper.delete_proxy(proxy_id)
    
    if result['success'] == '1':
        flash("Proxy deleted successfully.", "success")
    else:
        flash(f"Failed to delete proxy: {result.get('code', 'unknown error')}", "danger")
    
    return redirect(url_for('proxy_list'))

@app.route('/reset_proxies', methods=['POST'])
@login_required
@admin_required
def reset_proxies():
    """Reset all proxies to not in use"""
    # Initialize scraper
    scraper = setup_scraper()
    
    # Reset proxies
    result = scraper.reset_proxies()
    
    if result['success'] == '1':
        flash("All proxies reset successfully.", "success")
    else:
        flash("Failed to reset proxies.", "danger")
    
    return redirect(url_for('proxy_list'))



def generate_random_password(length=12):
    """Generate a random password"""
    characters = string.ascii_letters + string.digits + "!@#$%&*"
    password = ''.join(secrets.choice(characters) for _ in range(length))
    return password

@app.route('/admin')
@login_required
@admin_required
def admin_panel():
    """Admin panel for user management"""
    from sqlalchemy.orm import joinedload
    users = User.query.options(joinedload(User.prefix_permissions)).all()
    available_prefixes = get_available_prefixes()
    return render_template('admin_panel.html', users=users, available_prefixes=available_prefixes)

@app.route('/admin/create_user', methods=['POST'])
@login_required
@admin_required
def create_user():
    """Create a new user"""
    login = request.form['login'].strip()
    password = request.form.get('password', '').strip()
    is_admin = request.form.get('is_admin') == 'on'
    
    if not login:
        flash('Nazwa użytkownika jest wymagana.', 'danger')
        return redirect(url_for('admin_panel'))
    
    # Check if user already exists
    existing_user = get_user_by_login(login)
    if existing_user:
        flash('Użytkownik o tej nazwie już istnieje.', 'danger')
        return redirect(url_for('admin_panel'))
    
    # Generate password if not provided
    if not password:
        password = generate_random_password()
        generated_password = True
    else:
        generated_password = False
    
    # Create new user
    new_user = User(login=login, is_admin=is_admin)
    new_user.set_password(password)
    
    db.session.add(new_user)
    db.session.commit()
    
    if generated_password:
        flash(f'Użytkownik {login} został utworzony. Wygenerowane hasło: {password}', 'success')
    else:
        flash(f'Użytkownik {login} został utworzony.', 'success')
    
    return redirect(url_for('admin_panel'))

@app.route('/admin/change_password/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def change_password(user_id):
    """Change user password"""
    user = User.query.get_or_404(user_id)
    new_password = request.form.get('new_password', '').strip()
    
    # Generate password if not provided
    if not new_password:
        new_password = generate_random_password()
        generated_password = True
    else:
        generated_password = False
    
    user.set_password(new_password)
    db.session.commit()
    
    if generated_password:
        flash(f'Hasło dla użytkownika {user.login} zostało zmienione. Nowe hasło: {new_password}', 'success')
    else:
        flash(f'Hasło dla użytkownika {user.login} zostało zmienione.', 'success')
    
    return redirect(url_for('admin_panel'))

@app.route('/admin/toggle_admin/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def toggle_admin(user_id):
    """Toggle admin status for user"""
    user = User.query.get_or_404(user_id)
    
    # Don't allow removing admin rights from current user
    if user.id == current_user.id:
        flash('Nie możesz zmienić swoich własnych uprawnień administratora.', 'warning')
        return redirect(url_for('admin_panel'))
    
    user.is_admin = not user.is_admin
    db.session.commit()
    
    status = "administratorem" if user.is_admin else "zwykłym użytkownikiem"
    flash(f'Użytkownik {user.login} jest teraz {status}.', 'success')
    
    return redirect(url_for('admin_panel'))

@app.route('/admin/delete_user/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def delete_user(user_id):
    """Delete user"""
    user = User.query.get_or_404(user_id)
    
    # Don't allow deleting current user
    if user.id == current_user.id:
        flash('Nie możesz usunąć swojego własnego konta.', 'warning')
        return redirect(url_for('admin_panel'))
    
    login = user.login
    db.session.delete(user)
    db.session.commit()
    
    flash(f'Użytkownik {login} został usunięty.', 'success')
    return redirect(url_for('admin_panel'))

@app.route('/admin/assign_prefix/<int:user_id>', methods=['POST'])
@login_required
@admin_required
def assign_prefix(user_id):
    """Assign a prefix permission to a user"""
    user = User.query.get_or_404(user_id)
    prefix = request.form.get('prefix', '').strip()
    
    if not prefix:
        flash('Prefix nie może być pusty.', 'error')
        return redirect(url_for('admin_panel'))
    
    # Check if permission already exists
    existing = UserPrefixPermission.query.filter_by(user_id=user_id, prefix=prefix).first()
    if existing:
        flash(f'Użytkownik {user.login} już ma dostęp do prefiksu {prefix}.', 'warning')
        return redirect(url_for('admin_panel'))
    
    # Add new permission
    permission = UserPrefixPermission(user_id=user_id, prefix=prefix)
    db.session.add(permission)
    db.session.commit()
    
    flash(f'Przyznano użytkownikowi {user.login} dostęp do prefiksu {prefix}.', 'success')
    return redirect(url_for('admin_panel'))

@app.route('/admin/remove_prefix/<int:permission_id>', methods=['POST'])
@login_required
@admin_required
def remove_prefix(permission_id):
    """Remove a prefix permission from a user"""
    permission = UserPrefixPermission.query.get_or_404(permission_id)
    user = permission.user
    prefix = permission.prefix
    
    db.session.delete(permission)
    db.session.commit()
    
    flash(f'Usunięto dostęp użytkownika {user.login} do prefiksu {prefix}.', 'success')
    return redirect(url_for('admin_panel'))

@app.route('/admin/update_user_prefixes', methods=['POST'])
@login_required
@admin_required
def update_user_prefixes():
    """Update all prefix permissions for a user at once (AJAX)"""
    data = request.get_json()
    user_id = data.get('user_id')
    prefixes = data.get('prefixes', [])

    user = User.query.get_or_404(user_id)

    # Remove all current permissions
    UserPrefixPermission.query.filter_by(user_id=user_id).delete()

    # Add new permissions
    for prefix in prefixes:
        db.session.add(UserPrefixPermission(user_id=user_id, prefix=prefix))
    db.session.commit()

    return jsonify({'success': True})

def get_available_prefixes():
    """Get all unique department prefixes from the database"""
    # Get all unique prefixes by extracting the first part before the dash from ksiega field
    results = db.session.query(Informacje.ksiega).filter(
        Informacje.ksiega != 'Brak',
        Informacje.ksiega.like('%-%')
    ).distinct().all()
    
    prefixes = set()
    for result in results:
        ksiega = result[0]
        if '-' in ksiega:
            prefix = ksiega.split('-')[0]
            if len(prefix) >= 3:  # Only consider valid prefixes
                prefixes.add(prefix)
    
    return sorted(list(prefixes))

from bs4 import BeautifulSoup

def sanitize_html(html):
    """Remove unwanted <link> and <script> tags from HTML before serving in iframe."""
    soup = BeautifulSoup(html, "html.parser")
    # Remove all <link> tags except our injected kw.css
    for link in soup.find_all("link", href=True):
        href = link["href"]
        if not href.startswith("/static/kw.css"):
            link.decompose()
    # Remove all <script> tags
    for script in soup.find_all("script"):
        script.decompose()
    # Optionally, remove <img> or other tags if needed
    return str(soup)

@app.route('/api/refresh_book/<ksiega>', methods=['POST'])
@login_required
@admin_required
def refresh_book(ksiega):
    """Trigger an immediate, single-book scrape outside the queue in a background thread.

    This will not enqueue a TaskQueue item or use Celery. It will create a dedicated
    scraper, run the download_worker for the specific book, and return immediately.
    """
    from threading import Thread
    from helper import get_formatted_book_number, get_control_digit
    from kwscraper import setup_scraper, download_worker

    # ksiega expected in form PREFIX-NUMBER-CHECK or PREFIX-NUMBER
    # If user passed full ksiega (e.g. PO1P-00000001-2) try to parse department and number
    parts = ksiega.split('-')
    if len(parts) < 2:
        return jsonify({'error': 'Nieprawidłowy format księgi'}), 400

    department_code = parts[0]
    # The numeric part might be at index 1
    numeric_part = parts[1]

    def worker_thread(department_code, numeric_part, ksiega_full):
        try:
            scraper = setup_scraper()
        except Exception as e:
            print(f"Refresh book: failed to setup scraper: {e}")
            return

        try:
            try:
                ekw_number = int(numeric_part)
            except ValueError:
                # If numeric part contains leading zeros or is full formatted, strip non-digits
                import re
                digits = re.sub(r'\D', '', numeric_part)
                ekw_number = int(digits) if digits else 0

            # Call download_worker directly for this single book
            print(f"Refresh worker started for {ksiega_full}")
            download_worker(scraper, department_code, ekw_number)
            print(f"Refresh worker finished for {ksiega_full}")
        except Exception as e:
            print(f"Error refreshing book {ksiega_full}: {e}")
        finally:
            try:
                scraper.close()
            except Exception:
                pass

    t = Thread(target=worker_thread, args=(department_code, numeric_part, ksiega), daemon=True)
    t.start()

    return jsonify({'success': True, 'message': 'Scrapowanie księgi zostało uruchomione'}), 202


def parse_date(date_str):
    """Convert dd/mm/yyyy or d/m/yyyy to yyyy-mm-dd for SQL."""
    if not date_str or not isinstance(date_str, str):
        return None
    import re
    import datetime
    date_str = date_str.strip()
    match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', date_str)
    if match:
        day, month, year = match.groups()
        try:
            return datetime.date(int(year), int(month), int(day)).isoformat()
        except Exception:
            return None
    return None

if __name__ == '__main__':
    app.run(debug=False, host="0.0.0.0", port=8000)

