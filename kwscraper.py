import concurrent.futures
import logging
import os
import queue
import random
import shutil
import statistics
import sys
import threading
import time
from datetime import datetime, timedelta

import requests
import tenacity
from dotenv import load_dotenv

# Add parent directory to sys.path to ensure imports work correctly
parent_dir = os.path.dirname(os.path.abspath(__file__))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# Import after setting up the path
from helper import get_control_digit, get_formatted_book_number
from kwparser import parse_directory
from scraping_functions.scraper import Scraper
from scraping_functions.errors import DataError, NetworkError, log_error

load_dotenv()
logger = logging.getLogger(__name__)

# Get environment variables - now using DATABASE_URL instead of MONGO_URI
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://localhost/kwscraper",
)
TEST_MODE = os.getenv("TEST_MODE", "").lower() in ("true", "1", "yes", "y")
print(f"Running in TEST_MODE: {TEST_MODE}")  # Print test mode status
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUTS_DIRNAME = os.path.join(CURRENT_DIR, "ekw")

if not os.path.exists(OUTPUTS_DIRNAME):
    os.makedirs(OUTPUTS_DIRNAME)

# Create timing statistics storage
timing_stats = {
    "start_time": None,
    "end_time": None,
    "book_timings": [],
    "success_count": 0,
    "failure_count": 0,
    "not_found_count": 0,
}

# Queue for downloaded books ready to be parsed
parsing_queue = queue.Queue()
# Event to signal when processing is complete
processing_done_event = threading.Event()
# Lock for thread-safe operations
lock = threading.Lock()
# Global counter for tracking download progress
download_counter = {"processed": 0, "success": 0, "failed": 0, "not_found": 0}
# Global counter for tracking parsing progress
parsing_counter = {"processed": 0, "success": 0, "failed": 0}
# Maximum number of concurrent processes - now configurable via env vars
# Increased conservative defaults to better utilize a 4 vCPU machine.
# Adjust via environment variables if you see resource contention.
MAX_DOWNLOAD_WORKERS = int(os.getenv("DOWNLOAD_WORKERS", "6"))  # Download workers
MAX_PARSING_WORKERS = int(os.getenv("PARSING_WORKERS", "3"))  # Parse workers
def save_page_source(department_code, book_number, control_digit, section, page_source):
    """Save scraped HTML content to a file (sanitized)"""
    directory_name = f"{department_code}-{book_number}-{control_digit}"
    output_dir = os.path.join(OUTPUTS_DIRNAME, directory_name)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    filename = os.path.join(output_dir, f"{section}.html")
    # Sanitize HTML before saving
    cleaned_html = page_source
    with open(filename, "w", encoding="utf-8") as file:
        file.write(cleaned_html)


def save_debug_html(book_id, page_source):
    """Best-effort diagnostic capture that never hides the original site error."""
    safe_book_id = "".join(
        character for character in str(book_id) if character.isalnum() or character in "-_"
    )
    try:
        debug_directory = os.path.join(OUTPUTS_DIRNAME, "debug")
        os.makedirs(debug_directory, exist_ok=True)
        with open(
            os.path.join(debug_directory, f"{safe_book_id}.html"),
            "w",
            encoding="utf-8",
        ) as debug_file:
            debug_file.write(page_source)
    except OSError as error:
        log_error(
            logger,
            DataError(
                "Debug response HTML could not be saved",
                operation="scraper.save_debug_html",
                context={"book": safe_book_id, "cause": type(error).__name__},
            ),
            exc_info=True,
        )


def setup_scraper():
    """Set up and configure the scraper with proxy if not in test mode"""
    try:
        # Initialize scraper with PostgreSQL connection (DrissionPage - no Splash needed)
        scraper = Scraper(db_url=DATABASE_URL, timeout=180)

        # Check if we have any proxies configured
        proxies = scraper.get_all_proxies()

        # Test mode takes precedence - always use dummy direct connection
        if TEST_MODE:
            # In test mode, only use dummy direct connection
            print("TEST MODE: Using direct connection without proxy")

            # Remove any existing proxies if we're in test mode
            if proxies:
                print("TEST MODE: Removing existing proxies for clean test environment")
                for proxy in proxies:
                    scraper.delete_proxy(proxy.id)

            # Add a dummy direct proxy
            scraper.add_dummy_direct_connection()
            print("TEST MODE: Direct connection added. This will NOT use a proxy.")
            print("WARNING: Using direct connections may lead to IP blocking!")

        # Normal mode - require a real proxy
        elif not proxies:
            print(
                "No proxies found. Please add at least one proxy through the web interface."
            )
            print("The scraper requires at least one proxy to function.")
            print(
                "Alternatively, set TEST_MODE=1 environment variable to bypass proxy requirement for testing."
            )

        if len(proxies) > 0:
            print(f"Found {len(proxies)} existing proxies in the database.")

        # DO NOT reset proxies here!
        # This function is called by every thread worker.
        # Resetting here causes a race condition where workers unlock duplicates for each other.
        # scraper.reset_proxies()

        # Verify we have at least one proxy available (unless in TEST_MODE)
        available_proxies = scraper.get_all_proxies()
        if not available_proxies:
            print("ERROR: No proxies available after setup.")

        print(
            f"Scraper configured with {len(available_proxies)} {'direct connections' if TEST_MODE else 'proxies'}"
        )
        return scraper

    except ConnectionError as e:
        error = NetworkError(
            "Scraper setup could not connect to an external service",
            operation="scraper.setup",
            context={"cause": type(e).__name__},
        )
        log_error(logger, error, exc_info=True)
        raise error from e
    except Exception as e:
        error = DataError(
            "Scraper setup failed while loading configuration or proxy data",
            operation="scraper.setup",
            context={"cause": type(e).__name__},
        )
        log_error(logger, error, exc_info=True)
        raise error from e


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=1, max=8),
    stop=tenacity.stop_after_attempt(3),
)
def run_scraper(
    scraper, department_code: str, ekw_number, start_from: int = 0, end_at: int = 999999
):
    """
    Run the scraper for a single book with behavior matching the original implementation

    This function is designed to behave like the original Selenium-based function
    but using our new Scraper class instead
    """
    book_start_time = time.time()
    book_result = "failure"  # Default status

    try:
        # Format book number and control digit (same as original)
        book_number = get_formatted_book_number(str(ekw_number))
        control_digit = str(get_control_digit(department_code, book_number))

        print(f"Scraping {department_code}-{book_number}-{control_digit}")

        # Use the Scraper class to get book data
        # This handles all the form filling, submission and page navigation
        # that was previously done with Selenium
        result = scraper.scrape_book(department_code, book_number, control_digit)

        if result["success"] == "0":
            # Handle "not found" case
            if result.get("code") == "not-found":
                print(f"Book {department_code}-{book_number}-{control_digit} not found")
                book_result = "not_found"  # Set result for timing stats
                return True  # Return True so we continue to next book

            # Handle other errors
            print(f"Error scraping book: {result.get('code', 'unknown error')}")

            # Save debug HTML if available
            if "html" in result and result["html"]:
                save_debug_html(
                    f"{department_code}-{book_number}-{control_digit}", result["html"]
                )

            book_result = "failure"  # Set result for timing stats
            # These would have raised exceptions in the original function
            # to trigger retry mechanism
            raise Exception(f"Scraper error: {result.get('code', 'unknown')}")

        elif result["success"] == "1":
            # Book found, save all sections
            try:
                # Save main search results page (equivalent to the initial page in original)
                if "main" in result and result["main"]:
                    save_page_source(
                        department_code,
                        book_number,
                        control_digit,
                        "main",
                        result["main"],
                    )

                # Save all sections (same as in original)
                sections = [
                    ("zeroth", "DIO"),
                    ("first", "DIS"),
                    ("second", "DII"),
                    ("third", "DIII"),
                    ("fourth", "DIV"),
                ]

                for key, section_name in sections:
                    if key in result and result[key]:
                        save_page_source(
                            department_code,
                            book_number,
                            control_digit,
                            section_name,
                            result[key],
                        )

                print(
                    f"Successfully scraped and saved book {department_code}-{book_number}-{control_digit}"
                )
                book_result = "success"  # Set result for timing stats
                return True

            except Exception as e:
                error = DataError(
                    "Scraped book sections could not be saved",
                    operation="scraper.save_sections",
                    context={
                        "department_code": department_code,
                        "book_number": str(ekw_number),
                        "cause": type(e).__name__,
                    },
                )
                log_error(logger, error, exc_info=True)
                book_result = "failure"  # Set result for timing stats
                # Original would have continued to next book in this case
                return True

        else:
            # Unexpected status - would have raised exception in original
            book_result = "failure"  # Set result for timing stats
            raise Exception(f"Unexpected result status: {result['success']}")

    except Exception as e:
        # This matches the original's outermost exception handling
        print(f"run_scraper error: {e}")
        book_result = "failure"  # Set result for timing stats
        raise  # Re-raise to trigger tenacity retry, just like original
    finally:
        # Record timing stats if in test mode
        if TEST_MODE:
            elapsed_time = time.time() - book_start_time
            timing_stats["book_timings"].append(
                {
                    "ekw_number": ekw_number,
                    "department_code": department_code,
                    "elapsed_time": elapsed_time,
                    "result": book_result,
                }
            )

            # Update success/failure counts
            if book_result == "success":
                timing_stats["success_count"] += 1
            elif book_result == "not_found":
                timing_stats["not_found_count"] += 1
            else:
                timing_stats["failure_count"] += 1

            print(
                f"Book {ekw_number} completed in {elapsed_time:.2f} seconds with result: {book_result}"
            )


def cleanup_local_folder(directory_path):
    """
    Remove local folder after successful parsing

    Args:
        directory_path: Path to the directory to remove
    """
    try:
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
            print(f"[Cleanup] Removed local folder: {directory_path}")
    except Exception as e:
        print(f"[Cleanup] Error removing folder {directory_path}: {e}")


def download_worker(scraper, department_code, ekw_number):
    """Worker function for downloading and parsing a single book"""
    try:
        # Format book number and control digit
        book_number = get_formatted_book_number(str(ekw_number))
        control_digit = str(get_control_digit(department_code, book_number))
        book_id = f"{department_code}-{book_number}-{control_digit}"
        directory_name = book_id
        directory_path = os.path.join(OUTPUTS_DIRNAME, directory_name)

        # Scrape the book data
        result = scraper.scrape_book(department_code, book_number, control_digit)

        if result["success"] == "0":
            # Handle "not found" case
            if result.get("code") == "not-found":
                with lock:
                    download_counter["not_found"] += 1
                    download_counter["processed"] += 1
                # Return True for not-found books to treat them as processed
                return True

            # Handle other errors
            print(
                f"[Downloader] Error scraping book {book_id}: {result.get('code', 'unknown error')}"
            )

            # Save debug HTML if available
            if "html" in result and result["html"]:
                save_debug_html(book_id, result["html"])

            with lock:
                download_counter["failed"] += 1
                download_counter["processed"] += 1
            return None

        elif result["success"] == "1":
            # Book found, save all sections to files
            try:
                # Save main search results page
                if "main" in result and result["main"]:
                    save_page_source(
                        department_code,
                        book_number,
                        control_digit,
                        "main",
                        result["main"],
                    )

                # Save all sections
                sections = [
                    ("zeroth", "DIO"),
                    ("first", "DIS"),
                    ("second", "DII"),
                    ("third", "DIII"),
                    ("fourth", "DIV"),
                ]

                for key, section_name in sections:
                    if key in result and result[key]:
                        save_page_source(
                            department_code,
                            book_number,
                            control_digit,
                            section_name,
                            result[key],
                        )

                # Parse the book data
                try:
                    print(
                        f"[Downloader] Calling parse_directory for {directory_name}..."
                    )
                    parse_success = parse_directory(directory_name)
                    print(
                        f"[Downloader] parse_directory finished for {directory_name} with result: {parse_success}"
                    )

                    if parse_success:
                        with lock:
                            parsing_counter["success"] += 1
                            parsing_counter["processed"] += 1
                            cleanup_local_folder(directory_path)
                    else:
                        print(
                            f"[Downloader] Parsing failed (no KW found) for {book_id}."
                        )
                        with lock:
                            parsing_counter["failed"] += 1
                            parsing_counter["processed"] += 1
                        # Cleanup invalid data
                        cleanup_local_folder(directory_path)
                        return None  # Return failure

                except Exception as e:
                    print(f"[Parser] Error parsing book {book_id}: {e}")
                    with lock:
                        parsing_counter["failed"] += 1
                        parsing_counter["processed"] += 1
                    return None

                # Update counters
                with lock:
                    download_counter["success"] += 1
                    download_counter["processed"] += 1

                return True  # Indicates success

            except Exception as e:
                print(f"[Downloader] Error saving book {book_id}: {e}")
                with lock:
                    download_counter["failed"] += 1
                    download_counter["processed"] += 1
                return None

    except Exception as e:
        print(f"[Downloader] Unexpected error for book {ekw_number}: {e}")
        with lock:
            download_counter["failed"] += 1
            download_counter["processed"] += 1
        return None


def print_timing_report():
    """Print a detailed timing report"""
    if not timing_stats["start_time"] or not timing_stats["end_time"]:
        print("No timing data available")
        return

    # Calculate total elapsed time
    total_time = timing_stats["end_time"] - timing_stats["start_time"]

    # Get successful book timings
    success_timings = [
        book["elapsed_time"]
        for book in timing_stats["book_timings"]
        if book["result"] == "success"
    ]

    # Prepare the report
    print("\n" + "=" * 60)
    print("TEST MODE - TIMING REPORT")
    print("=" * 60)
    print(f"Start time: {timing_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time: {timing_stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total runtime: {str(timedelta(seconds=total_time.total_seconds()))}")
    print(f"Books processed: {len(timing_stats['book_timings'])}")
    print(f"Success count: {timing_stats['success_count']}")
    print(f"Not found count: {timing_stats['not_found_count']}")
    print(f"Failure count: {timing_stats['failure_count']}")

    if success_timings:
        print("\nTiming statistics for successful scrapes:")
        print(f"Average time per book: {statistics.mean(success_timings):.2f} seconds")
        print(f"Median time per book: {statistics.median(success_timings):.2f} seconds")
        print(f"Minimum time: {min(success_timings):.2f} seconds")
        print(f"Maximum time: {max(success_timings):.2f} seconds")

        if len(success_timings) > 1:
            print(
                f"Standard deviation: {statistics.stdev(success_timings):.2f} seconds"
            )

        books_per_hour = 3600 / statistics.mean(success_timings)
        print(f"Estimated throughput: {books_per_hour:.1f} books per hour")

    print("=" * 60)

    # Print the slowest 5 books (if any)
    if len(timing_stats["book_timings"]) > 0:
        print("\nSlowest books:")
        sorted_timings = sorted(
            timing_stats["book_timings"], key=lambda x: x["elapsed_time"], reverse=True
        )
        for i, book in enumerate(sorted_timings[:5]):  # Show top 5 slowest
            print(
                f"{i + 1}. Book {book['department_code']}-"
                f"{get_formatted_book_number(str(book['ekw_number']))}: "
                f"{book['elapsed_time']:.2f}s ({book['result']})"
            )

    print("=" * 60)


def main():
    """Main function to run the scraper"""
    # Get user input
    department_code = input("Enter the department code (e.g., PO1P): ")
    start_from = int(input("Enter the starting book number (e.g., 0): "))
    end_at = int(input("Enter the ending book number (e.g., 1000): "))

    # Create and configure the scraper
    scraper = setup_scraper()

    try:
        # Start timing if in test mode
        if TEST_MODE:
            timing_stats["start_time"] = datetime.now()
            print(
                f"TEST MODE: Starting timing at {timing_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}"
            )

        # Start download workers - parsing is now done in the same worker
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_DOWNLOAD_WORKERS
        ) as download_executor:
            # Use a bounded in-flight window to avoid creating huge numbers of pending futures
            IN_FLIGHT_LIMIT = int(
                os.getenv("IN_FLIGHT_LIMIT", max(12, MAX_DOWNLOAD_WORKERS * 2))
            )

            pending = {}
            completed = set()
            book_iter = iter(range(start_from, end_at + 1))

            def submit_next():
                try:
                    n = next(book_iter)
                except StopIteration:
                    return None
                f = download_executor.submit(
                    download_worker, scraper, department_code, n
                )
                pending[f] = n
                return f

            # Seed initial window
            try:
                for _ in range(IN_FLIGHT_LIMIT):
                    if submit_next() is None:
                        break
            except Exception:
                pass

            # Process as futures complete, submitting new ones to keep window filled
            for fut in concurrent.futures.as_completed(list(pending.keys())):
                book_num = pending.pop(fut)
                completed.add(fut)
                try:
                    fut.result()
                except Exception as e:
                    print(f"Error processing book {book_num}: {e}")

                # Submit next to refill window
                submit_next()

    except KeyboardInterrupt:
        print("\nScraping interrupted by user.")
    finally:
        # Clean up resources
        scraper.close()

        # End timing if in test mode
        if TEST_MODE:
            timing_stats["end_time"] = datetime.now()
            print_timing_report()

        print("Scraping completed.")


if __name__ == "__main__":
    main()
