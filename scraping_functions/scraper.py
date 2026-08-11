import hashlib
import logging
import os
import random
import re

# Import the Proxy model
import sys
import time
import traceback
from pathlib import Path

from dotenv import load_dotenv

# Import DrissionPage
from DrissionPage import ChromiumOptions, ChromiumPage

# Import SQLAlchemy dependencies
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_dir)
from www.models import Proxy, db
from scraping_functions.errors import (
    CaptchaError,
    CircuitOpenError,
    DataError,
    NetworkError,
    log_error,
)
from scraping_functions.resilience import CircuitBreaker, backoff_delay, retry_external_call
from scraping_functions.session_manager import BrowserSessionManager, DEFAULT_HEADERS

# Load environment variables
load_dotenv()

# Consistent User-Agent for DrissionPage
# Random User-Agent Rotation
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
]

# Rate limiting configuration
RATE_LIMIT_ENABLED = os.getenv("RATE_LIMIT_ENABLED", "true").lower() in (
    "true",
    "1",
    "yes",
    "y",
)
RATE_LIMIT_MIN = float(os.getenv("RATE_LIMIT_MIN", "2.0"))
RATE_LIMIT_MAX = float(os.getenv("RATE_LIMIT_MAX", "4.0"))

logger = logging.getLogger(__name__)

# Shared by all scraper instances. Once failures cross the threshold, workers
# pause together; after the cooldown, a single worker probes site recovery.
GOVERNMENT_SITE_CIRCUIT = CircuitBreaker(
    "ekw-government-site",
    failure_threshold=int(os.getenv("CB_THRESHOLD", "5")),
    recovery_timeout=int(os.getenv("CB_RESET_SECONDS", "60")),
)
EXTERNAL_RETRY_ATTEMPTS = int(os.getenv("EXTERNAL_RETRY_ATTEMPTS", "3"))
EXTERNAL_RETRY_BASE = float(os.getenv("CB_BASE", "1"))
EXTERNAL_RETRY_MAX = float(os.getenv("CB_MAX", "30"))
ELEMENT_TIMEOUT = float(os.getenv("ELEMENT_TIMEOUT", "10"))
PAGE_LOAD_TIMEOUT = float(os.getenv("PAGE_LOAD_TIMEOUT", "45"))
SCRIPT_TIMEOUT = float(os.getenv("SCRIPT_TIMEOUT", "20"))
RESULT_TIMEOUT = float(os.getenv("RESULT_TIMEOUT", "15"))
SESSION_MAX_REQUESTS = int(os.getenv("SESSION_MAX_REQUESTS", "20"))
SESSION_MAX_AGE_SECONDS = int(os.getenv("SESSION_MAX_AGE_SECONDS", "900"))

# Anti-detection delays — zredukowane: residential proxy z auto IP zmienia IP na każdy request,
# więc agresywne opóźnienia anty-detekcyjne są zbędne.
BOOK_DELAY_MIN = float(os.getenv("BOOK_DELAY_MIN", "0.5"))
BOOK_DELAY_MAX = float(os.getenv("BOOK_DELAY_MAX", "1.5"))

SECTION_DELAY_MIN = float(os.getenv("SECTION_DELAY_MIN", "0.2"))
SECTION_DELAY_MAX = float(os.getenv("SECTION_DELAY_MAX", "0.5"))

SEARCH_DELAY_MIN = float(os.getenv("SEARCH_DELAY_MIN", "0.1"))
SEARCH_DELAY_MAX = float(os.getenv("SEARCH_DELAY_MAX", "0.3"))


def create_proxy_auth_extension(host, port, username, password, scheme="http"):
    """
    Creates a Chrome extension to handle proxy authentication.
    Returns the path to the extension directory.
    """
    # Create unique path based on ALL credentials to avoid collisions with sticky sessions (same host:port)
    creds_hash = hashlib.md5(f"{host}{port}{username}{password}".encode()).hexdigest()[
        :8
    ]
    plugin_path = f"/tmp/proxy_auth_plugin_{host}_{port}_{creds_hash}"
    os.makedirs(plugin_path, exist_ok=True)

    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version": "22.0.0"
    }
    """

    background_js = f"""
    var config = {{
        mode: "fixed_servers",
        rules: {{
            singleProxy: {{
                scheme: "{scheme}",
                host: "{host}",
                port: parseInt({port})
            }},
            bypassList: ["localhost"]
        }}
    }};

    chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

    function callbackFn(details) {{
        return {{
            authCredentials: {{
                username: "{username}",
                password: "{password}"
            }}
        }};
    }}

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {{urls: ["<all_urls>"]}},
                ['blocking']
    );
    """

    with open(os.path.join(plugin_path, "manifest.json"), "w") as f:
        f.write(manifest_json)

    with open(os.path.join(plugin_path, "background.js"), "w") as f:
        f.write(background_js)

    return plugin_path


def detect_incapsula(content):
    """Detect real Incapsula/Imperva block page (specific patterns, not just 'Incapsula' string anywhere)"""
    if not isinstance(content, str):
        return False

    # Prawdziwa blokada Incapsula ma charakterystyczne tytuły strony
    title_check = "<title>Sorry, you have been blocked</title>" in content.lower()

    # Lub specyficzny tekst blokady
    incident_check = "Request unsuccessful. Incapsula incident ID" in content

    # Lub widoczny JS od Incapsula który pojawia się TYLKO na stronie blokady
    resource_check = "_Incapsula_Resource" in content

    if title_check or incident_check or resource_check:
        print(
            f"🔴 INCAPSULA BLOCK: Real block page detected (title={title_check}, incident={incident_check}, resource={resource_check})"
        )
        return True

    return False


def create_fresh_browser(proxy=None, user_agent=None, headers=None):
    """
    Create a fresh browser instance for each scraping session.
    This ensures unique fingerprint per session and proper proxy routing.
    """
    proxy_host = proxy.host if proxy and not proxy.is_direct else "direct"
    print(f"DEBUG: Creating FRESH browser (Proxy: {proxy_host})", flush=True)

    co = ChromiumOptions().headless(True)
    co.auto_port()
    co.set_argument("--no-sandbox")
    co.set_argument("--disable-dev-shm-usage")

    # The session manager keeps this identity stable for several books, then
    # deliberately rotates it when the bounded browser session is replaced.
    user_agent = user_agent or random.choice(USER_AGENTS)
    co.set_user_agent(user_agent)
    co.set_argument("--lang=pl-PL,pl,en-US,en")

    # ===== INCAPSULA/IMPERVA BYPASS SETTINGS =====
    co.set_argument("--disable-blink-features=AutomationControlled")
    co.set_argument("--disable-infobars")
    co.set_argument(
        "--disable-features=AutomationControlled,IsolateOrigins,site-per-process"
    )
    co.set_argument("--enable-features=NetworkService,NetworkServiceInProcess")

    # Randomize window size slightly
    width = random.randint(1280, 1920)
    height = random.randint(800, 1080)
    co.set_argument(f"--window-size={width},{height}")
    # ==============================================

    # NOTE: imagesEnabled=false and disable-remote-fonts REMOVED
    # These are strong automation signals detected by Incapsula
    co.mute(True)

    # Timeouts
    co.set_timeouts(
        base=ELEMENT_TIMEOUT,
        page_load=PAGE_LOAD_TIMEOUT,
        script=SCRIPT_TIMEOUT,
    )

    # Handle proxy configuration
    if proxy and not proxy.is_direct and proxy.host:
        try:
            extension_path = create_proxy_auth_extension(
                proxy.host, proxy.port, proxy.username, proxy.password
            )
            co.add_extension(extension_path)
            print(f"DEBUG: Added proxy extension from {extension_path}", flush=True)
        except Exception as pe:
            print(f"DEBUG: Failed to create/add proxy extension: {pe}", flush=True)
            proxy_str = (
                f"http://{proxy.username}:{proxy.password}@{proxy.host}:{proxy.port}"
            )
            co.set_proxy(proxy_str)

    browser = ChromiumPage(co, timeout=PAGE_LOAD_TIMEOUT)
    browser.set.headers(headers or DEFAULT_HEADERS)

    # ===== INJECT STEALTH JS VIA CDP — runs BEFORE every page load =====
    try:
        stealth_js = """
        // webdriver — return false (not undefined, which is suspicious)
        Object.defineProperty(navigator, 'webdriver', {
            get: () => false,
            configurable: true
        });

        // Remove CDP (Chrome DevTools Protocol) markers
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
        delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
        for (let prop in window) {
            if (prop.match && (prop.match(/^cdc_/) || prop.match(/\\$cdc_/))) {
                try { delete window[prop]; } catch(e) {}
            }
        }

        // Fix chrome.runtime (headless Chrome is missing this)
        if (window.chrome && !window.chrome.runtime) {
            window.chrome.runtime = {
                connect: () => {},
                sendMessage: () => {},
                id: undefined
            };
        }

        // Fake plugins array — real Chrome has at least PDF viewer
        Object.defineProperty(navigator, 'plugins', {
            get: () => {
                const plugins = [0, 1, 2, 3, 4];
                plugins.refresh = () => {};
                return plugins;
            }
        });

        // Languages must match the --lang argument
        Object.defineProperty(navigator, 'languages', {
            get: () => ['pl-PL', 'pl', 'en-US', 'en']
        });

        // Hide WebGL SwiftShader renderer (headless indicator)
        const getParameterOrig = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(parameter) {
            if (parameter === 37445) return 'Intel Inc.';
            if (parameter === 37446) return 'Intel Iris OpenGL Engine';
            return getParameterOrig.call(this, parameter);
        };

        // Spoof permissions API
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) => {
            if (parameters.name === 'notifications') {
                return Promise.resolve({ state: Notification.permission });
            }
            return originalQuery(parameters);
        };
        """
        # CDP method: script persists across ALL page navigations
        browser.run_cdp("Page.addScriptToEvaluateOnNewDocument", source=stealth_js)
        print(
            "DEBUG: Stealth JS injected via CDP (persists across navigations)",
            flush=True,
        )
    except Exception as e:
        # Fallback to run_js if CDP method fails
        print(
            f"DEBUG: CDP stealth injection failed ({e}), falling back to run_js",
            flush=True,
        )
        try:
            browser.run_js(stealth_js)
        except:
            pass

    return browser


# Human-like interaction helpers
def human_typing(element, text):
    """Simulate human typing with random delays between keystrokes"""
    element.clear()
    for char in text:
        element.input(char, clear=False)
        time.sleep(random.uniform(0.01, 0.03))  # 10-30ms — rapid but not instant


def human_move_and_click(tab, element):
    """Simulate human mouse movement and click"""
    # Random small hover delay before action
    time.sleep(random.uniform(0.05, 0.15))

    # In DrissionPage we can use actions to move
    # We simply move to the element first
    try:
        element.hover()
    except:
        # Fallback if hover fails (e.g. element covered), try JS scroll
        pass

    # Tiny hesitation before clicking
    time.sleep(random.uniform(0.02, 0.08))
    element.click()


def process_tab_scraping(tab, code, number, digit, view_type="current"):
    """Core scraping logic using a browser tab"""
    try:
        print(f"DEBUG: Starting tab scraping for {code}/{number}/{digit}", flush=True)

        # 1. Navigate to menu
        def load_menu():
            try:
                tab.get(
                    "https://ekw.ms.gov.pl/eukw_ogol/menu.do",
                    timeout=PAGE_LOAD_TIMEOUT,
                )
            except Exception as error:
                raise NetworkError(
                    "Government site menu could not be loaded",
                    operation="government_site.load_menu",
                    context={"book": f"{code}/{number}/{digit}"},
                ) from error

        try:
            retry_external_call(
                load_menu,
                operation="government_site.load_menu",
                attempts=EXTERNAL_RETRY_ATTEMPTS,
                base_delay=EXTERNAL_RETRY_BASE,
                max_delay=EXTERNAL_RETRY_MAX,
                jitter=0.25,
                circuit_breaker=GOVERNMENT_SITE_CIRCUIT,
                record_success=False,
                logger=logger,
            )
        except CircuitOpenError as error:
            log_error(logger, error)
            return {
                "success": "0",
                "code": "circuit-open",
                "error_details": str(error),
                "retry_after": error.retry_after,
            }
        except NetworkError as e:
            print(
                f"🚨 CONNECTION ERROR: Cannot load menu page. Reason: {e}", flush=True
            )
            return {"success": "0", "code": "connection-error", "error_details": str(e)}

        # Wait for Incapsula JS challenge to resolve (up to 10s)
        # Incapsula often serves a JS challenge that auto-solves in a real browser
        challenge_wait_start = time.time()
        challenge_resolved = False
        while time.time() - challenge_wait_start < 10:
            page_html = tab.html
            if not detect_incapsula(page_html):
                challenge_resolved = True
                break
            print(
                f"⏳ Incapsula challenge detected, waiting for resolution... ({time.time() - challenge_wait_start:.0f}s)",
                flush=True,
            )
            time.sleep(2)

        if not challenge_resolved:
            page_html = tab.html
            if detect_incapsula(page_html):
                print("🚨 INCAPSULA: Challenge did not resolve after 10s.", flush=True)
                return {"success": "0", "code": "incapsula-block"}

        # Re-read page HTML after challenge wait
        page_html = tab.html

        # Check if page is empty or blocked
        if len(page_html) < 500:
            print(
                f"🚨 EMPTY PAGE: Server response too short ({len(page_html)} chars).",
                flush=True,
            )
            return {
                "success": "0",
                "code": "empty-page",
                "error_details": f"Page too short: {len(page_html)} chars",
            }

        if "The requested URL was rejected" in page_html:
            print("🚨 REJECTED: Server rejected connection (WAF/Firewall).", flush=True)
            return {"success": "2", "code": "rejected"}

        if not tab.ele("text:Przeglądanie księgi wieczystej", timeout=10):
            print(
                "🚨 MENU NOT FOUND: 'Przeglądanie księgi wieczystej' element not found.",
                flush=True,
            )
            return {
                "success": "0",
                "code": "connection-error",
                "error_details": "Menu element not found",
            }

        tab.ele("text:Przeglądanie księgi wieczystej").click()

        # 2. Search Form (with retries)
        for attempt in range(3):
            try:
                if tab.ele("#kryteriaWKW", timeout=10):
                    print(f"✅ Search form found (attempt {attempt + 1}/3)", flush=True)
                    break
                print(
                    f"⚠️ Search form not found (attempt {attempt + 1}/3). Retrying...",
                    flush=True,
                )
                if tab.ele("text:Przeglądanie księgi wieczystej", timeout=2):
                    tab.ele("text:Przeglądanie księgi wieczystej").click()
                elif attempt == 1:
                    print("⚠️ Refreshing page...", flush=True)
                    tab.refresh()
            except Exception as retry_err:
                print(f"⚠️ Error during attempt {attempt + 1}: {retry_err}", flush=True)
            if attempt < 2:
                delay = backoff_delay(
                    attempt + 1,
                    base=EXTERNAL_RETRY_BASE,
                    maximum=EXTERNAL_RETRY_MAX,
                    jitter=0.25,
                )
                logger.warning(
                    "external_call_retry operation=government_site.search_form "
                    "attempt=%s/3 delay_seconds=%.2f",
                    attempt + 1,
                    delay,
                )
                time.sleep(delay)
        else:
            print(f"🚨 CRITICAL: Search form not found after 3 attempts.", flush=True)
            print(f"🔍 DEBUG CONTEXT: URL={tab.url}, Title='{tab.title}'", flush=True)
            # Print snippet of HTML to identify if it's a block page
            html_snippet = tab.html[:1000].replace("\n", " ")
            print(f"📄 HTML SNIPPET: {html_snippet}", flush=True)

            if "Incapsula" in html_snippet or "barier" in str(tab.title):
                return {"success": "0", "code": "incapsula-block"}

            return {
                "success": "0",
                "code": "connection-error",
                "error_details": "Search form not found after 3 retries",
            }

        # 3. Fill form
        if tab.ele("#kodWydzialuInput"):
            human_typing(tab.ele("#kodWydzialuInput"), code)
        else:
            return {
                "success": "0",
                "code": "connection-error",
                "error_details": "Code input missing",
            }

        human_typing(tab.ele("#numerKsiegiWieczystej"), number)
        human_typing(tab.ele("#cyfraKontrolna"), digit)

        human_move_and_click(tab, tab.ele("#wyszukaj"))

        # 4. Wait for results
        found = False
        start = time.time()
        max_wait = RESULT_TIMEOUT

        while time.time() - start < max_wait:
            # Handle page refresh errors gracefully
            try:
                html = tab.html
            except Exception as e:
                if "refreshed" in str(e).lower() or "ContextLost" in str(
                    type(e).__name__
                ):
                    print(
                        f"⏳ Page refreshing, waiting... ({e.__class__.__name__})",
                        flush=True,
                    )
                    time.sleep(1)
                    continue
                raise  # Re-raise other exceptions

            # Check for blocking first
            if detect_incapsula(html):
                print("🚨 INCAPSULA: Block detected during search.", flush=True)
                return {"success": "0", "code": "incapsula-block"}

            if "The requested URL was rejected" in html:
                print("🚨 REJECTED: WAF detected during search.", flush=True)
                return {"success": "2", "code": "rejected"}

            # Check for error messages in the error container
            error_box = tab.ele("#komunikatyBledow", timeout=0.1)
            if error_box and error_box.text.strip():
                error_text = error_box.text.strip()
                print(f"📋 DATABASE MESSAGE: {error_text}", flush=True)
                if (
                    "nie została odnaleziona" in error_text
                    or "nie odnaleziona" in error_text
                ):
                    return {
                        "success": "0",
                        "code": "not-found",
                        "error_details": error_text,
                    }
                return {
                    "success": "0",
                    "code": "not-found",
                    "error_details": error_text,
                }

            # Check for text in HTML (fallback)
            if (
                "nie została odnaleziona" in html
                or "nie występuje w Centralnej Bazie Danych" in html
            ):
                print(f"📋 BOOK NOT FOUND: {code}/{number}/{digit}", flush=True)
                return {"success": "0", "code": "not-found"}

            # SUCCESS CONDITION:
            # We only consider it "found" if the "Wydruk zwykły" button is present OR the "Przeglądanie aktualnej treści" link is present
            # This avoids false positives on the "Not Found" page which shares the same header.
            if tab.ele("#przyciskWydrukZwykly", timeout=0.1) or tab.ele(
                'button[name="przyciskWydrukZwykly"]', timeout=0.1
            ):
                found = True
                break

            # Sometimes we might land on a results list (multiple matches? rare for exact number)
            # or we need to click "Przeglądanie aktualnej treści"
            if tab.ele("text:Przeglądanie aktualnej treści KW", timeout=0.1):
                print("ℹ️ Found results list, clicking 'Przeglądanie...'", flush=True)
                tab.ele("text:Przeglądanie aktualnej treści KW").click()
                # Loop continues to wait for the button on next page

            time.sleep(0.5)

        if not found:
            print(
                f"⏱️ TIMEOUT: Search for {code}/{number}/{digit} timed out (Success button not found).",
                flush=True,
            )
            # For debugging, let's print a small snippet of the page text if we time out
            try:
                # content_snippet = tab.ele('body').text[:500].replace('\n', ' ')
                # print(f"DEBUG PAGE CONTENT: {content_snippet}...", flush=True)
                pass
            except:
                pass

                return {
                    "success": "0",
                    "code": "not-found",
                    "error_details": "Search result timed out or success element missing",
                }

        # Add random delay after finding results to simulate human reading
        time.sleep(random.uniform(SEARCH_DELAY_MIN, SEARCH_DELAY_MAX))

        # 5. Extract sections using FORM SUBMISSION (not JS fetch - ensures proxy routing)
        print("DEBUG: Extracting sections via form navigation...", flush=True)

        if view_type == "current":
            wydruk_btn = tab.ele('button[name="przyciskWydrukZwykly"]') or tab.ele(
                "#przyciskWydrukZwykly"
            )
            if not wydruk_btn:
                print(
                    "🚨 STRUCTURE CHANGED: 'przyciskWydrukZwykly' button not found.",
                    flush=True,
                )
                return {"success": "0", "code": "structure-changed"}

            # Store main page HTML
            main_html = tab.html

            # Get form values needed for section requests
            try:
                uuid_val = tab.ele('input[name="uuid"]').attr("value") or ""
                kod_val = tab.ele('input[name="kodWydzialu"]').attr("value") or ""
                nr_val = tab.ele('input[name="nrKw"]').attr("value") or ""
                cyfra_val = tab.ele('input[name="cyfraK"]').attr("value") or ""
            except Exception as e:
                print(f"🚨 Cannot get form values: {e}", flush=True)
                return {
                    "success": "0",
                    "code": "structure-changed",
                    "error_details": str(e),
                }

            if not uuid_val:
                print("🚨 UUID not found in form", flush=True)
                return {
                    "success": "0",
                    "code": "structure-changed",
                    "error_details": "UUID not found",
                }

            sections = ["DIO", "DIS", "DII", "DIII", "DIV"]
            section_html = {}

            for section_name in sections:
                try:
                    # Human-like delay between sections (Optimized)
                    time.sleep(random.uniform(SECTION_DELAY_MIN, SECTION_DELAY_MAX))

                    # Navigate to section by loading URL with POST via JS
                    # This goes through the browser -> through proxy extension
                    js_submit = f"""
                    return (function() {{
                        const form = document.createElement('form');
                        form.method = 'POST';
                        form.action = '/eukw_prz/KsiegiWieczyste/pokazWydruk';
                        form.style.display = 'none';

                        const addField = (name, value) => {{
                            const input = document.createElement('input');
                            input.type = 'hidden';
                            input.name = name;
                            input.value = value;
                            form.appendChild(input);
                        }};

                        addField('uuid', '{uuid_val}');
                        addField('kodWydzialu', '{kod_val}');
                        addField('nrKw', '{nr_val}');
                        addField('cyfraK', '{cyfra_val}');
                        addField('dzialKsiegi', '{section_name}');
                        addField('przyciskWydrukZwykly', '');

                        document.body.appendChild(form);
                        form.submit();
                        return 'submitted';
                    }})();
                    """
                    tab.run_js(js_submit)

                    # Wait for page to load (load_start już czeka — sleep był zbędny)
                    tab.wait.load_start(timeout=PAGE_LOAD_TIMEOUT)

                    # Get section HTML
                    section_content = tab.html

                    # Check for Incapsula block on this section
                    if detect_incapsula(section_content):
                        print(
                            f"🚨 INCAPSULA: Block detected on section {section_name}",
                            flush=True,
                        )
                        return {
                            "success": "0",
                            "code": "incapsula-block",
                            "error_details": f"Blocked on {section_name}",
                        }

                    section_html[section_name] = section_content
                    print(
                        f"✅ Section {section_name} fetched ({len(section_content)} chars)",
                        flush=True,
                    )

                    # Go back to results page for next section
                    tab.back()
                    # Back navigation delay
                    time.sleep(random.uniform(0.1, 0.2))

                except Exception as section_err:
                    print(
                        f"⚠️ Error fetching section {section_name}: {section_err}",
                        flush=True,
                    )
                    section_html[section_name] = ""
                    # Try to recover
                    try:
                        tab.back()
                        time.sleep(1)
                    except:
                        pass

            print("✅ SUCCESS: All sections retrieved via navigation.", flush=True)
            response = {
                "success": "1",
                "main": main_html,
                "zeroth": section_html.get("DIO", ""),
                "first": section_html.get("DIS", ""),
                "second": section_html.get("DII", ""),
                "third": section_html.get("DIII", ""),
                "fourth": section_html.get("DIV", ""),
            }

            return response

        return {"success": "0", "code": "structure-changed"}

    except Exception as e:
        print(f"🚨 EXCEPTION IN TAB: {e}")
        traceback.print_exc()
        return {"success": "0", "code": "error", "error_details": str(e)}


class Scraper:
    def __init__(self, db_url=None, timeout=180, extractor_path=None, splash_url=None):
        """
        Initialize the Scraper with database connectivity

        Args:
            db_url: Database connection URL
            timeout: Maximum time to keep trying proxies
            extractor_path: Deprecated (kept for compatibility)
            splash_url: Deprecated (kept for compatibility)
        """
        self.initialization_error = None
        self.is_initialized = False
        try:
            self.timeout = timeout

            # Initialize SQLAlchemy session
            db_url = db_url or os.getenv("DATABASE_URL")
            if not db_url:
                raise ValueError(
                    "Database URL not provided or found in environment variables."
                )

            logger.info(
                "database_connection_started",
                extra={"context": {"configured": True}},
            )
            self.engine = create_engine(db_url)
            self.Session = scoped_session(sessionmaker(bind=self.engine))
            self.session_manager = BrowserSessionManager(
                create_fresh_browser,
                USER_AGENTS,
                max_requests=SESSION_MAX_REQUESTS,
                max_age_seconds=SESSION_MAX_AGE_SECONDS,
                logger=logger,
            )
            print("Database connection successful")

            # URLs used for scraping (kept for reference)
            self.pre_search_url = "https://ekw.ms.gov.pl/eukw_ogol/menu.do"
            self.search_url = "https://przegladarka-ekw.ms.gov.pl/eukw_prz/KsiegiWieczyste/wyszukiwanieKW"
            self.book_url = "https://przegladarka-ekw.ms.gov.pl/eukw_prz/KsiegiWieczyste/pokazWydruk"

            self.is_initialized = True
            print("Scraper initialized with DrissionPage (no Splash required)")

        except Exception as e:
            self.initialization_error = str(e)
            print(f"Scraper initialization failed: {self.initialization_error}")
            self.engine = None
            self.Session = None
            self.session_manager = None

    # ------- Proxy management methods -------

    def get_all_proxies(self):
        """Get all proxies in the database"""
        session = self.Session()
        try:
            return session.query(Proxy).all()
        finally:
            session.close()

    def add_proxy(self, host, port, username, password):
        """Add a new proxy to the database"""
        session = self.Session()
        try:
            proxy_id = self._hash_proxy(host, port, username, password)

            if session.query(Proxy).filter_by(id=proxy_id).count() > 0:
                return {"success": "0", "code": "duplicate"}

            new_proxy = Proxy(
                id=proxy_id,
                host=host,
                port=port,
                username=username,
                password=password,
                in_use=False,
                call_count=0,
                failure_count=0,
                cookies_valid=False,
            )
            session.add(new_proxy)
            session.commit()

            return {"success": "1", "id": proxy_id}
        finally:
            session.close()

    def delete_proxy(self, proxy_id):
        """Delete a proxy from the database"""
        session = self.Session()
        try:
            proxy = session.query(Proxy).filter_by(id=proxy_id).first()
            if not proxy:
                return {"success": "0", "code": "not-found"}

            session.delete(proxy)
            session.commit()
            return {"success": "1"}
        finally:
            session.close()

    def reset_proxies(self):
        """Reset all proxies to initial state"""
        print("[Scraper] Executing proxy reset...")
        session = self.Session()
        try:
            session.query(Proxy).update(
                {
                    "in_use": False,
                    "failure_count": 0,
                    "cookies_valid": False,
                    "cookies": [],
                }
            )
            session.commit()
            return {"success": "1"}
        finally:
            session.close()

    def add_dummy_direct_connection(self):
        """Add a dummy direct connection for TEST_MODE operation"""
        session = self.Session()
        try:
            proxy_id = "direct-connection-no-proxy"

            existing_proxy = session.query(Proxy).filter_by(id=proxy_id).first()
            if existing_proxy:
                session.delete(existing_proxy)

            new_proxy = Proxy(
                id=proxy_id,
                host="direct-connection",
                port="0",
                username="no-proxy",
                password="no-proxy",
                is_direct=True,
                in_use=False,
                call_count=0,
                failure_count=0,
                cookies_valid=False,
            )
            session.add(new_proxy)
            session.commit()

            print("Added dummy direct connection for testing mode")
            return {"success": "1", "id": proxy_id}
        finally:
            session.close()

    def _hash_proxy(self, host, port, username, password):
        """Generate a unique hash for a proxy"""
        key = f"{host}:{port}:{username or ''}:{password or ''}"
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    # ------- Main public method for scraping -------

    def scrape_book(self, code, number, digit):
        """
        High-level method to scrape a book using DrissionPage
        Reuses a bounded browser session so cookies and headers remain stable,
        then rotates the session periodically or after a block/network failure.

        Args:
            code: Department code
            number: Book number
            digit: Control digit

        Returns:
            Scraped data or error information
        """
        session = self.Session()
        proxy = None
        browser = None

        try:
            start_time = time.time()
            retry_count = 0
            max_retries = EXTERNAL_RETRY_ATTEMPTS

            # Check for available proxies
            all_proxies = session.query(Proxy).all()
            if not all_proxies:
                return {
                    "success": "0",
                    "code": "no-proxies",
                    "message": "No proxies available. Please add at least one proxy.",
                }

            while time.time() - start_time < self.timeout and retry_count < max_retries:
                # Get available proxy
                proxy = self._get_valid_proxy(session)
                if proxy is None:
                    time.sleep(1.0)
                    continue

                retry_count += 1
                invalidate_session = False

                # Anti-detection: staggered delay between books
                delay = random.uniform(BOOK_DELAY_MIN, BOOK_DELAY_MAX)
                print(
                    f"⏳ Anti-detection delay: {delay:.1f}s before next book...",
                    flush=True,
                )
                time.sleep(delay)

                try:
                    print(
                        f"🔄 STARTING: {code}/{number}/{digit} | Proxy: {proxy.host}",
                        flush=True,
                    )

                    # Reuse cookies, headers, and browser identity for a bounded
                    # session. Rotation occurs by age/request count or on blocking.
                    browser = self.session_manager.acquire(proxy)
                    if not browser or not browser.process_id:
                        error = NetworkError(
                            "Browser process could not be started",
                            operation="government_site.create_browser",
                            context={"book": f"{code}/{number}/{digit}"},
                        )
                        log_error(logger, error)
                        self._increment_failure_count(proxy, session)
                        self.release_proxy(proxy, session)
                        time.sleep(
                            backoff_delay(
                                retry_count,
                                base=EXTERNAL_RETRY_BASE,
                                maximum=EXTERNAL_RETRY_MAX,
                                jitter=0.25,
                            )
                        )
                        continue

                    # Scrape using the browser directly (no tabs - simpler)
                    result = process_tab_scraping(browser, code, number, digit)

                    # Handle results
                    if result["success"] == "2":  # Rejected/Blocked
                        invalidate_session = True
                        error = CaptchaError(
                            "Government site rejected the request",
                            operation="government_site.scrape_book",
                            context={"book": f"{code}/{number}/{digit}"},
                        )
                        GOVERNMENT_SITE_CIRCUIT.record_success()
                        log_error(logger, error)
                        self._increment_failure_count(proxy, session)
                        self.release_proxy(proxy, session)
                        time.sleep(
                            backoff_delay(
                                retry_count,
                                base=EXTERNAL_RETRY_BASE,
                                maximum=EXTERNAL_RETRY_MAX,
                                jitter=0.25,
                            )
                        )
                        continue

                    elif result["success"] == "0" and result.get("code") == "not-found":
                        print(
                            f"📋 NOT FOUND: Book {code}/{number}/{digit} does not exist",
                            flush=True,
                        )
                        GOVERNMENT_SITE_CIRCUIT.record_success()
                        self.release_proxy(proxy, session)
                        return result

                    elif result["success"] == "1":
                        print(
                            f"✅ SUCCESS: Book {code}/{number}/{digit} downloaded",
                            flush=True,
                        )
                        GOVERNMENT_SITE_CIRCUIT.record_success()
                        self.release_proxy(proxy, session)
                        return result

                    elif result.get("code") == "incapsula-block":
                        invalidate_session = True
                        error = CaptchaError(
                            "Government site presented an Incapsula challenge",
                            operation="government_site.scrape_book",
                            context={"book": f"{code}/{number}/{digit}"},
                        )
                        GOVERNMENT_SITE_CIRCUIT.record_success()
                        log_error(logger, error)
                        self._increment_failure_count(proxy, session)
                        self.release_proxy(proxy, session)
                        time.sleep(
                            backoff_delay(
                                retry_count,
                                base=EXTERNAL_RETRY_BASE,
                                maximum=EXTERNAL_RETRY_MAX,
                                jitter=0.25,
                            )
                        )
                        continue

                    elif result.get("code") == "circuit-open":
                        # An open circuit is a site-wide pause, not a failed book.
                        # Do not consume this book's retry budget while waiting for
                        # the single half-open recovery probe.
                        retry_count = max(0, retry_count - 1)
                        retry_after = max(0.1, float(result.get("retry_after", 1)))
                        print(
                            f"Government-site circuit open; retrying after {retry_after:.1f}s",
                            flush=True,
                        )
                        time.sleep(retry_after)
                        continue

                    # Other failure
                    failure_code = result.get("code", "unknown")
                    print(
                        f"❌ FAILURE: {code}/{number}/{digit} | Code: {failure_code}",
                        flush=True,
                    )
                    self._increment_failure_count(proxy, session)
                    self.release_proxy(proxy, session)
                    error_type = (
                        DataError
                        if failure_code == "structure-changed"
                        else NetworkError
                    )
                    error = error_type(
                        f"Government site returned failure code '{failure_code}'",
                        operation="government_site.scrape_book",
                        context={"book": f"{code}/{number}/{digit}", "result_code": failure_code},
                    )
                    if isinstance(error, NetworkError):
                        invalidate_session = True
                        GOVERNMENT_SITE_CIRCUIT.record_failure()
                    else:
                        GOVERNMENT_SITE_CIRCUIT.record_success()
                    log_error(logger, error)
                    if error.retryable:
                        time.sleep(
                            backoff_delay(
                                retry_count,
                                base=EXTERNAL_RETRY_BASE,
                                maximum=EXTERNAL_RETRY_MAX,
                                jitter=0.25,
                            )
                        )
                    else:
                        return result

                except Exception as e:
                    invalidate_session = True
                    error = NetworkError(
                        "Unexpected failure while calling the government site",
                        operation="government_site.scrape_book",
                        context={
                            "book": f"{code}/{number}/{digit}",
                            "cause": type(e).__name__,
                        },
                    )
                    GOVERNMENT_SITE_CIRCUIT.record_failure()
                    log_error(logger, error, exc_info=True)
                    if proxy:
                        self._increment_failure_count(proxy, session)
                        self.release_proxy(proxy, session)
                    time.sleep(
                        backoff_delay(
                            retry_count,
                            base=EXTERNAL_RETRY_BASE,
                            maximum=EXTERNAL_RETRY_MAX,
                            jitter=0.25,
                        )
                    )

                finally:
                    # Persist cookies after every book. Broken or challenged
                    # sessions are discarded so poisoned state is never reused.
                    if browser:
                        if invalidate_session:
                            self.session_manager.invalidate(proxy)
                        else:
                            self.session_manager.checkpoint(proxy)
                        browser = None

                    if proxy:
                        self._increment_call_count(proxy, session)

                    try:
                        session.commit()
                    except Exception as e:
                        print(f"Error committing: {e}")
                        session.rollback()

            if retry_count >= max_retries:
                return {"success": "0", "code": "max-retries-exceeded"}

            return {"success": "0", "code": "timeout"}

        finally:
            if proxy:
                self.release_proxy(proxy, session)
            try:
                session.commit()
            except:
                session.rollback()
            session.close()

    def _get_valid_proxy(self, session=None):
        """Get a proxy from the database.

        Residential proxy z auto IP — lockowanie jest zbędne, bo każde połączenie
        dostaje nowe IP. `with_for_update` tylko sztucznie ograniczało throughput.
        """
        if session is None:
            session = self.Session()

        # Weź proxy z najmniejszą liczbą błędów — bez locka
        proxy = (
            session.query(Proxy).order_by(Proxy.failure_count, Proxy.call_count).first()
        )

        if proxy:
            return proxy

        return None

    def release_proxy(self, proxy, session=None):
        """Release a proxy after use"""
        if session is None:
            session = self.Session()
            close_session = True
        else:
            close_session = False
        try:
            proxy.in_use = False
            session.commit()
        finally:
            if close_session:
                session.close()

    def _increment_failure_count(self, proxy, session=None):
        """Increment the proxy failure counter"""
        if session is None:
            session = self.Session()
            close_session = True
        else:
            close_session = False
        try:
            proxy.failure_count += 1
            if close_session:
                session.commit()
        finally:
            if close_session:
                session.close()

    def _increment_call_count(self, proxy, session=None):
        """Increment the proxy call counter"""
        if session is None:
            session = self.Session()
            close_session = True
        else:
            close_session = False
        try:
            proxy.call_count += 1
            if close_session:
                session.commit()
        finally:
            if close_session:
                session.close()

    def close(self):
        """Close the reusable browser session owned by this scraper."""
        if self.session_manager:
            self.session_manager.close()

    def __del__(self):
        """Clean up resources when object is destroyed"""
        self.close()

    def check_site_health(self):
        """Check if the scraped site is available"""
        try:
            import requests

            def request_health():
                try:
                    response = requests.get(
                        self.pre_search_url,
                        timeout=(min(5, PAGE_LOAD_TIMEOUT), PAGE_LOAD_TIMEOUT),
                    )
                    if response.status_code >= 500:
                        raise NetworkError(
                            f"Government site returned HTTP {response.status_code}",
                            operation="government_site.health",
                            context={"status_code": response.status_code},
                        )
                    return response
                except requests.RequestException as error:
                    raise NetworkError(
                        "Government site health request failed",
                        operation="government_site.health",
                    ) from error

            response = retry_external_call(
                request_health,
                operation="government_site.health",
                attempts=EXTERNAL_RETRY_ATTEMPTS,
                base_delay=EXTERNAL_RETRY_BASE,
                max_delay=EXTERNAL_RETRY_MAX,
                jitter=0.25,
                circuit_breaker=GOVERNMENT_SITE_CIRCUIT,
                logger=logger,
            )
            if response.status_code == 200:
                if "przerwa serwisowa" in response.text:
                    GOVERNMENT_SITE_CIRCUIT.trip()
                    return {
                        "status": "maintenance",
                        "message": "Site is in maintenance mode",
                    }
                return {"status": "ok", "message": "Site is available"}
            return {
                "status": "error",
                "message": f"Site returned status {response.status_code}",
            }
        except CircuitOpenError as error:
            log_error(logger, error)
            return {
                "status": "circuit-open",
                "message": str(error),
                "retry_after": error.retry_after,
            }
        except NetworkError as error:
            log_error(logger, error)
            return {"status": "error", "message": str(error)}
