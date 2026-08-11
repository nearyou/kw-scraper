"""Reusable browser sessions with headers, cookie persistence, and rotation."""

import logging
import random
import time

from scraping_functions.errors import NetworkError, log_error


DEFAULT_HEADERS = {
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.7,en;q=0.5",
    "Cache-Control": "no-cache",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
}
COOKIE_FIELDS = {"name", "value", "domain", "path", "expires", "httpOnly", "secure", "sameSite"}


def choose_user_agent(user_agents, previous=None, chooser=random.choice):
    choices = [agent for agent in user_agents if agent != previous] or list(user_agents)
    if not choices:
        raise ValueError("At least one user agent is required")
    return chooser(choices)


def normalize_cookies(cookies):
    normalized = []
    for cookie in cookies or []:
        if not isinstance(cookie, dict) or not cookie.get("name"):
            continue
        normalized.append({key: value for key, value in cookie.items() if key in COOKIE_FIELDS})
    return normalized


class BrowserSessionManager:
    """Own one browser session and rotate it only at safe boundaries."""

    def __init__(
        self,
        browser_factory,
        user_agents,
        *,
        max_requests=20,
        max_age_seconds=900,
        headers=None,
        clock=time.monotonic,
        logger=None,
    ):
        self.browser_factory = browser_factory
        self.user_agents = tuple(user_agents)
        self.max_requests = max(1, max_requests)
        self.max_age_seconds = max(1, max_age_seconds)
        self.headers = dict(headers or DEFAULT_HEADERS)
        self.clock = clock
        self.logger = logger or logging.getLogger(__name__)
        self.browser = None
        self.proxy_id = None
        self.user_agent = None
        self.created_at = None
        self.requests_used = 0

    def _is_reusable(self, proxy):
        if not self.browser or not getattr(self.browser, "process_id", None):
            return False
        return (
            self.proxy_id == proxy.id
            and self.requests_used < self.max_requests
            and self.clock() - self.created_at < self.max_age_seconds
        )

    def acquire(self, proxy):
        if self._is_reusable(proxy):
            return self.browser

        self.close()
        self.user_agent = choose_user_agent(self.user_agents, self.user_agent)
        try:
            browser = self.browser_factory(proxy, self.user_agent, self.headers)
            if not browser or not getattr(browser, "process_id", None):
                raise RuntimeError("browser process did not start")
            if proxy.cookies_valid and proxy.cookies:
                browser.set.cookies(normalize_cookies(proxy.cookies))
            self.browser = browser
            self.proxy_id = proxy.id
            self.created_at = self.clock()
            self.requests_used = 0
            return browser
        except Exception as error:
            typed_error = NetworkError(
                "Browser session could not be created or restored",
                operation="session.acquire",
                context={"proxy_id": str(proxy.id), "cause": type(error).__name__},
            )
            log_error(self.logger, typed_error, exc_info=True)
            self.close()
            raise typed_error from error

    def checkpoint(self, proxy):
        if not self.browser or self.proxy_id != proxy.id:
            return
        try:
            proxy.cookies = normalize_cookies(
                list(self.browser.cookies(all_domains=True, all_info=True))
            )
            proxy.cookies_valid = bool(proxy.cookies)
            self.requests_used += 1
        except Exception as error:
            proxy.cookies_valid = False
            log_error(
                self.logger,
                NetworkError(
                    "Browser cookies could not be checkpointed",
                    operation="session.checkpoint",
                    context={"proxy_id": str(proxy.id), "cause": type(error).__name__},
                ),
                exc_info=True,
            )

    def invalidate(self, proxy=None):
        if proxy is not None:
            proxy.cookies = []
            proxy.cookies_valid = False
        self.close()

    def close(self):
        browser, self.browser = self.browser, None
        self.proxy_id = None
        self.created_at = None
        self.requests_used = 0
        if browser:
            try:
                browser.quit(timeout=5, force=True)
            except Exception:
                self.logger.warning("browser_session_close_failed", exc_info=True)
