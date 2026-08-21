"""Validation for identifiers and HTML before scraped data is persisted."""

import re

from scraping_functions.errors import DataError


DEPARTMENT_RE = re.compile(r"^[A-Z0-9]{4}$")
BOOK_RE = re.compile(r"^\d{8}$")
DIGIT_RE = re.compile(r"^\d$")
BLOCK_MARKERS = (
    "request unsuccessful. incapsula incident id",
    "sorry, you have been blocked",
    "the requested url was rejected",
)
REQUIRED_RESULT_SECTIONS = ("main", "zeroth", "first", "second", "third", "fourth")


def validate_book_identifier(department_code, book_number, control_digit):
    context = {
        "department_code": str(department_code),
        "book_number": str(book_number),
        "control_digit": str(control_digit),
    }
    if not DEPARTMENT_RE.fullmatch(context["department_code"]):
        raise DataError("Invalid department code", operation="data.validate_identifier", context=context)
    if not BOOK_RE.fullmatch(context["book_number"]):
        raise DataError("Invalid book number", operation="data.validate_identifier", context=context)
    if not DIGIT_RE.fullmatch(context["control_digit"]):
        raise DataError("Invalid control digit", operation="data.validate_identifier", context=context)
    return True


def validate_html_document(content, *, section, minimum_length=200):
    if not isinstance(content, str):
        raise DataError(
            "Scraped content is not text",
            operation="data.validate_html",
            context={"section": section, "value_type": type(content).__name__},
        )
    normalized = content.strip()
    if len(normalized) < minimum_length:
        raise DataError(
            "Scraped content is unexpectedly short",
            operation="data.validate_html",
            context={"section": section, "length": len(normalized)},
        )
    lowered = normalized.lower()
    if any(marker in lowered for marker in BLOCK_MARKERS):
        raise DataError(
            "Blocked response cannot be saved as book data",
            operation="data.validate_html",
            context={"section": section},
        )
    if "<html" not in lowered and "<body" not in lowered:
        raise DataError(
            "Scraped content is not an HTML document",
            operation="data.validate_html",
            context={"section": section},
        )
    return normalized


def validate_scrape_result(result):
    if not isinstance(result, dict):
        raise DataError(
            "Scraper returned a non-object result",
            operation="data.validate_result",
            context={"value_type": type(result).__name__},
        )
    if result.get("success") != "1":
        raise DataError(
            "Only successful scraper results can be saved",
            operation="data.validate_result",
            context={"success": result.get("success"), "code": result.get("code")},
        )
    for section in REQUIRED_RESULT_SECTIONS:
        validate_html_document(result.get(section), section=section)
    return result

