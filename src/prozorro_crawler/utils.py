from datetime import datetime
from typing import Optional


from prozorro_crawler.settings import (
    CRAWLER_USER_AGENT,
    BASE_URL,
    API_TOKEN,
    TIMEZONE,
)
from prozorro_crawler.storage.base import (
    BACKWARD_OFFSET_KEY,
    FORWARD_OFFSET_KEY,
    EARLIEST_DATE_MODIFIED_KEY,
    LATEST_DATE_MODIFIED_KEY,
)


def get_resource_url(resource: str) -> str:
    return f"{BASE_URL}/{resource}"


def get_default_headers(additional_headers: Optional[dict[str, str]]) -> dict[str, str]:
    headers = {}
    if API_TOKEN:
        headers["Authorization"] = f"Bearer {API_TOKEN}"
    headers["User-Agent"] = CRAWLER_USER_AGENT
    if isinstance(additional_headers, dict):
        headers.update(additional_headers)
    return headers


def get_offset_key(descending: bool = False) -> str:
    return BACKWARD_OFFSET_KEY if descending else FORWARD_OFFSET_KEY


def get_date_modified_key(descending: bool = False) -> str:
    return EARLIEST_DATE_MODIFIED_KEY if descending else LATEST_DATE_MODIFIED_KEY


def get_offset_age(offset: str) -> Optional[float]:
    """
    Get age of offset in seconds
    Offset has format like "1735484400.068.1.87eafe2d01ade9e61f892604ade9cbde"
    Where first part is unix timestamp
    We need to calculate age of timestamp from now
    Returns:
     - None if offset has invalid format
     - Age of offset in seconds

    :param offset: str
    :return: int | None
    """
    try:
        offset_parts = offset.split(".")
        now = datetime.now(TIMEZONE).timestamp()
        timestamp = float(offset_parts[0])
        return now - timestamp
    except (IndexError, TypeError, ValueError, AttributeError):
        return None
