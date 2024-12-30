from datetime import datetime
from prozorro_crawler.settings import (
    PUBLIC_API_HOST,
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

SERVER_ID_COOKIE_NAME = "SERVER_ID"


def get_resource_url(resource):
    return f"{BASE_URL}/{resource}"


def get_default_headers(additional_headers):
    headers = {}
    if API_TOKEN:
        headers["Authorization"] = f"Bearer {API_TOKEN}"
    headers["User-Agent"] = CRAWLER_USER_AGENT
    if isinstance(additional_headers, dict):
        headers.update(additional_headers)
    return headers


def get_session_server_id(session, request_url=PUBLIC_API_HOST):
    filtered = session.cookie_jar.filter_cookies(request_url)
    server_id_cookie = filtered.get(SERVER_ID_COOKIE_NAME)
    if server_id_cookie:
        return server_id_cookie.value


def get_offset_key(descending=False):
    return BACKWARD_OFFSET_KEY if descending else FORWARD_OFFSET_KEY


def get_date_modified_key(descending=False):
    return EARLIEST_DATE_MODIFIED_KEY if descending else LATEST_DATE_MODIFIED_KEY


def get_offset_age(offset):
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
