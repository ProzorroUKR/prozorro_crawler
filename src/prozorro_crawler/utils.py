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
    Offset has format like "1731103209.0000000001"
    Where first part is unix timestamp, second is increment
    We need to calculate age of timestamp from now
    Returns:
     - None if offset has invalid format
     - Age of offset in seconds

    :param offset: str
    :return: int | None
    """
    try:    
        offset_parts = offset.split(".")
        if len(offset_parts) == 2:
            now = datetime.now(TIMEZONE).timestamp()
            timestamp = float(offset_parts[0])
            return now - timestamp
    except (TypeError, ValueError, AttributeError):
        pass

    return None
