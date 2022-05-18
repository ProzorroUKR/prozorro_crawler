from prozorro_crawler.settings import CRAWLER_USER_AGENT, BASE_URL, API_TOKEN
from prozorro_crawler.storage.base import (
    BACKWARD_OFFSET_KEY,
    FORWARD_OFFSET_KEY,
    EARLIEST_DATE_MODIFIED_KEY,
    LATEST_DATE_MODIFIED_KEY,
)

DEFAULT_HEADERS = {
    "User-Agent": CRAWLER_USER_AGENT
}
if API_TOKEN:
    DEFAULT_HEADERS["Authorization"] = f"Bearer {API_TOKEN}"


def get_resource_url(resource):
    return f"{BASE_URL}/{resource}"


def get_default_headers(additional_headers):
    headers = DEFAULT_HEADERS
    if isinstance(additional_headers, dict):
        headers.update(additional_headers)
    return headers


def get_offset_key(descending=False):
    return BACKWARD_OFFSET_KEY if descending else FORWARD_OFFSET_KEY


def get_date_modified_key(descending=False):
    return EARLIEST_DATE_MODIFIED_KEY if descending else LATEST_DATE_MODIFIED_KEY
