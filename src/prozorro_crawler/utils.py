import dateutil.parser

from prozorro_crawler.settings import PUBLIC_API_HOST, CRAWLER_USER_AGENT, BASE_URL, API_TOKEN
from prozorro_crawler.storage.base import (
    BACKWARD_OFFSET_KEY,
    FORWARD_OFFSET_KEY,
    EARLIEST_DATE_MODIFIED_KEY,
    LATEST_DATE_MODIFIED_KEY,
)

SERVER_ID_COOKIE_NAME = "SERVER_ID"

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


def get_session_server_id(session, request_url=PUBLIC_API_HOST):
    filtered = session.cookie_jar.filter_cookies(request_url)
    server_id_cookie = filtered.get(SERVER_ID_COOKIE_NAME)
    if server_id_cookie:
        return server_id_cookie.value


def get_offset_key(descending=False):
    return BACKWARD_OFFSET_KEY if descending else FORWARD_OFFSET_KEY


def get_date_modified_key(descending=False):
    return EARLIEST_DATE_MODIFIED_KEY if descending else LATEST_DATE_MODIFIED_KEY


def parse_dt_string(dt_string):
    return dateutil.parser.parse(dt_string)
