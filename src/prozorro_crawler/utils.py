from prozorro_crawler.settings import PUBLIC_API_HOST, USER_AGENT, BASE_URL

SERVER_ID_COOKIE_NAME = "SERVER_ID"

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT
}


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
