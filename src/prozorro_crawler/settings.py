import os

from pythonjsonlogger import jsonlogger
import logging


def getenv(key, default=None, callback=None):
    value = os.environ.get(key, default)
    if callback:
        callback(key, value, default)
    return value


def warn_db_conflicts_base(enabled, key, value, default):
    if enabled and value == default:
        logger.warning(
            f"Environment variable {key} "
            f"casted with default value '{default}'. "
            f"This may cause conflicts if you use "
            f"one db for many crawlers, "
            f"better rename to a specific process, "
            f"in order different crawlers don't clash."
        )


def warn_db_conflicts(key, value, default):
    warn_db_conflicts_base(True, key, value, default)


def warn_mongodb_conflicts(key, value, default):
    warn_db_conflicts_base(MONGODB_URL, key, value, default)


def warn_postgres_conflicts(key, value, default):
    warn_db_conflicts_base(POSTGRES_HOST, key, value, default)


def warn_crawler_user_agent(key, value, default):
    if value == default:
        logger.warning(
            f"Using default '{value}' as crawler user agent. "
            f"To set another user agent set {key} environment variable."
        )
    else:
        logger.info(
            f"Using '{value}' as crawler user agent."
        )


def assert_url(key, value, default):
    assert not value.endswith("/")
    assert value.startswith("http")


# logging
LOGGER_NAME = getenv("LOGGER_NAME", "PRO-ZORRO-CRAWLER")
LOG_LEVEL = int(getenv("LOG_LEVEL", logging.INFO))
logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(LOG_LEVEL)
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    '%(levelname)s %(asctime)s %(module)s %(process)d '
    '%(message)s %(pathname)s $(lineno)d $(funcName)s'
)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)

# timeouts for api calls
# every FEED_STEP_INTERVAL every crawler(backward and forward) gets API_LIMIT items
# and processes every one of them
# so this is (API_LIMIT + 1) x number of crawlers every FEED_STEP_INTERVAL seconds
FEED_STEP_INTERVAL = int(getenv("FEED_STEP_INTERVAL", 0))
TOO_MANY_REQUESTS_INTERVAL = int(getenv("TOO_MANY_REQUESTS_INTERVAL", 10))
CONNECTION_ERROR_INTERVAL = int(getenv("CONNECTION_ERROR_INTERVAL", 5))
NO_ITEMS_INTERVAL = int(getenv("NO_ITEMS_INTERVAL", 15))
GET_ERROR_RETRIES = int(getenv("GET_ERROR_RETRIES", 5))

PUBLIC_API_HOST = getenv("PUBLIC_API_HOST", "https://public-api-sandbox.prozorro.gov.ua", assert_url)
API_VERSION = getenv("API_VERSION", "2.5")
API_LIMIT = int(getenv("API_LIMIT", 100))
API_MODE = getenv("API_MODE", "_all_")
API_OPT_FIELDS = getenv("API_OPT_FIELDS", "").split(",")
API_RESOURCE = getenv("API_RESOURCE", "tenders")
API_TOKEN = getenv("API_TOKEN")
BASE_URL = f"{PUBLIC_API_HOST}/api/{API_VERSION}"

DATE_MODIFIED_LOCK_ENABLED = bool(getenv("DATE_MODIFIED_LOCK_ENABLED", False))
DATE_MODIFIED_SKIP_STATUSES = getenv("DATE_MODIFIED_SKIP_STATUSES", "").split(",")
DATE_MODIFIED_MARGIN_SECONDS = int(getenv("DATE_MODIFIED_MARGIN_SECONDS", 86400))

CRAWLER_USER_AGENT = getenv("CRAWLER_USER_AGENT", "ProZorro Crawler 2.0", warn_crawler_user_agent)

MONGODB_URL = getenv("MONGODB_URL", "")
MONGODB_DATABASE = getenv("MONGODB_DATABASE", "prozorro-crawler")
MONGODB_STATE_COLLECTION = getenv("MONGODB_STATE_COLLECTION", "prozorro-crawler-state")
MONGODB_STATE_ID = getenv("MONGODB_STATE_ID", "FEED_CRAWLER_STATE", warn_mongodb_conflicts)

POSTGRES_HOST = getenv("POSTGRES_HOST", "")
POSTGRES_PORT = int(getenv("POSTGRES_PORT", 5432))
POSTGRES_DB = getenv("POSTGRES_DB", "prozorro-crawler")
POSTGRES_USER = getenv("POSTGRES_USER", "agent")
POSTGRES_PASSWORD = getenv("POSTGRES_PASSWORD", "kalina")
POSTGRES_STATE_TABLE = getenv("POSTGRES_STATE_TABLE", "crawler_state")
POSTGRES_STATE_ID = getenv("POSTGRES_STATE_ID", "crawler_state", warn_postgres_conflicts)

DB_ERROR_INTERVAL = int(getenv("DB_ERROR_INTERVAL", 5))

# lock
LOCK_ENABLED = bool(getenv("LOCK_ENABLED", False))
LOCK_COLLECTION_NAME = getenv("LOCK_COLLECTION_NAME", "process_lock")
LOCK_EXPIRE_TIME = int(getenv("LOCK_EXPIRE_TIME", 60))
LOCK_UPDATE_TIME = int(getenv("LOCK_UPDATE_TIME", 30))
LOCK_ACQUIRE_INTERVAL = int(getenv("LOCK_ACQUIRE_INTERVAL", 10))
LOCK_PROCESS_NAME = getenv("LOCK_PROCESS_NAME", "crawler_lock", warn_db_conflicts)
