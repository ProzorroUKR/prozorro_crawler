from pythonjsonlogger import jsonlogger
import logging
import os

# timeouts for api calls
# every FEED_STEP_INTERVAL every crawler(backward and forward) gets API_LIMIT items
# and processes every one of them
# so this is (API_LIMIT + 1) x number of crawlers every FEED_STEP_INTERVAL seconds
FEED_STEP_INTERVAL = int(os.environ.get("FEED_STEP_INTERVAL", 0))
TOO_MANY_REQUESTS_INTERVAL = int(os.environ.get("TOO_MANY_REQUESTS_INTERVAL", 10))
CONNECTION_ERROR_INTERVAL = int(os.environ.get("CONNECTION_ERROR_INTERVAL", 5))
NO_ITEMS_INTERVAL = int(os.environ.get("CONNECTION_ERROR_INTERVAL", 15))
GET_ERROR_RETRIES = int(os.environ.get("GET_ERROR_RETRIES", 5))

LOGGER_NAME = os.environ.get("LOGGER_NAME", "PRO-ZORRO-CRAWLER")
PUBLIC_API_HOST = os.environ.get("PUBLIC_API_HOST", "https://public-api-sandbox.prozorro.gov.ua")
assert not PUBLIC_API_HOST.endswith("/")
assert PUBLIC_API_HOST.startswith("http")
API_VERSION = os.environ.get("API_VERSION", "2.5")
API_LIMIT = int(os.environ.get("API_LIMIT", 100))
API_MODE = os.environ.get("API_MODE", "_all_")
API_OPT_FIELDS = os.environ.get("API_OPT_FIELDS", "").split(",")
BASE_URL = f"{PUBLIC_API_HOST}/api/{API_VERSION}/tenders"

MONGODB_URL = os.environ.get("MONGODB_URL", "mongodb://root:example@mongo:27017")
MONGODB_DATABASE = os.environ.get("MONGODB_DATABASE", "prozorro-crawler")
MONGODB_COLLECTION = os.environ.get("MONGODB_COLLECTION", "complaints")
MONGODB_STATE_COLLECTION = os.environ.get("MONGODB_STATE_COLLECTION", "prozorro-crawler-state")
MONGODB_STATE_ID = os.environ.get("MONGODB_STATE_ID", "FEED_CRAWLER_STATE")
MONGODB_ERROR_INTERVAL = int(os.environ.get("MONGODB_ERROR_INTERVAL", 5))


# logging
LOG_LEVEL = int(os.environ.get("LOG_LEVEL", logging.INFO))
logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(LOG_LEVEL)
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    '%(levelname)s %(asctime)s %(module)s %(process)d '
    '%(message)s %(pathname)s $(lineno)d $(funcName)s'
)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


# lock
LOCK_ENABLED = bool(os.environ.get("LOCK_ENABLED", False))
LOCK_COLLECTION_NAME = os.environ.get("LOCK_COLLECTION_NAME", "process_lock")
LOCK_EXPIRE_TIME = int(os.environ.get("LOCK_EXPIRE_TIME", 60))
LOCK_UPDATE_TIME = int(os.environ.get("LOCK_UPDATE_TIME", 30))
LOCK_ACQUIRE_INTERVAL = int(os.environ.get("LOCK_ACQUIRE_INTERVAL", 10))
# if you use one MONGODB_DATABASE for many crawlers,
# better rename to a specific process, in order different crawlers don't clash
LOCK_PROCESS_NAME = os.environ.get("LOCK_PROCESS_NAME", "crawler_lock")
