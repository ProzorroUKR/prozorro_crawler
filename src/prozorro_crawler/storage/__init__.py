from prozorro_crawler.settings import POSTGRES_HOST, MONGODB_URL
from prozorro_crawler.storage.base import *

if MONGODB_URL:
    from prozorro_crawler.storage.mongodb import *
elif POSTGRES_HOST:
    from prozorro_crawler.storage.postgres import *
else:
    raise RuntimeError("Either mongo or postgres required to store crawler position")
