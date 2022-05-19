from prozorro_crawler.settings import POSTGRES_HOST
from prozorro_crawler.storage.base import *

if POSTGRES_HOST:
    from prozorro_crawler.storage.postgres import *
else:
    from prozorro_crawler.storage.mongodb import *
