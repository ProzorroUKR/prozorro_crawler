from prozorro_crawler.settings import POSTGRES_HOST
from prozorro_crawler.storage.base import (
    BACKWARD_OFFSET_KEY,
    FORWARD_OFFSET_KEY,
    EARLIEST_DATE_MODIFIED_KEY,
    LATEST_DATE_MODIFIED_KEY,
)

if POSTGRES_HOST:
    from prozorro_crawler.storage.postgres import (
        close_connection,
        save_feed_position,
        get_feed_position,
        drop_feed_position,
    )
else:
    from prozorro_crawler.storage.mongodb import (
        close_connection,
        save_feed_position,
        get_feed_position,
        drop_feed_position,
    )

__all__ = (
    "BACKWARD_OFFSET_KEY",
    "FORWARD_OFFSET_KEY",
    "EARLIEST_DATE_MODIFIED_KEY",
    "LATEST_DATE_MODIFIED_KEY",
    "close_connection",
    "save_feed_position",
    "get_feed_position",
    "drop_feed_position",
)
