from functools import lru_cache
from typing import Any, Optional

from pymongo.errors import PyMongoError
from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorCollection,
    AsyncIOMotorDatabase,
)
from prozorro_crawler.settings import (
    logger,
    MONGODB_URL,
    MONGODB_DATABASE,
    MONGODB_STATE_COLLECTION,
    MONGODB_STATE_ID,
    DB_ERROR_INTERVAL,
)
from .base import (
    BACKWARD_OFFSET_KEY,
    FORWARD_OFFSET_KEY,
)
import asyncio


async def close_connection() -> None:
    return None  # TODO: do I need to close MongoDB connection ?


@lru_cache(maxsize=1)
def _get_mongodb_database() -> AsyncIOMotorDatabase[Any]:
    client: AsyncIOMotorClient[Any] = AsyncIOMotorClient(MONGODB_URL)
    return client.get_database(MONGODB_DATABASE)


def get_mongodb_collection(collection_name: str) -> AsyncIOMotorCollection[Any]:
    db = _get_mongodb_database()

    return db.get_collection(collection_name)


async def save_feed_position(data: dict[str, str]) -> None:
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            await collection.update_one(
                {"_id": MONGODB_STATE_ID},
                {"$set": data},
                upsert=True,
            )
        except PyMongoError as e:
            logger.warning(
                f"Save feed pos {type(e)}: {e}",
                extra={"MESSAGE_ID": "MONGODB_EXC"},
            )
            await asyncio.sleep(DB_ERROR_INTERVAL)
        else:
            return None


async def get_feed_position() -> Optional[dict[str, str]]:
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            return await collection.find_one({"_id": MONGODB_STATE_ID})
        except PyMongoError as e:
            logger.warning(
                f"Get feed pos {type(e)}: {e}",
                extra={"MESSAGE_ID": "MONGODB_EXC"},
            )
            await asyncio.sleep(DB_ERROR_INTERVAL)


async def drop_feed_position() -> None:
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            await collection.update_one(
                {"_id": MONGODB_STATE_ID},
                {
                    "$unset": {
                        BACKWARD_OFFSET_KEY: "",
                        FORWARD_OFFSET_KEY: "",
                    },
                },
            )
        except PyMongoError as e:
            logger.warning(
                f"Drop feed pos {type(e)}: {e}",
                extra={"MESSAGE_ID": "MONGODB_EXC"},
            )
            await asyncio.sleep(DB_ERROR_INTERVAL)
        else:
            return None
