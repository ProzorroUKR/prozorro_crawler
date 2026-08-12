from typing import Any, Optional

from pymongo.errors import PyMongoError
from pymongo.asynchronous.mongo_client import AsyncMongoClient
from pymongo.asynchronous.collection import AsyncCollection
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

client: Optional[AsyncMongoClient[Any]] = None


def get_client() -> AsyncMongoClient[Any]:
    global client
    if client is None:
        client = AsyncMongoClient(MONGODB_URL)
    return client


async def close_client() -> None:
    global client
    if client is not None:
        await client.close()
        client = None


async def close_connection() -> None:
    await close_client()


def get_mongodb_collection(collection_name: str) -> AsyncCollection[Any]:
    db = get_client().get_database(MONGODB_DATABASE)
    return db.get_collection(collection_name)


async def handle_db_exception(e: PyMongoError, message: str) -> None:
    logger.warning(
        f"{message} {type(e)}: {e}",
        extra={"MESSAGE_ID": "MONGODB_EXC"},
    )
    await asyncio.sleep(DB_ERROR_INTERVAL)


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
            await handle_db_exception(e, "Save feed pos")
        else:
            return None


async def get_feed_position() -> Optional[dict[str, str]]:
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            return await collection.find_one({"_id": MONGODB_STATE_ID})
        except PyMongoError as e:
            await handle_db_exception(e, "Get feed pos")


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
            await handle_db_exception(e, "Drop feed pos")
        else:
            return None
