from pymongo.errors import PyMongoError
from motor.motor_asyncio import AsyncIOMotorClient
from prozorro_crawler.settings import (
    logger,
    MONGODB_URL,
    MONGODB_DATABASE,
    MONGODB_STATE_COLLECTION,
    MONGODB_STATE_ID,
    MONGODB_ERROR_INTERVAL,
)
import asyncio


BACKWARD_OFFSET_KEY = "backward_offset"
FORWARD_OFFSET_KEY = "forward_offset"
SERVER_ID_KEY = "server_id"
EARLIEST_DATE_MODIFIED_KEY = "earliest_date_modified"
LATEST_DATE_MODIFIED_KEY = "latest_date_modified"
LOCK_DATE_MODIFIED_KEY = "lock_date_modified"


def get_mongodb_collection(collection_name):
    client = AsyncIOMotorClient(MONGODB_URL)
    db = getattr(client, MONGODB_DATABASE)
    collection = getattr(db, collection_name)
    return collection


async def save_feed_position(data):
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            return await collection.update_one(
                {"_id": MONGODB_STATE_ID},
                {"$set": data},
                upsert=True
            )
        except PyMongoError as e:
            logger.warning(f"Save feed pos {type(e)}: {e}", extra={"MESSAGE_ID": "MONGODB_EXC"})
            await asyncio.sleep(MONGODB_ERROR_INTERVAL)


async def get_feed_position():
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            return await collection.find_one({"_id": MONGODB_STATE_ID})
        except PyMongoError as e:
            logger.warning(f"Get feed pos {type(e)}: {e}", extra={"MESSAGE_ID": "MONGODB_EXC"})
            await asyncio.sleep(MONGODB_ERROR_INTERVAL)


async def drop_feed_position():
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            return await collection.update_one(
                {"_id": MONGODB_STATE_ID},
                {
                    "$unset": {
                        BACKWARD_OFFSET_KEY: "",
                        FORWARD_OFFSET_KEY: "",
                        SERVER_ID_KEY: "",
                    }
                }
            )
        except PyMongoError as e:
            logger.warning(f"Drop feed pos {type(e)}: {e}", extra={"MESSAGE_ID": "MONGODB_EXC"})
            await asyncio.sleep(MONGODB_ERROR_INTERVAL)


async def lock_feed_position():
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            return await collection.update_one(
                {"_id": MONGODB_STATE_ID},
                {
                    "$set": {
                        LOCK_DATE_MODIFIED_KEY: True
                    }
                }
            )
        except PyMongoError as e:
            logger.warning(f"Lock feed pos {type(e)}: {e}", extra={"MESSAGE_ID": "MONGODB_EXC"})
            await asyncio.sleep(MONGODB_ERROR_INTERVAL)


async def unlock_feed_position():
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            return await collection.update_one(
                {"_id": MONGODB_STATE_ID},
                {
                    "$set": {
                        LOCK_DATE_MODIFIED_KEY: False
                    },
                }
            )
        except PyMongoError as e:
            logger.warning(f"Unlock feed pos {type(e)}: {e}", extra={"MESSAGE_ID": "MONGODB_EXC"})
            await asyncio.sleep(MONGODB_ERROR_INTERVAL)
