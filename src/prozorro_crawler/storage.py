from pymongo.errors import PyMongoError
from motor.motor_asyncio import AsyncIOMotorClient
from prozorro_crawler.settings import (
    MONGODB_COLLECTION, MONGODB_URL, MONGODB_DATABASE,
    MONGODB_STATE_COLLECTION, MONGODB_STATE_ID, MONGODB_ERROR_INTERVAL,
    logger
)
import asyncio


def get_mongodb_collection(collection_name=MONGODB_COLLECTION):
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
            return await collection.delete_one({"_id": MONGODB_STATE_ID})
        except PyMongoError as e:
            logger.warning(f"Drop feed pos {type(e)}: {e}", extra={"MESSAGE_ID": "MONGODB_EXC"})
            await asyncio.sleep(MONGODB_ERROR_INTERVAL)
