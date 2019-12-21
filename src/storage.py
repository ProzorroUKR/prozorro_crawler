from pymongo.errors import PyMongoError
from motor.motor_asyncio import AsyncIOMotorClient
from settings import (
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


async def prepare_storage():
    collection = get_mongodb_collection()
    for field in ("tender.id", "complaint.id"):
        while True:
            try:
                r = await collection.create_index([(field, 1)])
            except PyMongoError as e:
                logger.exception(e, extra={"MESSAGE_ID": "MONGODB_EXC"})
            else:
                logger.info(f"Created index: {r}", extra={"MESSAGE_ID": "MONGODB_INDEX_SUCCESS"})
                break


async def save_complaint_data(data):
    collection = get_mongodb_collection()

    while True:
        try:
            return await collection.update_one(
                {
                    "tender.id": data["tender"]["id"],
                    "complaint.id": data["complaint"]["id"],
                },
                {"$set": data},
                upsert=True
            )
        except PyMongoError as exc:
            logger.exception(exc, extra={"MESSAGE_ID": "MONGODB_EXC"})
            await asyncio.sleep(MONGODB_ERROR_INTERVAL)


async def save_feed_position(data):
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            return await collection.update_one(
                {"_id": MONGODB_STATE_ID},
                {"$set": data},
                upsert=True
            )
        except PyMongoError as exc:
            logger.exception(exc, extra={"MESSAGE_ID": "MONGODB_EXC"})
            await asyncio.sleep(MONGODB_ERROR_INTERVAL)


async def get_feed_position():
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            return await collection.find_one({"_id": MONGODB_STATE_ID})
        except PyMongoError as exc:
            logger.exception(exc, extra={"MESSAGE_ID": "MONGODB_EXC"})
            await asyncio.sleep(MONGODB_ERROR_INTERVAL)


async def drop_feed_position():
    collection = get_mongodb_collection(MONGODB_STATE_COLLECTION)
    while True:
        try:
            return await collection.delete_one({"_id": MONGODB_STATE_ID})
        except PyMongoError as exc:
            logger.exception(exc, extra={"MESSAGE_ID": "MONGODB_EXC"})
            await asyncio.sleep(MONGODB_ERROR_INTERVAL)
