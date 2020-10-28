from prozorro_crawler.storage import get_mongodb_collection
from prozorro_crawler.settings import (
    LOCK_ENABLED,
    LOCK_COLLECTION_NAME,
    LOCK_EXPIRE_TIME,
    LOCK_UPDATE_TIME,
    LOCK_PROCESS_NAME,
    LOCK_ACQUIRE_INTERVAL,
    MONGODB_ERROR_INTERVAL,
    logger,
)
from pymongo.errors import PyMongoError, DuplicateKeyError
from datetime import datetime, timedelta
from uuid import uuid4
from asyncio import sleep, get_event_loop
import os, signal


def get_lock_collection():
    return get_mongodb_collection(LOCK_COLLECTION_NAME)


async def init_lock_index():
    try:
        await get_lock_collection().create_index("expireAt", expireAfterSeconds=0)
    except PyMongoError as e:
        logger.exception(e,  extra={"MESSAGE_ID": "MONGODB_INDEX_CREATION_ERROR"})


class Lock:

    def __init__(self):
        self.id = uuid4().hex
        logger.info(f"Lock initialized: {self.id}")

    async def acquire(self, should_run):
        """
        Setting lock for LOCK_EXPIRE_TIME seconds
        Use "update" to continue locking for another LOCK_EXPIRE_TIME period.
        IMPORTANT: if lock won't be updated before LOCK_EXPIRE_TIME,
        another process may acquire it and start processing,
        that is why "update" method raises an exception that causes the crawler to stop
        Use "release" to allow another process to acquire the lock.
        :return:
        """
        while should_run():
            try:
                await get_lock_collection().insert_one(
                    {
                        "_id": LOCK_PROCESS_NAME,
                        "lockId": self.id,
                        "expireAt": datetime.utcnow() + timedelta(seconds=LOCK_EXPIRE_TIME),
                    },
                )
            except PyMongoError as e:
                logger.debug(e)
                await sleep(LOCK_ACQUIRE_INTERVAL)
            else:
                logger.info(f"Lock {LOCK_PROCESS_NAME} #{self.id} acquired")
                return True
        return False

    async def update(self, should_run):
        await sleep(LOCK_UPDATE_TIME)

        while should_run():
            try:
                result = await get_lock_collection().update_one(
                    {
                        "_id": LOCK_PROCESS_NAME,  # inserting may fail because of _id DuplicateError
                        "lockId": self.id,
                    },
                    {"$set": {"expireAt": datetime.utcnow() + timedelta(seconds=LOCK_EXPIRE_TIME)}},
                    upsert=True
                )
            except PyMongoError as e:
                if isinstance(e, DuplicateKeyError):
                    logger.critical(
                        "Another process acquired the lock, "
                        "the time between 'update' maybe takes more than LOCK_EXPIRE_TIME"
                    )
                    return os.kill(os.getpid(), signal.SIGTERM)
                logger.warning(e)
                await sleep(MONGODB_ERROR_INTERVAL)
            else:
                logger.info(f"Updated lock {LOCK_PROCESS_NAME} #{self.id}: "
                            f"{result.acknowledged} {result.modified_count} {result.upserted_id}")
                await sleep(LOCK_UPDATE_TIME)

    async def release(self):
        try:
            result = await get_lock_collection().delete_one(
                {
                    "_id": LOCK_PROCESS_NAME,
                    "lockId": self.id,
                },
            )
        except PyMongoError as e:
            logger.exception(e)
        else:
            logger.info(f"Deleted lock {LOCK_PROCESS_NAME} #{self.id}: {result.deleted_count}")

    # task wrapper
    @classmethod
    async def run_locked(cls, get_app, should_run):
        if LOCK_ENABLED:
            await init_lock_index()
            loop = get_event_loop()

            lock = cls()
            if await lock.acquire(should_run):
                # creating background update task
                loop.create_task(lock.update(should_run))
                # waiting for app to finish
                try:
                    await get_app()
                finally:
                    await lock.release()
        else:
            await get_app()
