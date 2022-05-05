from prozorro_crawler.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_STATE_TABLE,
    POSTGRES_STATE_ID,
    DB_ERROR_INTERVAL,
)
from .base import (
    BACKWARD_OFFSET_KEY,
    FORWARD_OFFSET_KEY,
    SERVER_ID_KEY,
)
import asyncpg
import asyncio
import logging

logger = logging.getLogger(__name__)


_connection = None


async def get_connection():
    global _connection
    if _connection is None:
        while True:
            try:
                _connection = await asyncpg.connect(
                    user=POSTGRES_USER,
                    password=POSTGRES_PASSWORD,
                    database=POSTGRES_DB,
                    host=POSTGRES_HOST,
                    port=POSTGRES_PORT,
                )
            except Exception as e:
                logger.error(f"Unable to connect: {e.args}")
                await asyncio.sleep(DB_ERROR_INTERVAL)
            else:
                break

        while True:
            try:
                await _connection.execute(
                    f'''
                        CREATE TABLE IF NOT EXISTS {POSTGRES_STATE_TABLE}(
                            id varchar PRIMARY KEY,
                            {SERVER_ID_KEY} varchar,
                            {FORWARD_OFFSET_KEY} varchar,
                            {BACKWARD_OFFSET_KEY} varchar
                        )
                    '''
                )
            except Exception as e:
                logger.error(f"sql command error: {e.args}")
                await asyncio.sleep(DB_ERROR_INTERVAL)
            else:
                break
    return _connection


async def close_connection():
    conn = await get_connection()
    await conn.close()


async def unlock_feed_position():
    raise NotImplementedError


async def lock_feed_position():
    raise NotImplementedError


async def get_feed_position():
    conn = await get_connection()
    while True:
        try:
            row = await conn.fetchrow(
                f'SELECT * FROM {POSTGRES_STATE_TABLE} WHERE id = $1',
                POSTGRES_STATE_ID,
            )
        except Exception as e:
            logger.warning(f"sql command error: {e.args}")
            await asyncio.sleep(DB_ERROR_INTERVAL)
        else:
            break
    return row


async def save_feed_position(data):
    conn = await get_connection()
    offset_key = FORWARD_OFFSET_KEY if FORWARD_OFFSET_KEY in data else BACKWARD_OFFSET_KEY
    result = await conn.execute(
        f"UPDATE {POSTGRES_STATE_TABLE} "
        f"SET {SERVER_ID_KEY} = $1, {offset_key} = $2"
        "WHERE id = $3",
        data[SERVER_ID_KEY],
        str(data[offset_key]),
        POSTGRES_STATE_ID,
    )
    if result == "UPDATE 0":  # "UPDATE 1" is expected
        result = await conn.execute(
            f"INSERT INTO {POSTGRES_STATE_TABLE} VALUES($1, $2, $3, $4)",
            POSTGRES_STATE_ID,
            data.get(SERVER_ID_KEY),
            str(data.get(FORWARD_OFFSET_KEY, "")),
            str(data.get(BACKWARD_OFFSET_KEY, "")),
        )
        if result != "INSERT 0 1":
            logger.error(f"Unexpected insert result: {result}")


async def drop_feed_position():
    conn = await get_connection()
    while True:
        try:
            await conn.execute(f"DELETE FROM {POSTGRES_STATE_TABLE} WHERE id = $1", POSTGRES_STATE_ID)
        except Exception as e:
            logger.warning(f"sql command error: {e.args}")
            await asyncio.sleep(DB_ERROR_INTERVAL)
        else:
            break
