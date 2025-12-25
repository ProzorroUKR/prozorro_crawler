from typing import Optional, Any

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
)
import asyncpg
import asyncio
import logging

logger = logging.getLogger(__name__)


_connection = None


async def reconnect() -> asyncpg.Connection:
    global _connection
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
            return _connection


async def get_connection() -> asyncpg.Connection:
    connection = await reconnect()
    while True:
        try:
            await connection.execute(
                f"""
                    CREATE TABLE IF NOT EXISTS {POSTGRES_STATE_TABLE}(
                        id varchar PRIMARY KEY,
                        server_id varchar,
                        {FORWARD_OFFSET_KEY} varchar,
                        {BACKWARD_OFFSET_KEY} varchar
                    )
                """,
            )
        except Exception as e:
            logger.error(f"sql command error: {e.args}")
            await asyncio.sleep(DB_ERROR_INTERVAL)
        else:
            break
    return connection


async def close_connection() -> None:
    conn = await get_connection()
    await conn.close()


async def handle_exception(e: BaseException) -> None:
    logger.warning(f"sql command error: {e.args}")
    if e.args and "connection is closed" in e.args[0]:
        await reconnect()
    await asyncio.sleep(DB_ERROR_INTERVAL)


async def get_feed_position() -> Optional[dict[str, str]]:
    while True:
        conn = await get_connection()
        try:
            row = await conn.fetchrow(
                f"SELECT * FROM {POSTGRES_STATE_TABLE} WHERE id = $1",
                POSTGRES_STATE_ID,
            )
        except Exception as e:
            await handle_exception(e)
            return None
        else:
            if row is None:
                return None
            return dict(row.items())


async def save_feed_position(data: dict[str, str]) -> None:
    offset_key = (
        FORWARD_OFFSET_KEY if FORWARD_OFFSET_KEY in data else BACKWARD_OFFSET_KEY
    )
    result = await execute_command(
        f"UPDATE {POSTGRES_STATE_TABLE} "
        f"SET server_id = $1, {offset_key} = $2 WHERE id = $3",
        "",
        str(data[offset_key]),
        POSTGRES_STATE_ID,
    )
    if result == "UPDATE 0":  # "UPDATE 1" is expected
        result = await execute_command(
            f"INSERT INTO {POSTGRES_STATE_TABLE} VALUES($1, $2, $3, $4)",
            POSTGRES_STATE_ID,
            "",
            str(data.get(FORWARD_OFFSET_KEY, "")),
            str(data.get(BACKWARD_OFFSET_KEY, "")),
        )
        if result != "INSERT 0 1":
            logger.error(f"Unexpected insert result: {result}")


async def drop_feed_position() -> None:
    await execute_command(
        f"DELETE FROM {POSTGRES_STATE_TABLE} WHERE id = $1",
        POSTGRES_STATE_ID,
    )


async def execute_command(comm: str, *args: Any) -> str:
    while True:
        conn = await get_connection()
        try:
            result = await conn.execute(comm, *args)
        except Exception as e:
            await handle_exception(e)
        else:
            return str(result)
