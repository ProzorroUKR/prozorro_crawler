from prozorro_crawler.storage import (
    save_feed_position,
    get_feed_position,
    drop_feed_position,
)
from pymongo.errors import ServerSelectionTimeoutError
from unittest.mock import MagicMock, patch, call
from prozorro_crawler.settings import (
    DB_ERROR_INTERVAL,
    MONGODB_STATE_ID,
)
from .base import AsyncMock


@patch("prozorro_crawler.storage.mongodb.get_mongodb_collection")
@patch("prozorro_crawler.storage.mongodb.asyncio.sleep")
async def test_save_feed_position(
    sleep_mock: AsyncMock,
    collection_mock: MagicMock,
) -> None:
    collection_mock.return_value.update_one = AsyncMock(
        side_effect=[
            ServerSelectionTimeoutError("Oops"),
            ServerSelectionTimeoutError("Oops"),
            "",
        ],
    )
    test_data = {"test": "hi"}

    await save_feed_position(test_data)

    assert (
        sleep_mock.mock_calls
        == [
            call(DB_ERROR_INTERVAL),
        ]
        * 2
    )

    assert (
        collection_mock.return_value.update_one.mock_calls
        == [call({"_id": MONGODB_STATE_ID}, {"$set": test_data}, upsert=True)] * 3
    )


@patch("prozorro_crawler.storage.mongodb.get_mongodb_collection")
@patch("prozorro_crawler.storage.mongodb.asyncio.sleep")
async def test_get_feed_position(
    sleep_mock: MagicMock,
    collection_mock: MagicMock,
) -> None:
    test_data = {"test": "hi"}
    collection_mock.return_value.find_one = AsyncMock(
        side_effect=[
            ServerSelectionTimeoutError("Oops"),
            ServerSelectionTimeoutError("Oops"),
            ServerSelectionTimeoutError("Oops"),
            test_data,
        ],
    )

    result = await get_feed_position()

    assert (
        sleep_mock.mock_calls
        == [
            call(DB_ERROR_INTERVAL),
        ]
        * 3
    )

    assert (
        collection_mock.return_value.find_one.mock_calls
        == [call({"_id": MONGODB_STATE_ID})] * 4
    )
    assert result is test_data


@patch("prozorro_crawler.storage.mongodb.get_mongodb_collection")
@patch("prozorro_crawler.storage.mongodb.asyncio.sleep")
async def test_drop_feed_position(
    sleep_mock: MagicMock,
    collection_mock: MagicMock,
) -> None:
    collection_mock.return_value.update_one = AsyncMock(
        side_effect=[
            ServerSelectionTimeoutError("Oops"),
            {},
        ],
    )

    await drop_feed_position()

    assert sleep_mock.mock_calls == [
        call(DB_ERROR_INTERVAL),
    ]

    assert (
        collection_mock.return_value.update_one.mock_calls
        == [
            call(
                {"_id": MONGODB_STATE_ID},
                {
                    "$unset": {
                        "backward_offset": "",
                        "forward_offset": "",
                    },
                },
            ),
        ]
        * 2
    )
