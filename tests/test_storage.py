from prozorro_crawler.storage import (
    get_mongodb_collection,
    save_feed_position,
    get_feed_position,
    drop_feed_position,
)
from pymongo.errors import ServerSelectionTimeoutError
from unittest.mock import MagicMock, patch, call
from prozorro_crawler.settings import (
    MONGODB_DATABASE,
    DB_ERROR_INTERVAL,
    MONGODB_STATE_ID,
)
from .base import AsyncMock
import unittest
import pytest


class GetCollectionTestCase(unittest.TestCase):

    @patch("prozorro_crawler.storage.mongodb.AsyncIOMotorClient")
    def test_get_mongodb_custom_collection(self, motor_client):
        collection = "my_collection"

        database = MagicMock()
        setattr(database, collection, "hi, r2")
        client = MagicMock()
        setattr(client, MONGODB_DATABASE, database)
        motor_client.return_value = client

        collection = get_mongodb_collection(collection)
        self.assertEqual(collection, "hi, r2")


@pytest.mark.asyncio
@patch("prozorro_crawler.storage.mongodb.get_mongodb_collection")
@patch("prozorro_crawler.storage.asyncio.sleep")
async def test_save_feed_position(sleep_mock, collection_mock):
    collection_mock.return_value.update_one = AsyncMock(
        side_effect=[
            ServerSelectionTimeoutError("Oops"),
            ServerSelectionTimeoutError("Oops"),
            "",
        ]
    )
    test_data = {"test": "hi"}

    await save_feed_position(test_data)

    assert sleep_mock.mock_calls == [
        call(DB_ERROR_INTERVAL),
    ] * 2

    assert collection_mock.return_value.update_one.mock_calls == [
        call(
            {"_id": MONGODB_STATE_ID},
            {"$set": test_data},
            upsert=True
        )
    ] * 3


@pytest.mark.asyncio
@patch("prozorro_crawler.storage.mongodb.get_mongodb_collection")
@patch("prozorro_crawler.storage.asyncio.sleep")
async def test_get_feed_position(sleep_mock, collection_mock):
    test_data = {"test": "hi"}
    collection_mock.return_value.find_one = AsyncMock(
        side_effect=[
            ServerSelectionTimeoutError("Oops"),
            ServerSelectionTimeoutError("Oops"),
            ServerSelectionTimeoutError("Oops"),
            test_data,
        ]
    )

    result = await get_feed_position()

    assert sleep_mock.mock_calls == [
        call(DB_ERROR_INTERVAL),
    ] * 3

    assert collection_mock.return_value.find_one.mock_calls == [
        call({"_id": MONGODB_STATE_ID})
    ] * 4
    assert result is test_data


@pytest.mark.asyncio
@patch("prozorro_crawler.storage.mongodb.get_mongodb_collection")
@patch("prozorro_crawler.storage.asyncio.sleep")
async def test_drop_feed_position(sleep_mock, collection_mock):
    test_data = {"test": "hi"}
    collection_mock.return_value.update_one = AsyncMock(
        side_effect=[
            ServerSelectionTimeoutError("Oops"),
            test_data,
        ]
    )

    result = await drop_feed_position()

    assert sleep_mock.mock_calls == [
        call(DB_ERROR_INTERVAL),
    ]

    assert collection_mock.return_value.update_one.mock_calls == [
        call(
            {"_id": MONGODB_STATE_ID},
            {
                "$unset": {
                    "backward_offset": "",
                    "forward_offset": "",
                    "server_id": "",
                }
            }
        )
    ] * 2
    assert result is test_data
