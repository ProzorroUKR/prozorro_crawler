from prozorro_crawler.resource import process_resource, get_response_data
from unittest.mock import MagicMock, patch, call
from prozorro_crawler.settings import (
    CONNECTION_ERROR_INTERVAL, TOO_MANY_REQUESTS_INTERVAL, GET_ERROR_RETRIES,
)
from json.decoder import JSONDecodeError
from .base import AsyncMock
import aiohttp
import pytest


@pytest.mark.asyncio
@patch("prozorro_crawler.resource.get_response_data")
async def test_process_resource(get_response_data_mock):
    process_function = AsyncMock()
    session = MagicMock()
    data = {"something": "hello"}
    get_response_data_mock.return_value = data

    await process_resource(session, "/abc", "resource", process_function)

    get_response_data_mock.assert_called_once_with(session, "/abc/resource")
    process_function.assert_called_once_with(session, data)


@pytest.mark.asyncio
@patch("prozorro_crawler.main.asyncio.sleep")
async def test_get_response_data(sleep_mock):
    session = MagicMock()
    data = {"data": "hello"}
    response = MagicMock(
        status=200,
        json=AsyncMock(
            return_value=data
        )
    )
    session.get = AsyncMock(side_effect=[
        aiohttp.ClientConnectionError("Sheep happens"),
        MagicMock(status=429, text=AsyncMock(return_value="Too many")),
        response,
    ])

    result = await get_response_data(session, "/abc")

    assert result == "hello"
    assert sleep_mock.mock_calls == [
        call(CONNECTION_ERROR_INTERVAL),
        call(TOO_MANY_REQUESTS_INTERVAL),
    ]
    assert session.get.mock_calls == [
        call("/abc")
    ] * 3


@pytest.mark.asyncio
@patch("prozorro_crawler.main.asyncio.sleep")
async def test_get_response_data_error(sleep_mock):
    process_function = AsyncMock()
    session = MagicMock()
    session.get = AsyncMock(return_value=MagicMock(status=404, text=AsyncMock(return_value="Not found")))

    await get_response_data(session, "/abc")

    assert session.get.mock_calls == [
        call("/abc")
    ] * GET_ERROR_RETRIES

    process_function.assert_not_called()
    assert sleep_mock.mock_calls == [
        call(CONNECTION_ERROR_INTERVAL),
    ] * (GET_ERROR_RETRIES - 1)


@pytest.mark.asyncio
@patch("prozorro_crawler.main.asyncio.sleep")
async def test_get_response_data_payload_error(sleep_mock):
    process_function = AsyncMock()
    session = MagicMock()
    session.get = AsyncMock(side_effect=[
        MagicMock(
            status=200,
            json=AsyncMock(side_effect=aiohttp.ClientPayloadError("Response payload is not completed"))
        ),
        MagicMock(
            status=200,
            json=AsyncMock(side_effect=JSONDecodeError("Failed decode", "", 0))
        ),
        StopAsyncIteration
    ])

    try:
        await get_response_data(session, "/abc")
    except StopAsyncIteration:
        pass

    assert sleep_mock.mock_calls == [
        call(CONNECTION_ERROR_INTERVAL),
        call(CONNECTION_ERROR_INTERVAL),
    ]
    process_function.assert_not_called()
