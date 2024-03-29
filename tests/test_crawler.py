from prozorro_crawler.main import (
    should_run,
)
from prozorro_crawler.crawler import (
    init_feed,
    crawler,
    init_crawler,
)
from unittest.mock import MagicMock, patch, call
from prozorro_crawler.settings import (
    FEED_STEP_INTERVAL,
    CONNECTION_ERROR_INTERVAL,
    TOO_MANY_REQUESTS_INTERVAL,
    NO_ITEMS_INTERVAL,
)
from json.decoder import JSONDecodeError
from .base import AsyncMock
import aiohttp
import pytest
import json


@pytest.mark.asyncio
@patch("prozorro_crawler.crawler.get_feed_position")
@patch("prozorro_crawler.crawler.crawler")
async def test_init_crawler_saved_feed(crawler_mock, get_feed_position_mock):
    session = MagicMock()
    saved_feed_position = {
        "backward_offset": "b",
        "forward_offset": "f",
        "server_id": "007",
    }
    get_feed_position_mock.side_effect = [
        saved_feed_position,
        StopAsyncIteration,
    ]
    data_handler = AsyncMock()
    opt_fields = "test1,test2"

    try:
        await init_crawler(should_run, session, "/abc", data_handler, opt_fields=opt_fields)
    except StopAsyncIteration:
        pass

    assert crawler_mock.mock_calls == [
        call(
            should_run,
            session,
            "/abc",
            data_handler,
            offset="f",
            opt_fields=opt_fields,
            json_loads=json.loads,
        ),
        call(
            should_run,
            session,
            "/abc",
            data_handler,
            offset="b",
            descending="1",
            opt_fields=opt_fields,
            json_loads=json.loads,
        ),
    ]
    session.cookie_jar.update_cookies.assert_called_once_with({"SERVER_ID": "007"})


@pytest.mark.asyncio
@patch("prozorro_crawler.crawler.get_feed_position", AsyncMock(side_effect=[None, StopAsyncIteration]))
@patch("prozorro_crawler.crawler.init_feed")
@patch("prozorro_crawler.crawler.crawler")
async def test_init_crawler_init_feed(crawler_mock, init_feed_mock):
    session = MagicMock()
    init_feed_mock.return_value = ("b-2", "f1")
    data_handler = AsyncMock()
    opt_fields = "test1,test2"

    try:
        await init_crawler(should_run, session, "/abc", data_handler, opt_fields=opt_fields)
    except StopAsyncIteration:
        pass

    init_feed_mock.assert_called_once_with(
        should_run,
        session,
        "/abc",
        data_handler,
        opt_fields=opt_fields,
        json_loads=json.loads,
    )
    assert crawler_mock.mock_calls == [
        call(
            should_run,
            session,
            "/abc",
            data_handler,
            offset="f1",
            opt_fields=opt_fields,
            json_loads=json.loads,
        ),
        call(
            should_run,
            session,
            "/abc",
            data_handler,
            offset="b-2",
            descending="1",
            opt_fields=opt_fields,
            json_loads=json.loads,
        ),
    ]


@pytest.mark.asyncio
@patch("prozorro_crawler.main.asyncio.sleep")
async def test_init_feed(sleep_mock):
    data_handler = AsyncMock()
    session = MagicMock()
    response = MagicMock(
        status=200,
        json=AsyncMock(
            return_value={
                "next_page": {"offset": 12},
                "prev_page": {"offset": 999},
                "data": ["w", "t", "f"],
            }
        )
    )
    session.get = AsyncMock(side_effect=[
        aiohttp.ClientConnectionError("Sheep happens"),
        MagicMock(status=429, text=AsyncMock(return_value="Too many")),
        response,
    ])

    result = await init_feed(should_run, session, "/abc", data_handler, json_loads=json.loads)

    assert result == (12, 999)
    assert sleep_mock.mock_calls == [call(CONNECTION_ERROR_INTERVAL), call(FEED_STEP_INTERVAL)]
    data_handler.assert_called_once_with(session, ["w", "t", "f"])


@pytest.mark.asyncio
@patch("prozorro_crawler.main.asyncio.sleep")
async def test_init_feed_payload_error(sleep_mock):
    data_handler = AsyncMock()
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
        await init_feed(should_run, session, "/abc", data_handler, json_loads=json.loads)
    except StopAsyncIteration:
        pass

    assert sleep_mock.mock_calls == [call(CONNECTION_ERROR_INTERVAL), call(CONNECTION_ERROR_INTERVAL)]
    data_handler.assert_not_called()


@pytest.mark.asyncio
@patch("prozorro_crawler.crawler.API_LIMIT", 3)
@patch("prozorro_crawler.crawler.save_feed_position")
@patch("prozorro_crawler.main.asyncio.sleep")
async def test_crawler(sleep_mock, save_feed_position_mock):
    data_handler = AsyncMock()
    session = MagicMock()
    session.cookie_jar.filter_cookies.return_value = {"SERVER_ID": MagicMock(value="jah")}
    items = [
        {
            "dateModified": "w"
        },
        {
            "dateModified": "t"
        },
        {
            "dateModified": "f"
        }
    ]
    data = {
        "next_page": {"offset": 2},
        "prev_page": {"offset": 99},
        "data": items,
    }
    response = MagicMock(
        status=200,
        json=AsyncMock(
            return_value=data
        )
    )
    session.get = AsyncMock(side_effect=[
        aiohttp.ClientConnectionError("Sheep happens"),
        MagicMock(status=429, text=AsyncMock(return_value="Too many")),
        MagicMock(status=412, text=AsyncMock(return_value="Precondition failed")),
        MagicMock(status=500, text=AsyncMock(return_value="Server fail")),
        response,
        StopAsyncIteration("Exit loop")
    ])

    try:
        await crawler(should_run, session, "/abc", data_handler, json_loads=json.loads)
    except StopAsyncIteration:
        pass

    assert sleep_mock.mock_calls == [
        call(CONNECTION_ERROR_INTERVAL),
        call(TOO_MANY_REQUESTS_INTERVAL), call(FEED_STEP_INTERVAL),
        call(FEED_STEP_INTERVAL),
        call(FEED_STEP_INTERVAL),
        call(FEED_STEP_INTERVAL),
    ]
    save_feed_position_mock.assert_called_once_with({
        "latest_date_modified": "f",
        "forward_offset": 2,
        "server_id": "jah"
    })
    data_handler.assert_called_once_with(session, items)


@pytest.mark.asyncio
@patch("prozorro_crawler.crawler.save_feed_position")
@patch("prozorro_crawler.main.asyncio.sleep")
async def test_crawler_few_items(sleep_mock, save_feed_position_mock):
    data_handler = AsyncMock()
    session = MagicMock()
    session.cookie_jar.filter_cookies.return_value = {"SERVER_ID": MagicMock(value="jah")}
    items = [
        {
            "dateModified": "w"
        },
        {
            "dateModified": "t"
        },
        {
            "dateModified": "f"
        }
    ]
    data = {
        "next_page": {"offset": 1},
        "prev_page": {"offset": 9},
        "data": items,
    }
    response = MagicMock(
        status=200,
        json=AsyncMock(
            return_value=data
        )
    )
    empty_response = MagicMock(
        status=200,
        json=AsyncMock(
            return_value={
                "next_page": {"offset": 1},
                "prev_page": {"offset": 9},
                "data": [],
            }
        )
    )
    session.get = AsyncMock(side_effect=[
        response,
        empty_response,
    ])

    await crawler(should_run, session, "/abc", data_handler, descending="1")

    assert sleep_mock.mock_calls == [
        call(NO_ITEMS_INTERVAL),
        call(FEED_STEP_INTERVAL),
    ]
    save_feed_position_mock.assert_called_once_with({
        "earliest_date_modified": "f",
        "backward_offset": 1,
        "server_id": "jah"
    })
    data_handler.assert_called_once_with(session, items)


@pytest.mark.asyncio
@patch("prozorro_crawler.crawler.drop_feed_position")
@patch("prozorro_crawler.main.asyncio.sleep")
async def test_crawler_404(sleep_mock, drop_feed_position_mock):
    data_handler = AsyncMock()
    session = MagicMock()
    session.get = AsyncMock(side_effect=[
        MagicMock(status=404, text=AsyncMock(return_value="Not found")),
    ])

    await crawler(should_run, session, "/abc", data_handler)

    assert sleep_mock.mock_calls == []
    drop_feed_position_mock.assert_called_once()
    data_handler.assert_not_called()


@pytest.mark.asyncio
@patch("prozorro_crawler.main.asyncio.sleep")
async def test_crawler_payload_error(sleep_mock):
    data_handler = AsyncMock()
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
        await crawler(should_run, session, "/abc", data_handler)
    except StopAsyncIteration:
        pass

    assert sleep_mock.mock_calls == [
        call(CONNECTION_ERROR_INTERVAL),
        call(CONNECTION_ERROR_INTERVAL),
    ]
    data_handler.assert_not_called()
