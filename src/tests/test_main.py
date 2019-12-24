from main import main, init_feed, process_tender, crawler, save_crawler_position
from unittest.mock import MagicMock, patch, call
from settings import (
    FEED_STEP_INTERVAL, CONNECTION_ERROR_INTERVAL, TOO_MANY_REQUESTS_INTERVAL,
    NO_ITEMS_INTERVAL, BASE_URL
)
from .base import AsyncMock
import aiohttp
import pytest


@pytest.mark.asyncio
async def test_main_saved_feed():
    saved_feed_position = {
        "backward_offset": "b",
        "forward_offset": "f",
        "server_id": "007",
    }
    with patch("main.get_feed_position", AsyncMock(side_effect=[saved_feed_position, StopAsyncIteration])):
        with patch("main.prepare_storage", AsyncMock()) as prepare_storage_mock:
            session = AsyncMock()
            session.cookie_jar.update_cookies = MagicMock()
            with patch("main.aiohttp.ClientSession", MagicMock(return_value=session)) as client_mock:
                with patch("main.crawler", AsyncMock()) as crawler_mock:
                    try:
                        await main()
                    except StopAsyncIteration:
                        pass

    prepare_storage_mock.assert_called_once()
    session = client_mock.return_value
    assert crawler_mock.mock_calls == [
        call(session, offset="f"),
        call(session, offset="b", descending="1"),
    ]
    session.cookie_jar.update_cookies.assert_called_once_with({"SERVER_ID": "007"})


@pytest.mark.asyncio
async def test_main_init_feed():
    with patch("main.get_feed_position", AsyncMock(side_effect=[None, StopAsyncIteration])):
        with patch("main.prepare_storage", AsyncMock()) as prepare_storage_mock:
            with patch("main.init_feed", AsyncMock(return_value=("b-2", "f1"))):
                with patch("main.aiohttp.ClientSession", MagicMock(return_value=AsyncMock())) as client_mock:
                    with patch("main.crawler", AsyncMock()) as crawler_mock:
                        try:
                            await main()
                        except StopAsyncIteration:
                            pass

    prepare_storage_mock.assert_called_once()
    prepare_storage_mock.assert_called_once()
    assert crawler_mock.mock_calls == [
        call(client_mock.return_value, offset="f1"),
        call(client_mock.return_value, offset="b-2", descending="1"),
    ]


@pytest.mark.asyncio
async def test_init_feed():
    session = MagicMock()
    response = MagicMock(
        status=200,
        json=AsyncMock(
            return_value={
                "next_page": {"offset": "b"},
                "prev_page": {"offset": "f"},
                "data": ["w", "t", "f"],
            }
        )
    )
    session.get = AsyncMock(side_effect=[
        aiohttp.ClientConnectionError("Sheep happens"),
        MagicMock(status=429, text=AsyncMock(return_value="Too many")),
        response,
    ])

    with patch("main.asyncio.sleep", AsyncMock()) as sleep_mock:
        with patch("main.asyncio.gather", AsyncMock()) as gather_mock:
            with patch("main.process_tender", lambda *args: args):  # process_tender just returns its args
                result = await init_feed(session)

    assert result == ("b", "f")
    assert sleep_mock.mock_calls == [call(CONNECTION_ERROR_INTERVAL), call(FEED_STEP_INTERVAL)]
    gather_mock.assert_called_once_with(
        (session, "w"),
        (session, "t"),
        (session, "f"),
    )


@pytest.mark.asyncio
async def test_crawler():
    session = MagicMock()
    data = {
        "next_page": {"offset": "b"},
        "prev_page": {"offset": "f"},
        "data": ["w", "t", "f"],
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

    with patch("main.API_LIMIT", 3):
        with patch("main.save_crawler_position", AsyncMock()) as save_crawler_position_mock:
            with patch("main.asyncio.sleep", AsyncMock()) as sleep_mock:
                with patch("main.asyncio.gather", AsyncMock()) as gather_mock:
                    with patch("main.process_tender", lambda *args: args):  # process_tender just returns its args
                        try:
                            await crawler(session)
                        except StopAsyncIteration:
                            pass

    assert sleep_mock.mock_calls == [
        call(CONNECTION_ERROR_INTERVAL),
        call(TOO_MANY_REQUESTS_INTERVAL), call(FEED_STEP_INTERVAL),
        call(FEED_STEP_INTERVAL),
        call(FEED_STEP_INTERVAL),
        call(FEED_STEP_INTERVAL),
    ]
    save_crawler_position_mock.assert_called_once_with(session, data, descending="")
    gather_mock.assert_called_once_with(
        (session, "w"),
        (session, "t"),
        (session, "f"),
    )


@pytest.mark.asyncio
async def test_crawler_few_items():
    session = MagicMock()
    data = {
        "next_page": {"offset": "b"},
        "prev_page": {"offset": "f"},
        "data": ["w", "t", "f"],
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
                "next_page": {"offset": "b"},
                "prev_page": {"offset": "f"},
                "data": [],
            }
        )
    )
    session.get = AsyncMock(side_effect=[
        response,
        empty_response,
    ])

    with patch("main.save_crawler_position", AsyncMock()) as save_crawler_position_mock:
        with patch("main.asyncio.sleep", AsyncMock()) as sleep_mock:
            with patch("main.asyncio.gather", AsyncMock()) as gather_mock:
                with patch("main.process_tender", lambda *args: args):  # process_tender just returns its args
                    await crawler(session, descending="1")

    assert sleep_mock.mock_calls == [
        call(NO_ITEMS_INTERVAL),
        call(FEED_STEP_INTERVAL),
    ]
    save_crawler_position_mock.assert_called_once_with(session, data, descending="1")
    gather_mock.assert_called_once_with(
        (session, "w"),
        (session, "t"),
        (session, "f"),
    )


@pytest.mark.asyncio
async def test_crawler_404():
    session = MagicMock()
    session.get = AsyncMock(side_effect=[
        MagicMock(status=404, text=AsyncMock(return_value="Not found")),
    ])

    with patch("main.drop_feed_position", AsyncMock()) as drop_feed_position_mock:
        with patch("main.asyncio.sleep", AsyncMock()) as sleep_mock:
            await crawler(session)

    assert sleep_mock.mock_calls == []
    drop_feed_position_mock.assert_called_once()


@pytest.mark.asyncio
async def test_process_tender():
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
        # MagicMock(status=404, text=AsyncMock(return_value="Not found")),
        response,
    ])

    complaints = ["ab", "bc", "ac"]
    with patch("main.get_complaint_data", MagicMock(return_value=complaints)) as get_complaint_data_mock:
        with patch("main.asyncio.gather", AsyncMock()) as gather_mock:
            with patch("main.save_complaint_data", list):
                with patch("main.asyncio.sleep", AsyncMock()) as sleep_mock:
                    await process_tender(session, {"id": "abc"})

    assert sleep_mock.mock_calls == [
        call(CONNECTION_ERROR_INTERVAL),
        call(TOO_MANY_REQUESTS_INTERVAL),
    ]
    assert session.get.mock_calls == [
        call(BASE_URL + "/abc")
    ] * 3
    assert get_complaint_data_mock.mock_calls == [
        call("hello")
    ]
    gather_mock.assert_called_once_with(["a", "b"], ["b", "c"], ["a", "c"])


@pytest.mark.asyncio
async def test_process_tender_error():
    session = MagicMock()
    session.get = AsyncMock(side_effect=[
        MagicMock(status=404, text=AsyncMock(return_value="Not found")),
    ])
    with patch("main.get_complaint_data") as get_complaint_data_mock:
        await process_tender(session, {"id": "abc"})
    session.get.assert_called_once_with(BASE_URL + "/abc")
    get_complaint_data_mock.assert_not_called()


@pytest.mark.asyncio
async def test_save_crawler_position():
    session = MagicMock()
    session.cookie_jar.filter_cookies.return_value = {"SERVER_ID": MagicMock(value="jah")}
    response = {
        "data": [
            {
                "dateModified": "yesterday"
            }
        ],
        "next_page": {
            "offset": "001"
        }
    }
    with patch("main.save_feed_position", AsyncMock()) as save_feed_position_mock:
        await save_crawler_position(session, response)

    save_feed_position_mock.assert_called_once_with(
        {
            "latest_date_modified": "yesterday",
            "forward_offset": "001",
            "server_id": "jah"
        }
    )


@pytest.mark.asyncio
async def test_save_backward_crawler_position():
    session = MagicMock()
    session.cookie_jar.filter_cookies.return_value = {"SERVER_ID": MagicMock(value="jah")}
    response = {
        "data": [
            {
                "dateModified": "yesterday"
            }
        ],
        "next_page": {
            "offset": "001"
        }
    }
    with patch("main.save_feed_position", AsyncMock()) as save_feed_position_mock:
        await save_crawler_position(session, response, descending=True)

    save_feed_position_mock.assert_called_once_with(
        {
            "earliest_date_modified": "yesterday",
            "backward_offset": "001",
            "server_id": "jah"
        }
    )
