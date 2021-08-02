from prozorro_crawler.main import (
    main, run_app, init_feed, process_resource,
    get_response_data, crawler, save_crawler_position
)
from unittest.mock import MagicMock, Mock, patch, call
from prozorro_crawler.settings import (
    FEED_STEP_INTERVAL, CONNECTION_ERROR_INTERVAL, TOO_MANY_REQUESTS_INTERVAL,
    NO_ITEMS_INTERVAL, GET_ERROR_RETRIES, BASE_URL, API_RESOURCE, API_OPT_FIELDS,
)
from json.decoder import JSONDecodeError
from .base import AsyncMock
import aiohttp
import pytest


@pytest.mark.asyncio
async def test_main_function():
    data_handler = AsyncMock()
    init_task = AsyncMock()
    headers = {1: 2, 3: "4"}
    loop = MagicMock()
    with patch("prozorro_crawler.main.asyncio.get_event_loop", lambda: loop):
        with patch("prozorro_crawler.main.Lock", MagicMock()) as lock_mock:
            with patch("prozorro_crawler.main.asyncio.sleep", MagicMock()) as sleep_mock:
                main(data_handler, init_task, additional_headers=headers)

    lock_mock.run_locked.assert_called_once()
    assert loop.run_until_complete.mock_calls == [
        call(lock_mock.run_locked.return_value),
        call(sleep_mock(0.250)),
    ]


@pytest.mark.asyncio
async def test_run_app_saved_feed():
    saved_feed_position = {
        "backward_offset": "b",
        "forward_offset": "f",
        "server_id": "007",
    }
    data_handler = AsyncMock()
    prepare_storage = AsyncMock()
    additional_headers = {"User-Agent": "Safari", "Auth": "Token"}
    with patch("prozorro_crawler.main.get_feed_position",
               AsyncMock(side_effect=[saved_feed_position, StopAsyncIteration])):

        session = AsyncMock()
        session.cookie_jar.update_cookies = MagicMock()
        with patch("prozorro_crawler.main.aiohttp.ClientSession",
                   Mock(return_value=session)) as client_mock:
            with patch("prozorro_crawler.main.crawler", AsyncMock()) as crawler_mock:
                try:
                    await run_app(data_handler, prepare_storage, additional_headers)
                except StopAsyncIteration:
                    pass

    prepare_storage.assert_called_once()
    _, client_call_kwargs = client_mock.call_args
    assert client_call_kwargs.get("headers") == additional_headers
    session = client_mock.return_value
    url = f"{BASE_URL}/{API_RESOURCE}"
    opt_fields = API_OPT_FIELDS
    assert crawler_mock.mock_calls == [
        call(session, url, data_handler, offset="f", opt_fields=opt_fields),
        call(session, url, data_handler, offset="b", descending="1", opt_fields=opt_fields),
    ]
    session.cookie_jar.update_cookies.assert_called_once_with({"SERVER_ID": "007"})


@pytest.mark.asyncio
async def test_run_app_init_feed():
    data_handler = AsyncMock()
    with patch("prozorro_crawler.main.get_feed_position",
               AsyncMock(side_effect=[None, StopAsyncIteration])):
        with patch("prozorro_crawler.main.init_feed", AsyncMock(return_value=("b-2", "f1"))) as init_feed_mock:
            with patch("prozorro_crawler.main.aiohttp.ClientSession",
                       MagicMock(return_value=AsyncMock())) as client_mock:
                with patch("prozorro_crawler.main.crawler", AsyncMock()) as crawler_mock:
                    try:
                        await run_app(data_handler)
                    except StopAsyncIteration:
                        pass
    url = f"{BASE_URL}/{API_RESOURCE}"
    opt_fields = API_OPT_FIELDS
    init_feed_mock.assert_called_once_with(client_mock.return_value, url, data_handler, opt_fields=opt_fields)
    assert crawler_mock.mock_calls == [
        call(client_mock.return_value, url, data_handler, offset="f1", opt_fields=opt_fields),
        call(client_mock.return_value, url, data_handler, offset="b-2", descending="1", opt_fields=opt_fields),
    ]


@pytest.mark.asyncio
async def test_init_feed():
    data_handler = AsyncMock()
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

    with patch("prozorro_crawler.main.asyncio.sleep", AsyncMock()) as sleep_mock:
        result = await init_feed(session, "url", data_handler)

    assert result == ("b", "f")
    assert sleep_mock.mock_calls == [call(CONNECTION_ERROR_INTERVAL), call(FEED_STEP_INTERVAL)]
    data_handler.assert_called_once_with(session, ["w", "t", "f"])


@pytest.mark.asyncio
async def test_init_feed_payload_error():
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

    with patch("prozorro_crawler.main.asyncio.sleep", AsyncMock()) as sleep_mock:
        try:
            await init_feed(session, "url", data_handler)
        except StopAsyncIteration:
            pass

    assert sleep_mock.mock_calls == [call(CONNECTION_ERROR_INTERVAL), call(CONNECTION_ERROR_INTERVAL)]
    data_handler.assert_not_called()


@pytest.mark.asyncio
async def test_crawler():
    data_handler = AsyncMock()
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

    with patch("prozorro_crawler.main.API_LIMIT", 3):
        with patch("prozorro_crawler.main.save_crawler_position", AsyncMock()) as save_crawler_position_mock:
            with patch("prozorro_crawler.main.asyncio.sleep", AsyncMock()) as sleep_mock:
                try:
                    await crawler(session, "url", data_handler)
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
    data_handler.assert_called_once_with(session, ["w", "t", "f"])


@pytest.mark.asyncio
async def test_crawler_few_items():
    data_handler = AsyncMock()
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

    with patch("prozorro_crawler.main.save_crawler_position", AsyncMock()) as save_crawler_position_mock:
        with patch("prozorro_crawler.main.asyncio.sleep", AsyncMock()) as sleep_mock:
            await crawler(session, "url", data_handler, descending="1")

    assert sleep_mock.mock_calls == [
        call(NO_ITEMS_INTERVAL),
        call(FEED_STEP_INTERVAL),
    ]
    save_crawler_position_mock.assert_called_once_with(session, data, descending="1")
    data_handler.assert_called_once_with(session, ["w", "t", "f"])


@pytest.mark.asyncio
async def test_crawler_404():
    data_handler = AsyncMock()
    session = MagicMock()
    session.get = AsyncMock(side_effect=[
        MagicMock(status=404, text=AsyncMock(return_value="Not found")),
    ])

    with patch("prozorro_crawler.main.drop_feed_position", AsyncMock()) as drop_feed_position_mock:
        with patch("prozorro_crawler.main.asyncio.sleep", AsyncMock()) as sleep_mock:
            await crawler(session, "url", data_handler)

    assert sleep_mock.mock_calls == []
    drop_feed_position_mock.assert_called_once()
    data_handler.assert_not_called()


@pytest.mark.asyncio
async def test_crawler_payload_error():
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

    with patch("prozorro_crawler.main.asyncio.sleep", AsyncMock()) as sleep_mock:
        try:
            await crawler(session, "url", data_handler)
        except StopAsyncIteration:
            pass

    assert sleep_mock.mock_calls == [
        call(CONNECTION_ERROR_INTERVAL),
        call(CONNECTION_ERROR_INTERVAL),
    ]
    data_handler.assert_not_called()


@pytest.mark.asyncio
async def test_process_resource():
    process_function = AsyncMock()
    session = MagicMock()
    data = {"something": "hello"}
    with patch("prozorro_crawler.main.get_response_data",
               AsyncMock(return_value=data)) as get_response_data_mock:
        await process_resource(session, "url", "abc", process_function)

    get_response_data_mock.assert_called_once_with(session, "url/abc")
    process_function.assert_called_once_with(session, data)


@pytest.mark.asyncio
async def test_get_response_data():
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

    with patch("prozorro_crawler.main.asyncio.sleep", AsyncMock()) as sleep_mock:
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
async def test_get_response_data_error():
    process_function = AsyncMock()
    session = MagicMock()
    session.get = AsyncMock(return_value=MagicMock(status=404, text=AsyncMock(return_value="Not found")))

    with patch("prozorro_crawler.main.asyncio.sleep", AsyncMock()) as sleep_mock:
        await get_response_data(session, "/abc")

    assert session.get.mock_calls == [
        call("/abc")
    ] * GET_ERROR_RETRIES

    process_function.assert_not_called()
    assert sleep_mock.mock_calls == [
        call(CONNECTION_ERROR_INTERVAL),
    ] * (GET_ERROR_RETRIES - 1)


@pytest.mark.asyncio
async def test_get_response_data_payload_error():
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

    with patch("prozorro_crawler.main.asyncio.sleep", AsyncMock()) as sleep_mock:
        try:
            await get_response_data(session, "/abc")
        except StopAsyncIteration:
            pass

    assert sleep_mock.mock_calls == [
        call(CONNECTION_ERROR_INTERVAL),
        call(CONNECTION_ERROR_INTERVAL),
    ]
    process_function.assert_not_called()


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
    with patch("prozorro_crawler.main.save_feed_position", AsyncMock()) as save_feed_position_mock:
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
    with patch("prozorro_crawler.main.save_feed_position", AsyncMock()) as save_feed_position_mock:
        await save_crawler_position(session, response, descending=True)

    save_feed_position_mock.assert_called_once_with(
        {
            "earliest_date_modified": "yesterday",
            "backward_offset": "001",
            "server_id": "jah"
        }
    )
