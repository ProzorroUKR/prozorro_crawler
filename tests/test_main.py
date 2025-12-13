from prozorro_crawler.main import (
    main,
    should_run,
    run_app,
)
from unittest.mock import MagicMock, patch, call
from prozorro_crawler.settings import (
    BASE_URL,
    API_RESOURCE,
    API_OPT_FIELDS,
)
from .base import AsyncMock
import pytest
import json


@pytest.mark.asyncio
@patch("prozorro_crawler.main.asyncio.sleep", new_callable=MagicMock)
@patch("prozorro_crawler.main.Lock")
@patch("prozorro_crawler.main.asyncio.get_event_loop")
@patch("prozorro_crawler.main.close_connection", new_callable=MagicMock)
async def test_main_function(
    close_connection_mock: MagicMock,
    get_event_loop_mock: MagicMock,
    lock_mock: MagicMock,
    sleep_mock: MagicMock,
) -> None:
    data_handler = AsyncMock()
    init_task = AsyncMock()
    headers = {"Authorization": "Smile", "Content-Type": "Letters?"}

    main(data_handler, init_task, additional_headers=headers)

    lock_mock.run_locked.assert_called_once()
    assert get_event_loop_mock.return_value.run_until_complete.mock_calls == [
        call(lock_mock.run_locked.return_value),
        call(close_connection_mock()),
        call(sleep_mock(0.250)),
    ]


@pytest.mark.asyncio
@patch("prozorro_crawler.main.aiohttp.ClientSession")
@patch("prozorro_crawler.main.init_crawler")
async def test_init_crawler_saved_feed(
    init_crawler_mock: MagicMock,
    client_mock: MagicMock,
) -> None:
    data_handler = AsyncMock()
    prepare_storage = AsyncMock()
    additional_headers = {"User-Agent": "Safari", "Auth": "Token"}
    session = AsyncMock()
    client_mock.return_value = session

    try:
        await run_app(
            data_handler,
            init_task=prepare_storage,
            json_loads=json.loads,
            additional_headers=additional_headers,
        )
    except StopAsyncIteration:
        pass

    prepare_storage.assert_called_once()
    assert client_mock.call_args.kwargs.get("headers") == additional_headers
    url = f"{BASE_URL}/{API_RESOURCE}"
    opt_fields = ",".join(API_OPT_FIELDS)
    assert init_crawler_mock.mock_calls == [
        call(
            should_run,
            session,
            url,
            data_handler,
            opt_fields=opt_fields,
            json_loads=json.loads,
        ),
    ]
