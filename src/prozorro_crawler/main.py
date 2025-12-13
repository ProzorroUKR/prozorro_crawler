from types import FrameType
from typing import Callable, Any, Awaitable, Optional

import aiohttp
import asyncio
import signal
import json
from aiohttp.typedefs import JSONDecoder
from prozorro_crawler.crawler import init_crawler
from prozorro_crawler.lock import Lock
from prozorro_crawler.storage import close_connection
from prozorro_crawler.settings import (
    logger,
    API_OPT_FIELDS,
    API_RESOURCE,
)
from prozorro_crawler.utils import (
    get_default_headers,
    get_resource_url,
)

RUN = True


def should_run() -> bool:
    return RUN


def stop_run() -> None:
    global RUN
    RUN = False


async def run_app(
    data_handler: Callable[
        [aiohttp.ClientSession, list[dict[str, Any]]],
        Awaitable[None],
    ],
    json_loads: JSONDecoder,
    init_task: Optional[Callable[[], Awaitable[None]]] = None,
    additional_headers: Optional[dict[str, str]] = None,
    resource: str = API_RESOURCE,
    opt_fields: list[str] = API_OPT_FIELDS,
) -> None:
    if init_task is not None:
        await init_task()

    conn = aiohttp.TCPConnector(ttl_dns_cache=300)
    headers = get_default_headers(additional_headers)
    async with aiohttp.ClientSession(connector=conn, headers=headers) as session:
        url = get_resource_url(resource)
        await init_crawler(
            should_run,
            session,
            url,
            data_handler,
            opt_fields=",".join(opt_fields),
            json_loads=json_loads,
        )


def get_stop_signal_handler(sig: str) -> Callable[[int, Optional[FrameType]], None]:
    def handler(signum: int, frame: Optional[FrameType]) -> None:
        logger.warning(
            f"Handling {sig} signal: stopping crawlers",
            extra={"MESSAGE_ID": "HANDLE_STOP_SIG"},
        )
        stop_run()

    return handler


def main(
    data_handler: Callable[
        [aiohttp.ClientSession, list[dict[str, Any]]],
        Awaitable[None],
    ],
    init_task: Optional[Callable[[], Awaitable[None]]] = None,
    additional_headers: Optional[dict[str, str]] = None,
    resource: str = API_RESOURCE,
    opt_fields: list[str] = API_OPT_FIELDS,
    json_loads: Optional[JSONDecoder] = None,
) -> None:
    signal.signal(signal.SIGINT, get_stop_signal_handler("SIGINT"))
    signal.signal(signal.SIGTERM, get_stop_signal_handler("SIGTERM"))

    loop = asyncio.get_event_loop()

    def get_app() -> Awaitable[None]:
        app = run_app(
            data_handler,
            resource=resource,
            opt_fields=opt_fields,
            init_task=init_task,
            additional_headers=additional_headers,
            json_loads=json_loads or json.loads,
        )
        return app

    loop.run_until_complete(Lock.run_locked(get_app, should_run))
    # Wait 250 ms for the underlying SSL connections to close
    loop.run_until_complete(close_connection())
    loop.run_until_complete(asyncio.sleep(0.250))
    loop.close()


async def dummy_data_handler(
    session: aiohttp.ClientSession,
    items: list[dict[str, Any]],
) -> None:
    for item in items:
        logger.info(f"Processing {item['id']}")
        await asyncio.sleep(1)


if __name__ == "__main__":
    main(dummy_data_handler)
