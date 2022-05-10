import aiohttp
import asyncio
import signal
import json

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


def should_run():
    return RUN


def stop_run():
    global RUN
    RUN = False


async def run_app(
    data_handler,
    init_task=None,
    additional_headers=None,
    resource=API_RESOURCE,
    opt_fields=API_OPT_FIELDS,
    json_loads=json.loads,
):

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


def get_stop_signal_handler(sig):
    def handler(signum, frame):
        logger.warning(
            f"Handling {sig} signal: stopping crawlers",
            extra={"MESSAGE_ID": "HANDLE_STOP_SIG"}
        )
        stop_run()
    return handler


def main(
    data_handler,
    init_task=None,
    additional_headers=None,
    resource=API_RESOURCE,
    opt_fields=API_OPT_FIELDS,
    json_loads=json.loads,
):
    signal.signal(signal.SIGINT, get_stop_signal_handler("SIGINT"))
    signal.signal(signal.SIGTERM, get_stop_signal_handler("SIGTERM"))

    loop = asyncio.get_event_loop()

    def get_app():
        app = run_app(
            data_handler,
            resource=resource,
            opt_fields=opt_fields,
            init_task=init_task,
            additional_headers=additional_headers,
            json_loads=json_loads,
        )
        return app

    loop.run_until_complete(
        Lock.run_locked(get_app, should_run)
    )
    # Wait 250 ms for the underlying SSL connections to close
    loop.run_until_complete(close_connection())
    loop.run_until_complete(asyncio.sleep(0.250))
    loop.close()


async def dummy_data_handler(session, items):
    for item in items:
        logger.info(f"Processing {item['id']}")
        await asyncio.sleep(1)


if __name__ == '__main__':
    main(dummy_data_handler)
