# -*- coding: utf-8 -*-
from prozorro_crawler.storage import get_feed_position, drop_feed_position, save_feed_position
from prozorro_crawler.settings import (
    logger, BASE_URL, PUBLIC_API_HOST, API_LIMIT, API_OPT_FIELDS, API_MODE,
    CONNECTION_ERROR_INTERVAL, FEED_STEP_INTERVAL, NO_ITEMS_INTERVAL, TOO_MANY_REQUESTS_INTERVAL,
)
from json.decoder import JSONDecodeError
import aiohttp
import asyncio
import signal


RUN = True


async def run_app(data_handler, init_task=None, additional_headers=None):

    if init_task is not None:
        await init_task()

    logger.info(f"Start crawling {BASE_URL}", extra={"MESSAGE_ID": "START_CRAWLING"})
    conn = aiohttp.TCPConnector(ttl_dns_cache=300)
    headers = {"User-Agent": "ProZorro Crawler 2.0"}
    if isinstance(additional_headers, dict):
        headers.update(additional_headers)
    async with aiohttp.ClientSession(connector=conn, headers=headers) as session:
        while RUN:
            """
            At the end of this block we await for forward and backward crawlers
            Backward crawler finishes when there're no results.
            Forward crawler finishes only on 404( hen offset is invalid)
            in this case the whole process should be reinitialized
            """
            feed_position = await get_feed_position()
            if feed_position and "backward_offset" in feed_position and "forward_offset" in feed_position:
                logger.info(f"Start from saved position: {feed_position}",
                            extra={"MESSAGE_ID": "LOAD_CRAWLER_POSITION"})
                forward_offset = feed_position["forward_offset"]
                backward_offset = feed_position["backward_offset"]
                server_id = feed_position.get("server_id")
                if server_id:
                    session.cookie_jar.update_cookies({"SERVER_ID": server_id})
            else:
                backward_offset, forward_offset = await init_feed(session, data_handler)

            await asyncio.gather(
                crawler(session, data_handler, offset=forward_offset),
                crawler(session, data_handler, offset=backward_offset, descending="1"),  # backward crawler
            )

        # we actually don't need this, as crawlers await their tasks now, but just in case
        pending_tasks = asyncio.all_tasks() - {asyncio.current_task()}
        logger.info("Waiting for tasks to finish",
                    extra={"AWAIT_PENDING_TASKS": len(pending_tasks), "MESSAGE_ID": "AWAIT_TASKS_ON_STOP"})
        await asyncio.gather(*pending_tasks)


async def init_feed(session, data_handler):
    feed_params = get_feed_params(descending="1")
    logger.info("Crawler initialization", extra={"MESSAGE_ID": "CRAWLER_INIT"})
    while True:
        try:
            resp = await session.get(BASE_URL, params=feed_params)
        except aiohttp.ClientError as e:
            logger.warning(f"Init feed {type(e)}: {e}", extra={"MESSAGE_ID": "HTTP_EXCEPTION"})
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
        else:
            if resp.status == 200:
                try:
                    init_response = await resp.json()
                except (aiohttp.ClientPayloadError, JSONDecodeError) as e:
                    logger.warning(e, extra={"MESSAGE_ID": "HTTP_EXCEPTION"})
                    await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
                    continue

                await data_handler(session, init_response["data"])
                return init_response["next_page"]["offset"], init_response["prev_page"]["offset"]
            else:
                logger.error(
                    "Error on feed initialize request: {} {}".format(
                        resp.status,
                        await resp.text()
                    ),
                    extra={"MESSAGE_ID": "FEED_ERROR"}
                )
            await asyncio.sleep(FEED_STEP_INTERVAL)


async def crawler(session, data_handler, **kwargs):
    feed_params = get_feed_params(**kwargs)
    while RUN:
        logger.info(
            f"Feed request: {feed_params}",
            extra={"TASKS_LEN": len(asyncio.all_tasks()), "MESSAGE_ID": "FEED_REQUEST"}
        )
        try:
            resp = await session.get(BASE_URL, params=feed_params)
        except aiohttp.ClientError as e:
            logger.warning(f"Crawler {type(e)}: {e}", extra={"MESSAGE_ID": "HTTP_EXCEPTION"})
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
        else:
            if resp.status == 200:
                try:
                    response = await resp.json()
                except (aiohttp.ClientPayloadError, JSONDecodeError) as e:
                    logger.warning(e, extra={"MESSAGE_ID": "HTTP_EXCEPTION"})
                    await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
                    continue
                if response["data"]:
                    await data_handler(session, response["data"])
                    await save_crawler_position(session, response, descending=feed_params["descending"])
                    feed_params.update(offset=response["next_page"]["offset"])

                elif feed_params["descending"]:
                    logger.info("Stop backward crawling", extra={"MESSAGE_ID": "BACK_CRAWLER_STOP"})
                    break  # got all ancient stuff; stop crawling

                if len(response["data"]) < API_LIMIT:
                    await asyncio.sleep(NO_ITEMS_INTERVAL)

            elif resp.status == 429:
                logger.warning("Too many requests while getting feed", extra={"MESSAGE_ID": "TOO_MANY_REQUESTS"})
                await asyncio.sleep(TOO_MANY_REQUESTS_INTERVAL)

            elif resp.status == 412:
                logger.warning("Precondition failed", extra={"MESSAGE_ID": "PRECONDITION_FAILED"})

            elif resp.status == 404:
                logger.error("Offset expired/invalid", extra={"MESSAGE_ID": "OFFSET_INVALID"})
                await drop_feed_position()
                break  # stop crawling
            else:
                logger.error(
                    "Crawler request error: {} {}".format(
                        resp.status,
                        await resp.text()
                    ),
                    extra={"MESSAGE_ID": "FEED_UNEXPECTED_ERROR"}
                )
            await asyncio.sleep(FEED_STEP_INTERVAL)

    logger.info("Crawler stopped", extra={"FEED_PARAMS": feed_params, "MESSAGE_ID": "CRAWLER_STOPPED"})


async def process_tender(session, tender_id, process_function):
    while True:
        try:
            resp = await session.get(f"{BASE_URL}/{tender_id}")
        except aiohttp.ClientError as e:
            logger.warning(f"Tender {type(e)}: {e}", extra={"MESSAGE_ID": "HTTP_EXCEPTION"})
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
        else:
            if resp.status == 200:
                try:
                    response = await resp.json()
                except (aiohttp.ClientPayloadError, JSONDecodeError) as e:
                    logger.warning(e, extra={"MESSAGE_ID": "HTTP_EXCEPTION"})
                    await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
                else:
                    return await process_function(session, response["data"])
            elif resp.status == 429:
                logger.warning("Too many requests while getting tender",
                               extra={"MESSAGE_ID": "TOO_MANY_REQUESTS"})
                await asyncio.sleep(TOO_MANY_REQUESTS_INTERVAL)
            else:
                return logger.error(
                    "Error on getting tender: {} {}".format(
                        resp.status,
                        await resp.text()
                    ),
                    extra={"MESSAGE_ID": "TENDER_UNEXPECTED_ERROR"}
                )


def get_feed_params(**kwargs):
    feed_params = dict(
        feed="changes",
        descending="",
        offset="",
        limit=API_LIMIT,
        opt_fields="%2C".join(API_OPT_FIELDS),
        mode=API_MODE,
    )
    feed_params.update(kwargs)
    return feed_params


async def save_crawler_position(session, response, descending=False):
    data = {
       "backward_offset" if descending else "forward_offset": response["next_page"]["offset"]
    }
    if response["data"]:
        if descending:
            data["earliest_date_modified"] = response["data"][-1]["dateModified"]
        else:
            data["latest_date_modified"] = response["data"][-1]["dateModified"]

    filtered = session.cookie_jar.filter_cookies(PUBLIC_API_HOST)
    server_id = filtered.get("SERVER_ID")
    if server_id:
        data["server_id"] = server_id.value
    await save_feed_position(data)


def get_stop_signal_handler(sig):
    def handler(signum, frame):
        global RUN
        logger.warning(f"Handling {sig} signal: stopping crawlers", extra={"MESSAGE_ID": "HANDLE_STOP_SIG"})
        RUN = False
    return handler


def main(data_handler, init_task=None, additional_headers=None):
    signal.signal(signal.SIGINT, get_stop_signal_handler("SIGINT"))
    signal.signal(signal.SIGTERM, get_stop_signal_handler("SIGTERM"))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_app(data_handler, init_task=init_task, additional_headers=additional_headers))
    # Wait 250 ms for the underlying SSL connections to close
    loop.run_until_complete(asyncio.sleep(0.250))
    loop.close()


async def dummy_data_handler(session, items):
    for item in items:
        logger.info(f"Processing {item['id']}")

if __name__ == '__main__':
    main(dummy_data_handler)
