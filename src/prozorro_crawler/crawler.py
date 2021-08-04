import aiohttp
import asyncio
from json.decoder import JSONDecodeError

from prozorro_crawler.settings import (
    logger,
    API_OPT_FIELDS,
    CONNECTION_ERROR_INTERVAL,
    FEED_STEP_INTERVAL,
    API_LIMIT,
    NO_ITEMS_INTERVAL,
    TOO_MANY_REQUESTS_INTERVAL,
    API_MODE,
)
from prozorro_crawler.storage import (
    drop_feed_position,
    save_feed_position,
    get_feed_position,
)
from prozorro_crawler.utils import (
    get_session_server_id,
    SERVER_ID_COOKIE_NAME,
)


async def init_crawler(should_run, session, url, data_handler, **kwargs):
    logger.info(
        f"Start crawling {url}",
        extra={"MESSAGE_ID": "START_CRAWLING"}
    )
    while should_run():
        """
        At the end of this block we await for forward and backward crawlers
        Backward crawler finishes when there're no results.
        Forward crawler finishes only on 404 (when offset is invalid)
        in this case the whole process should be reinitialized
        """
        feed_position = await get_feed_position()
        if (
            feed_position
            and "backward_offset" in feed_position
            and "forward_offset" in feed_position
        ):
            logger.info(
                f"Start from saved position: {feed_position}",
                extra={"MESSAGE_ID": "LOAD_CRAWLER_POSITION"}
            )
            forward_offset = feed_position["forward_offset"]
            backward_offset = feed_position["backward_offset"]
            server_id = feed_position.get("server_id")
            if server_id:
                session.cookie_jar.update_cookies({SERVER_ID_COOKIE_NAME: server_id})
        else:
            backward_offset, forward_offset = await init_feed(
                should_run,
                session,
                url,
                data_handler,
                **kwargs,
            )
        await asyncio.gather(
            # forward crawler
            crawler(
                should_run,
                session,
                url,
                data_handler,
                offset=forward_offset,
                **kwargs,
            ),
            # backward crawler
            crawler(
                should_run,
                session,
                url,
                data_handler,
                offset=backward_offset,
                descending="1",
                **kwargs,
            ),
        )


async def init_feed(should_run, session, url, data_handler, **kwargs):
    feed_params = get_feed_params(descending="1", **kwargs)
    logger.info(
        "Crawler initialization",
        extra={"MESSAGE_ID": "CRAWLER_INIT"}
    )
    while should_run():
        try:
            resp = await session.get(url, params=feed_params)
        except aiohttp.ClientError as e:
            logger.warning(
                f"Init feed {type(e)}: {e}",
                extra={"MESSAGE_ID": "HTTP_EXCEPTION"}
            )
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
        else:
            if resp.status == 200:
                try:
                    response = await resp.json()
                except (aiohttp.ClientPayloadError, JSONDecodeError) as e:
                    logger.warning(e, extra={"MESSAGE_ID": "HTTP_EXCEPTION"})
                    await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
                    continue

                await data_handler(session, response["data"])
                return response["next_page"]["offset"], response["prev_page"]["offset"]
            else:
                logger.error(
                    "Error on feed initialize request: {} {}".format(
                        resp.status,
                        await resp.text()
                    ),
                    extra={"MESSAGE_ID": "FEED_ERROR"}
                )
            await asyncio.sleep(FEED_STEP_INTERVAL)


async def crawler(should_run, session, url, data_handler, **kwargs):
    feed_params = get_feed_params(**kwargs)
    logger.info(
        "Crawler started",
        extra={
            "MESSAGE_ID": "CRAWLER_STARTED",
            "FEED_PARAMS": feed_params,
        }
    )
    while should_run():
        logger.debug(
            "Feed request",
            extra={
                "MESSAGE_ID": "FEED_REQUEST",
                "FEED_PARAMS": feed_params,
                # this requires python 3.7
                # "TASKS_LEN": len(asyncio.all_tasks()),
            }
        )
        try:
            resp = await session.get(url, params=feed_params)
        except aiohttp.ClientError as e:
            logger.warning(
                f"Crawler {type(e)}: {e}",
                extra={"MESSAGE_ID": "HTTP_EXCEPTION"}
            )
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
                    await save_crawler_position(
                        session, response, descending=feed_params["descending"]
                    )
                    feed_params.update(offset=response["next_page"]["offset"])

                elif feed_params["descending"]:
                    logger.info(
                        "Stop backward crawling",
                        extra={"MESSAGE_ID": "BACK_CRAWLER_STOP"}
                    )
                    break  # got all ancient stuff; stop crawling

                if len(response["data"]) < API_LIMIT:
                    await asyncio.sleep(NO_ITEMS_INTERVAL)

            elif resp.status == 429:
                logger.warning(
                    "Too many requests while getting feed",
                    extra={"MESSAGE_ID": "TOO_MANY_REQUESTS"}
                )
                await asyncio.sleep(TOO_MANY_REQUESTS_INTERVAL)

            elif resp.status == 412:
                logger.warning(
                    "Precondition failed",
                    extra={"MESSAGE_ID": "PRECONDITION_FAILED"}
                )

            elif resp.status == 404:
                logger.error(
                    "Offset expired/invalid",
                    extra={"MESSAGE_ID": "OFFSET_INVALID"}
                )
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

    logger.info(
        "Crawler stopped",
        extra={
            "MESSAGE_ID": "CRAWLER_STOPPED",
            "FEED_PARAMS": feed_params,
        }
    )


def get_feed_params(**kwargs):
    feed_params = dict(
        feed="changes",
        descending="",
        offset="",
        limit=API_LIMIT,
        opt_fields=",".join(API_OPT_FIELDS),
        mode=API_MODE,
    )
    feed_params.update(kwargs)
    return feed_params


async def save_crawler_position(session, response, descending=False):
    position_data = {}

    offset_key = "backward_offset" if descending else "forward_offset"
    position_data[offset_key] = response["next_page"]["offset"]

    if response["data"]:
        date_modified_key = "earliest_date_modified" if descending else "latest_date_modified"
        position_data[date_modified_key] = response["data"][-1]["dateModified"]

    position_data["server_id"] = get_session_server_id(session)
    await save_feed_position(position_data)
