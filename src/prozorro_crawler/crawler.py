from datetime import timedelta

import aiohttp
import asyncio
import json
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
    DATE_MODIFIED_SKIP_STATUSES,
    DATE_MODIFIED_LOCK_ENABLED,
    DATE_MODIFIED_MARGIN_SECONDS,
)
from prozorro_crawler.storage import (
    lock_feed_position,
    save_feed_position,
    get_feed_position,
    unlock_feed_position,
    drop_feed_position,
    BACKWARD_OFFSET_KEY,
    FORWARD_OFFSET_KEY,
    SERVER_ID_KEY,
    LOCK_DATE_MODIFIED_KEY,
    LATEST_DATE_MODIFIED_KEY,
)
from prozorro_crawler.utils import (
    get_session_server_id,
    get_offset_key,
    get_date_modified_key,
    parse_dt_string,
    SERVER_ID_COOKIE_NAME,
)


DATE_MODIFIED_MARGIN = timedelta(seconds=DATE_MODIFIED_MARGIN_SECONDS)


async def init_crawler(should_run, session, url, data_handler, json_loads=json.loads, **kwargs):
    logger.info(
        f"Start crawling",
        extra={
            "MESSAGE_ID": "START_CRAWLING",
            "FEED_URL": url,
        }
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
            and BACKWARD_OFFSET_KEY in feed_position
            and FORWARD_OFFSET_KEY in feed_position
        ):
            logger.info(
                f"Start from saved position: {feed_position}",
                extra={
                    "MESSAGE_ID": "LOAD_CRAWLER_POSITION",
                    "FEED_URL": url,
                }
            )
            forward_offset = feed_position[FORWARD_OFFSET_KEY]
            backward_offset = feed_position[BACKWARD_OFFSET_KEY]
            server_id = feed_position.get(SERVER_ID_KEY)
            if server_id:
                session.cookie_jar.update_cookies({SERVER_ID_COOKIE_NAME: server_id})
        else:
            backward_offset, forward_offset = await init_feed(
                should_run,
                session,
                url,
                data_handler,
                json_loads=json_loads,
                **kwargs,
            )
        await asyncio.gather(
            # forward crawler
            crawler(
                should_run,
                session,
                url,
                data_handler,
                json_loads=json_loads,
                offset=forward_offset,
                **kwargs,
            ),
            # backward crawler
            crawler(
                should_run,
                session,
                url,
                data_handler,
                json_loads=json_loads,
                offset=backward_offset,
                descending="1",
                **kwargs,
            ),
        )


async def init_feed(should_run, session, url, data_handler, json_loads, **kwargs):
    feed_params = get_feed_params(descending="1", **kwargs)
    logger.info(
        "Crawler initialization",
        extra={
            "MESSAGE_ID": "CRAWLER_INIT",
            "FEED_URL": url,
        }
    )
    while should_run():
        try:
            resp = await session.get(url, params=feed_params)
        except aiohttp.ClientError as e:
            logger.warning(
                f"Init feed {type(e)}: {e}",
                extra={
                    "MESSAGE_ID": "HTTP_EXCEPTION",
                    "FEED_URL": url,
                }
            )
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
        else:
            if resp.status == 200:
                try:
                    response = await resp.json(loads=json_loads)
                except (aiohttp.ClientPayloadError, JSONDecodeError) as e:
                    logger.warning(e, extra={"MESSAGE_ID": "HTTP_EXCEPTION"})
                    await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
                    continue

                await data_handler(session, response["data"])
                return float(response["next_page"]["offset"]), float(response["prev_page"]["offset"])
            else:
                logger.error(
                    "Error on feed initialize request: {} {}".format(
                        resp.status,
                        await resp.text()
                    ),
                    extra={
                        "MESSAGE_ID": "FEED_ERROR",
                        "FEED_URL": url,
                    }
                )
            await asyncio.sleep(FEED_STEP_INTERVAL)


async def crawler(should_run, session, url, data_handler, json_loads=json.loads, **kwargs):
    feed_params = get_feed_params(**kwargs)
    logger.info(
        "Crawler started",
        extra={
            "MESSAGE_ID": "CRAWLER_STARTED",
            "FEED_URL": url,
            "FEED_PARAMS": feed_params,
        }
    )
    while should_run():
        logger.debug(
            "Feed request",
            extra={
                "MESSAGE_ID": "FEED_REQUEST",
                "FEED_URL": url,
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
                extra={
                    "MESSAGE_ID": "HTTP_EXCEPTION",
                    "FEED_URL": url,
                }
            )
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
        else:
            if resp.status == 200:
                try:
                    response = await resp.json(loads=json_loads)
                except (aiohttp.ClientPayloadError, JSONDecodeError) as e:
                    logger.warning(e, extra={
                        "MESSAGE_ID": "HTTP_EXCEPTION",
                        "FEED_URL": url,
                    })
                    await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
                    continue

                if response["data"]:
                    await data_handler(session, response["data"])
                    await save_crawler_position(
                        session, url, response, descending=feed_params["descending"]
                    )

                if await check_crawler_should_stop(
                    session, url, response, descending=feed_params["descending"]
                ):
                    logger.info(
                        "Stop backward crawling",
                        extra={
                            "MESSAGE_ID": "BACK_CRAWLER_STOP",
                            "FEED_URL": url,
                        }
                    )
                    break  # stop crawling

                feed_params.update(offset=float(response["next_page"]["offset"]))

                if len(response["data"]) < API_LIMIT:
                    await asyncio.sleep(NO_ITEMS_INTERVAL)

            elif resp.status == 429:
                logger.warning(
                    "Too many requests while getting feed",
                    extra={
                        "MESSAGE_ID": "TOO_MANY_REQUESTS",
                        "FEED_URL": url,
                    }
                )
                await asyncio.sleep(TOO_MANY_REQUESTS_INTERVAL)

            elif resp.status == 412:
                logger.warning(
                    "Precondition failed",
                    extra={
                        "MESSAGE_ID": "PRECONDITION_FAILED",
                        "FEED_URL": url,
                    }
                )

            elif resp.status == 404:
                logger.error(
                    "Offset expired/invalid",
                    extra={
                        "MESSAGE_ID": "OFFSET_INVALID",
                        "FEED_URL": url,
                    }
                )
                await drop_crawler_position(
                    session, url, descending=feed_params["descending"]
                )
                break  # stop crawling
            else:
                logger.error(
                    "Crawler request error: {} {}".format(
                        resp.status,
                        await resp.text()
                    ),
                    extra={
                        "MESSAGE_ID": "FEED_UNEXPECTED_ERROR",
                        "FEED_URL": url,
                    }
                )
            await asyncio.sleep(FEED_STEP_INTERVAL)

    logger.info(
        "Crawler stopped",
        extra={
            "MESSAGE_ID": "CRAWLER_STOPPED",
            "FEED_URL": url,
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


async def save_crawler_position(session, url, response, descending=False):
    feed_position_data = {}

    if response["data"]:
        feed_position = await get_feed_position()
        if (
            not feed_position or
            not DATE_MODIFIED_LOCK_ENABLED or
            not feed_position.get(LOCK_DATE_MODIFIED_KEY, False)
        ):
            date_modified = get_feed_date_modified(response)
            if date_modified:
                feed_position_data[get_date_modified_key(descending)] = date_modified
        else:
            logger.debug(
                f"Skip save date modified position for locked crawler position.",
                extra={
                    "MESSAGE_ID": "CRAWLER_SKIP_SAVE_DATE_MODIFIED",
                    "FEED_URL": url,
                }
            )

    feed_position_data[get_offset_key(descending)] = response["next_page"]["offset"]
    feed_position_data[SERVER_ID_KEY] = get_session_server_id(session)
    await save_feed_position(feed_position_data)


async def drop_crawler_position(session, url, descending=False):
    await drop_feed_position()
    logger.info(
        f"Drop feed position.",
        extra={
            "MESSAGE_ID": "CRAWLER_DROP_FEED_POSITION",
            "FEED_URL": url,
        }
    )
    if DATE_MODIFIED_LOCK_ENABLED:
        await lock_feed_position()
        logger.info(
            f"Lock feed position date modified.",
            extra={
                "MESSAGE_ID": "CRAWLER_LOCK_DATE_MODIFIED",
                "FEED_URL": url,
            }
        )


async def check_crawler_should_stop(session, url, response, descending=False):
    if not response["data"] and descending:
        # got all ancient stuff
        return True

    if response["data"] and descending and DATE_MODIFIED_LOCK_ENABLED:
        feed_position = await get_feed_position()
        date_modified = get_feed_date_modified(response)
        date_modified_saved = feed_position.get(LATEST_DATE_MODIFIED_KEY)
        if date_modified and date_modified_saved and parse_dt_string(date_modified) < (
            parse_dt_string(date_modified_saved) - DATE_MODIFIED_MARGIN
        ):
            logger.info(
                f"Reached saved dateModified {date_modified_saved} + "
                f"{DATE_MODIFIED_MARGIN_SECONDS} seconds.",
                extra={
                    "MESSAGE_ID": "CRAWLER_DATE_MODIFIED_REACHED",
                    "FEED_URL": url,
                }
            )
            await unlock_feed_position()
            logger.info(
                f"Unlock feed position date modified.",
                extra={
                    "MESSAGE_ID": "CRAWLER_UNLOCK_DATE_MODIFIED",
                    "FEED_URL": url,
                }
            )
            return True

    return False


def get_feed_date_modified(response):
    for item in reversed(response["data"]):
        date_modified = item.get("dateModified")
        if date_modified:
            status = item.get("status")
            # skip statuses where item doesn't change its date modified
            # so that we skip outdated date modified
            # Example:
            # tender doesn't change date modified in active.tendering on bidding
            if not status or status not in DATE_MODIFIED_SKIP_STATUSES:
                return date_modified
            else:
                logger.debug(f"Ignore date modified for feed item in {status} status")
