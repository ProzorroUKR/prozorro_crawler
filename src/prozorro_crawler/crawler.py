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
    BACKWARD_OFFSET,
    FORWARD_OFFSET,
    FORWARD_CHANGES_COOLDOWN_SECONDS,
    SLEEP_FORWARD_CHANGES_SECONDS,
)
from prozorro_crawler.storage import (
    save_feed_position,
    get_feed_position,
    drop_feed_position,
    BACKWARD_OFFSET_KEY,
    FORWARD_OFFSET_KEY,
    SERVER_ID_KEY,
)
from prozorro_crawler.utils import (
    get_offset_age,
    get_session_server_id,
    get_offset_key,
    get_date_modified_key,
    SERVER_ID_COOKIE_NAME,
)

DEFAULT_FEED_PARAMS = dict(
    descending="",
    offset="",
    limit=API_LIMIT,
    opt_fields=",".join(API_OPT_FIELDS),
    mode=API_MODE,
)

async def init_crawler(should_run, session, url, data_handler, json_loads=json.loads, **kwargs):
    """
    Crawlers start point
    Initialize two crawlers and run them in parallel
    Forward crawler: waiting for new data
    Backward crawler: processing all ancient data
    """
    logger.info(
        "Start crawling",
        extra={
            "MESSAGE_ID": "START_CRAWLING",
            "FEED_URL": url,
        }
    )
    
    # At the end of this block we await for forward and backward crawlers
    # Backward crawler finishes when there're no results.
    # Forward crawler finishes only on 404 (when offset is invalid)
    # in this case the whole process should be reinitialized
    while should_run():

        # Get current feed position from storage
        feed_position = await get_feed_position()
        
        # If we have saved position, use it
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

            if server_id := feed_position.get(SERVER_ID_KEY):
                session.cookie_jar.update_cookies({SERVER_ID_COOKIE_NAME: server_id})

        # If we don't have saved position, use default offsets if they are set
        elif BACKWARD_OFFSET and FORWARD_OFFSET:
            # Only used in first initialization if no saved position in db
            backward_offset = BACKWARD_OFFSET
            forward_offset = FORWARD_OFFSET

        # If we don't have default offsets, initialize feed from scratch
        else:
            backward_offset, forward_offset = await init_feed(
                should_run,
                session,
                url,
                data_handler,
                json_loads=json_loads,
                **kwargs,
            )

        # Run 2 crawlers in parallel
        await asyncio.gather(
            # Forward crawler
            # It should run forever waiting for new data
            crawler(
                should_run,
                session,
                url,
                data_handler,
                json_loads=json_loads,
                offset=forward_offset,
                **kwargs
            ),
            # Backward crawler
            # It should run until all ancient data is processed
            crawler(
                should_run,
                session,
                url,
                data_handler,
                json_loads=json_loads,
                offset=backward_offset,
                descending="1",
                **kwargs
            ),
        )


async def init_feed(should_run, session, url, data_handler, json_loads, **kwargs):
    feed_params = DEFAULT_FEED_PARAMS.copy()
    feed_params.update(kwargs)

    # Initialize crawler from feed head (newest data)
    feed_params["descending"] = "1"

    logger.info(
        "Crawler initialization",
        extra={
            "MESSAGE_ID": "CRAWLER_INIT",
            "FEED_URL": url,
            "FEED_PARAMS": feed_params,
        }
    )

    while should_run():
        try:
            # Make request to feed head
            resp = await session.get(url, params=feed_params)
        except aiohttp.ClientError as e:
            logger.warning(
                f"Init feed exception: {type(e)} {e}",
                extra={
                    "MESSAGE_ID": "HTTP_EXCEPTION",
                    "FEED_URL": url,
                }
            )
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
            continue

        if resp.status != 200:
            logger.error(
                f"Error on feed initialize request: {resp.status} {await resp.text()}",
                extra={
                    "MESSAGE_ID": "FEED_ERROR",
                    "FEED_URL": url,
                }
            )
            await asyncio.sleep(FEED_STEP_INTERVAL)
            continue

        # No errors, try to parse response
        try:
            response = await resp.json(loads=json_loads)
        except (aiohttp.ClientPayloadError, JSONDecodeError) as e:
            logger.warning(e, extra={"MESSAGE_ID": "HTTP_EXCEPTION"})
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
            continue

        # Process data
        await data_handler(session, response["data"])

        # Return offsets for crawlers
        return (
            response.get("next_page", {}).get("offset") or "",
            response.get("prev_page", {}).get("offset") or ""
        )


async def crawler(should_run, session, url, data_handler, json_loads=json.loads, **kwargs):
    """
    Single crawler loop
    """
    feed_params = DEFAULT_FEED_PARAMS.copy()
    feed_params.update(kwargs)

    logger.info(
        "Crawler started",
        extra={
            "MESSAGE_ID": "CRAWLER_STARTED",
            "FEED_URL": url,
            "FEED_PARAMS": feed_params,
        }
    )

    while should_run():
        # Ensure new forward page is cooked enough
        if FORWARD_CHANGES_COOLDOWN_SECONDS and SLEEP_FORWARD_CHANGES_SECONDS:
            offset_age = get_offset_age(feed_params["offset"])
            if offset_age is None:
                logger.critical(
                    f"Can't detect offset age for cooldown, "
                    f"probably offset has invalid format: {feed_params['offset']}",
                    extra={
                        "MESSAGE_ID": "INVALID_OFFSET",
                        "FEED_URL": url,
                    }
                )
            elif offset_age < FORWARD_CHANGES_COOLDOWN_SECONDS:
                # Pause processing to allow the forward page to stabilize
                # This helps avoid processing rapidly changing records
                logger.info(
                    f"New data is less than {FORWARD_CHANGES_COOLDOWN_SECONDS} seconds old, "
                    f"sleeping for {SLEEP_FORWARD_CHANGES_SECONDS} seconds",
                    extra={
                        "MESSAGE_ID": "SLEEP_FORWARD_CHANGES",
                        "FEED_URL": url,
                    }
                )
                await asyncio.sleep(SLEEP_FORWARD_CHANGES_SECONDS)
                continue

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
            # Make request to feed
            resp = await session.get(url, params=feed_params)
        except aiohttp.ClientError as e:
            logger.warning(
                f"Crawler exception: {type(e)} {e}",
                extra={
                    "MESSAGE_ID": "HTTP_EXCEPTION",
                    "FEED_URL": url,
                }
            )
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
            continue

        if resp.status == 429:
            logger.warning(
                "Too many requests while getting feed",
                extra={
                    "MESSAGE_ID": "TOO_MANY_REQUESTS",
                    "FEED_URL": url,
                }
            )
            await asyncio.sleep(TOO_MANY_REQUESTS_INTERVAL)
            await asyncio.sleep(FEED_STEP_INTERVAL)
            continue

        elif resp.status == 412:
            logger.warning(
                "Precondition failed",
                extra={
                    "MESSAGE_ID": "PRECONDITION_FAILED",
                    "FEED_URL": url,
                }
            )
            await asyncio.sleep(FEED_STEP_INTERVAL)
            continue

        elif resp.status == 404:
            logger.error(
                "Invalid offset",
                extra={
                    "MESSAGE_ID": "OFFSET_INVALID",
                    "FEED_URL": url,
                }
            )
            await drop_feed_position()
            logger.info(
                f"Drop feed position.",
                extra={
                    "MESSAGE_ID": "CRAWLER_DROP_FEED_POSITION",
                    "FEED_URL": url,
                }
            )
            # The feed is messed up for some reason
            # Stop crawling
            break 

        elif resp.status != 200:
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
            continue

        # No errors, try to parse response
        try:
            response = await resp.json(loads=json_loads)
        except (aiohttp.ClientPayloadError, JSONDecodeError) as e:
            logger.warning(e, extra={
                "MESSAGE_ID": "HTTP_EXCEPTION",
                "FEED_URL": url,
            })
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
            continue

        if not response["data"] and feed_params["descending"]:
            # Got empty response for backward crawler
            # That's mean we got all ancient stuff
            # Time to stop backward crawler
            logger.info(
                "Stop backward crawling",
                extra={
                    "MESSAGE_ID": "BACK_CRAWLER_STOP",
                    "FEED_URL": url,
                }
            )
            if BACKWARD_OFFSET:
                # In case of initial backward offset was set to feed start
                # we need to save it because we will got empty response 
                # and will not hit usual position save
                offset_key = get_offset_key(feed_params["descending"])
                await save_feed_position({
                    SERVER_ID_KEY: get_session_server_id(session),
                    offset_key: response["next_page"]["offset"]
                })
            # Stop crawling
            break

        # Check if we got new data
        if response["data"]:
            # Weeeee, got new data
            # Process it
            await data_handler(session, response["data"])
            # Save new position
            date_modified_key = get_date_modified_key(feed_params["descending"])
            offset_key = get_offset_key(feed_params["descending"])
            await save_feed_position({
                SERVER_ID_KEY: get_session_server_id(session),
                date_modified_key: response["data"][-1]["dateModified"],
                offset_key: response["next_page"]["offset"]
            })

        # Update feed params with new offset for next request
        feed_params.update(offset=response["next_page"]["offset"])

        # Less than API_LIMIT items received
        # That's mean we got all stuff from feed for now
        # Wait before next request to avoid flooding the server
        # and to give some time for new data to appear in feed
        if len(response["data"]) < API_LIMIT:
            await asyncio.sleep(NO_ITEMS_INTERVAL)

        # Wait before next request
        await asyncio.sleep(FEED_STEP_INTERVAL)

    # Left crawler loop
    # Crawler is done
    logger.info(
        "Crawler stopped",
        extra={
            "MESSAGE_ID": "CRAWLER_STOPPED",
            "FEED_URL": url,
            "FEED_PARAMS": feed_params,
        }
    )
