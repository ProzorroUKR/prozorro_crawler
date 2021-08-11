import aiohttp
import asyncio
from json.decoder import JSONDecodeError
from prozorro_crawler.settings import (
    logger,
    GET_ERROR_RETRIES,
    CONNECTION_ERROR_INTERVAL,
    TOO_MANY_REQUESTS_INTERVAL,
)


async def process_resource(session, url, resource_id, process_function):
    resource_url = f"{url}/{resource_id}"
    data = await get_response_data(session, resource_url)
    return await process_function(session, data)


async def get_response_data(session, url, error_retries=GET_ERROR_RETRIES):
    while True:
        try:
            resp = await session.get(url)
        except aiohttp.ClientError as e:
            logger.warning(
                f"Error from {url} {type(e)}: {e}",
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
                else:
                    return response["data"]
            elif resp.status == 429:
                logger.warning(
                    "Too many requests while getting tender",
                    extra={"MESSAGE_ID": "TOO_MANY_REQUESTS"}
                )
                await asyncio.sleep(TOO_MANY_REQUESTS_INTERVAL)
            else:
                if error_retries > 1:
                    logger.warning(
                        "Error on getting tender: {} {}".format(
                            resp.status,
                            await resp.text()
                        ),
                        extra={"MESSAGE_ID": "REQUEST_UNEXPECTED_ERROR"}
                    )
                    error_retries -= 1
                    await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
                else:
                    return logger.error(
                        "Error on getting tender: {} {}".format(
                            resp.status,
                            await resp.text()
                        ),
                        extra={"MESSAGE_ID": "REQUEST_UNEXPECTED_ERROR"}
                    )
