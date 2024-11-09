import aiohttp
import asyncio
import json
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


async def get_response_data(session, url, json_loads=json.loads, error_retries=GET_ERROR_RETRIES):
    while True:
        try:
            resp = await session.get(url)
        except aiohttp.ClientError as e:
            logger.warning(
                f"Error from {url} {type(e)}: {e}",
                extra={"MESSAGE_ID": "HTTP_EXCEPTION"}
            )
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
            continue
        
        if resp.status == 429:
            logger.warning(
                "Too many requests while getting tender",
                extra={"MESSAGE_ID": "TOO_MANY_REQUESTS"}
            )
            await asyncio.sleep(TOO_MANY_REQUESTS_INTERVAL)
            continue

        elif resp.status != 200:
            error_message = "Error on getting tender: {} {}".format(
                resp.status, 
                await resp.text()
            )
            
            if error_retries > 1:
                logger.warning(
                    error_message,
                    extra={"MESSAGE_ID": "REQUEST_UNEXPECTED_ERROR"}
                )
                error_retries -= 1
                await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
                continue
            
            logger.error(
                error_message,
                extra={"MESSAGE_ID": "REQUEST_UNEXPECTED_ERROR"}
            )
            break

        try:
            response = await resp.json(loads=json_loads)
        except (aiohttp.ClientPayloadError, JSONDecodeError) as e:
            logger.warning(e, extra={"MESSAGE_ID": "HTTP_EXCEPTION"})
            await asyncio.sleep(CONNECTION_ERROR_INTERVAL)
            continue

        return response["data"]
