# Prozorro crawler

## Usage

Install
```bash
uv add git+https://github.com/ProzorroUKR/prozorro_crawler.git
```
OR
```bash
pip install git+https://github.com/ProzorroUKR/prozorro_crawler.git
```

Use it in your code to get all the changed objects from the feed
```python
from typing import Any
from aiohttp import ClientSession
from prozorro_crawler.main import main as run_crawler


async def item_data_handler(session: ClientSession, items: list[dict[str, Any]]) -> None:
    for item in items:
        print(f"Item with id {item['id']} and status {item['status']} got updated!")

if __name__ == "__main__":
    run_crawler(  # this will run IO loop and process feed updates
        data_handler=item_data_handler,
        opt_fields=["status"],
        resource="tenders",  # can be plans, contracts, frameworks, etc.
    )
```

If you need more object details than feed `opt_fields` provides, you can use `process_resource` callback

```python
from typing import Any
from aiohttp import ClientSession
from prozorro_crawler.main import main as run_crawler
from prozorro_crawler.resource import process_resource
from prozorro_crawler.utils import get_resource_url


RESOURCE = "tenders"
TENDERS_URL = get_resource_url(RESOURCE)


async def process_tender(session: ClientSession, tender: dict[str, Any]) -> None:
    print("Got full tender", tender)

    for n, bid in enumerate(tender.get("bids", []), start=1):
        print(f"Bid #{n}: {bid['value']} {bid['suppliers']}")

    # You also can do `session` to make more calls to API


async def item_data_handler(session: ClientSession, items: list[dict[str, Any]]) -> None:
    for item in items:
        print(f"Item with id {item['id']} and status {item['status']} got updated!")

        # process the tender
        await process_resource(session, url=TENDERS_URL, resource_id=item["id"], process_function=process_tender)

if __name__ == "__main__":
    run_crawler(  # this will run IO loop and process feed updates
        data_handler=item_data_handler,
        opt_fields=["status"],
        resource=RESOURCE,  # can be plans, contracts, frameworks, etc.
    )
```


## Development

###  Pre-commit

To install `pre-commit` simply run inside the shell:

```bash
uv run pre-commit install
uv run pre-commit install --hook-type commit-msg
```

### Run tests

```bash
uv run pytest tests -x -s -vvv
```
