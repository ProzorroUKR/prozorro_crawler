from typing import Self, Any
from unittest.mock import MagicMock


class AsyncMock(MagicMock):
    async def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return super(AsyncMock, self).__call__(*args, **kwargs)

    async def __aexit__(self, *_: Any) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self
