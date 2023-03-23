from typing import Any, Optional

from pydantic import BaseModel


class Request(BaseModel):
    id_: Any
    url: str
    data: Optional[dict] = {}


class Response(BaseModel):
    id_: Any
    status_code: int
    data: dict
