from typing import Any

from pydantic import BaseModel


class Request(BaseModel):
    id_: Any
    url: str
    data: dict


class Response(BaseModel):
    id_: Any
    status_code: int
    data: dict
