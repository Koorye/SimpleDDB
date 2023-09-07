import json

from pydantic import BaseModel


class Entry(BaseModel):
    index: int
    node: int
    role: str
    msg: str = ''
    command: str = ''
    data: dict = dict()
