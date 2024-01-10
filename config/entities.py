import uuid
from typing import Dict

from pydantic import BaseModel


class RequestUnbagQuery(BaseModel):
    id: uuid.UUID
    operation_type: str
    files: list[str]
    output_path: str
    params: dict

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            uuid.UUID: lambda x: str(x)
        }


class RequestQuery(BaseModel):
    id: uuid.UUID
    operation_type: str
    file: str
    output_path: str
    params: Dict[str, str]

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            uuid.UUID: lambda x: str(x)
        }


class ResponseQuery(BaseModel):
    id: uuid.UUID
    operation_type: str
    file: str

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            uuid.UUID: lambda x: str(x)
        }
