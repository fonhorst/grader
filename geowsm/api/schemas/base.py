from pydantic import Field
from pydantic.main import BaseModel


class User(BaseModel):
    uid: str = Field(..., description="Unique identifier of the user")
    name: str = Field(..., description="Name of the user")
