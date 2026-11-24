from pydantic import BaseModel

class CounterIn(BaseModel):
    value: int

class CounterOut(BaseModel):
    value: int

    class Config:
        orm_mode = True
