import pydantic

# from loguru import logger


class BaseModel(pydantic.BaseModel):
    class Config:
        arbitrary_types_allowed = True
        orm_mode = True

    # @classmethod
    # def logger(cls) -> Logger:
    #     return logger
