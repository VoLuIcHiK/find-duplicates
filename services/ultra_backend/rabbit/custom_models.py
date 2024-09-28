import pydantic


class RabbitPipelineOut(pydantic.BaseModel):
    video_link: str


class RabbitPipelineIn(pydantic.BaseModel):
    video_link: str
    is_duplicate: bool
    duplicate_for: str | None
