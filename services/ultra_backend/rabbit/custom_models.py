import pydantic


class RabbitPipelineOut(pydantic.BaseModel):
    video_link: str


class RabbitPipelineIn(pydantic.BaseModel):
    class InnerResult(pydantic.BaseModel):
        video_link: str
        is_duplicate: bool
        duplicate_for: str | None = None

    result: InnerResult


def __main():
    d = {
        "inputs": {
            "video_link": "https://s3.ritm.media/yappy-db-duplicates/dda04107-4a60-4335-a37a-c078ae1b7880.mp4"},
        "process_time": 0.8173038959503174,
        "current_time": "2024-09-28 13:31:22",
        "result": {
            "video_link": "https://s3.ritm.media/yappy-db-duplicates/dda04107-4a60-4335-a37a-c078ae1b7880.mp4",
            "is_duplicate": True
        }
    }
    model = RabbitPipelineIn.model_validate(d)
    print(model.model_dump())


if __name__ == '__main__':
    __main()
