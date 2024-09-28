from enum import Enum

import pydantic

from services.new_backend.rabbit.ысруьфы import NNOutput


class ResponseModel(pydantic.BaseModel):
    class Status(str, Enum):
        success = "success"
        error = "error"

    status: Status = Status.success
    data: dict = {}
    error: str = ""
    comment: str | None = None

    @property
    def dict(self, **kwargs) -> dict:
        return self.model_dump(warnings='none', exclude_none=True)


class ErrorResponse(ResponseModel):
    def __init__(self, error: str = None):
        super().__init__()
        self.status = ResponseModel.Status.error
        if error is not None:
            self.error = error


class WebhookRequestModel(ResponseModel):
    class WebhookDataModel(pydantic.BaseModel):
        upload_id: str
        webhook_url_request_id: str | None = None
        upload_url: str | None = None
        familiar_videos: list[str] | None = None

    data: WebhookDataModel | None = None

