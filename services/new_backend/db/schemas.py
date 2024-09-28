import datetime
import json
from time import strftime

import pydantic

from services.new_backend.rabbit.ысруьфы import NNOutput


class TaskBase(pydantic.BaseModel):
    path_or_url: str
    creation_date: datetime.datetime = datetime.datetime.now()

class TaskCreate(TaskBase):
    pass

class Task(TaskBase):
    id: int

    class ConfigDict:
        from_attributes = True

class UploadBase(pydantic.BaseModel):
    path_or_url: str
    upload_date: datetime.datetime = datetime.datetime.now()
    webhook_url: str | None = None
    webhook_url_request_id: int | None = None
    task_answer: str | None = None

class UploadCreate(UploadBase):
    pass

class Upload(UploadBase):
    id: int

    class ConfigDict:
        from_attributes = True