import asyncio
import datetime
import json
import os
import pathlib
import subprocess
from dataclasses import dataclass

import dotenv
import fastapi
import fastapi.responses
import pydantic
import requests
import uvicorn
import yaml
from aiohttp.web_fileresponse import FileResponse
from fastapi import FastAPI, Request, UploadFile, File, Body, Depends
from loguru import logger
from sqlalchemy.orm import Session

from services.new_backend.database import database_folder
from services.new_backend.rabbit.ысруьфы import NNOutput
from . import rabbit_worker
from .config import settings
from .db import crud, models
from .db.database import SessionLocal, engine
from .db.schemas import TaskCreate, UploadCreate, Upload
from .flow_utils import FlowController
from .models import ResponseModel, WebhookRequestModel
from .rabbit_worker import RabbitConsumerThread
from .rabbit.connector import compact_publish_data

base_folder = pathlib.Path(__file__).parent
TEMP_FOLDER = base_folder / 'temp'
BACKEND_OUTPUT_QUEUE: str | None = None
RABBIT_URL: str | None = None
app = FastAPI(title=settings.PROJECT_NAME, version=settings.PROJECT_VERSION)

rabbit_consumer_thread: RabbitConsumerThread | None = None
@app.get("/")
@logger.catch(reraise=False, default=fastapi.responses.Response(status_code=500))
async def root(request: Request):
    resp = fastapi.responses.RedirectResponse(url=f"{request.base_url}docs",
                                              status_code=fastapi.status.HTTP_303_SEE_OTHER)
    # raise Exception("=)")
    return resp

from .celery_main import send_notification

@app.get("/push/{device_token}")
@logger.catch(reraise=True)
async def notify(device_token: str):
    logger.info("sending notification in background")
    send_notification.delay(device_token)
    return {"message": "Notification sent"}

@dataclass
class RabbitConnectionInfo:
    def __init__(self, rabbit_url, queue, rabbit_consumer):
        self.rabbit_url = rabbit_url
        self.queue = queue
        self.rabbit_consumer_thread: RabbitConsumerThread = rabbit_consumer

def get_rabbit():
    yield RabbitConnectionInfo(RABBIT_URL, BACKEND_OUTPUT_QUEUE, RabbitConsumerThread(RABBIT_URL, BACKEND_OUTPUT_QUEUE))

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def upload_to_file(folder, upload: UploadFile) -> str | None:
    if upload is None:
        return None
    name = upload.filename
    date_str = datetime.datetime.now().strftime('%Y%m%d_%H%M')
    filename = pathlib.Path(name)
    tmp_filename = f"{filename.name.removesuffix(filename.suffix)}_{date_str}{filename.suffix}"
    tmp_filepath = folder / tmp_filename
    if not folder.exists():
        logger.info(f"\"Folder {folder} is not exists. Creating new in \"{folder.absolute()}\"\"")
        folder.mkdir()
    with open(tmp_filepath, "wb") as f:
        f.write(upload.file.read())
    url_or_path = str(tmp_filepath)
    return url_or_path

def send_model_to_webhook(data: ResponseModel, webhook: str):
    logger.info(f"Sending {data} to webhook {webhook}")
    requests.post(webhook, json=data.dict)
@app.post('/upload', status_code=201, response_model=ResponseModel)
async def upload_file(request: Request, file: UploadFile= File(None),
                      url: str | None = Body(None, embed=True),
                      path: str | None = Body(None, embed=True),
                      webhook_url: str | None = Body(None, embed=True),
                      webhook_request_id: str | None = Body(None, embed=True),
                      db: Session = Depends(get_db),
                      rabbit: RabbitConnectionInfo = Depends(get_rabbit)):
    answer = ResponseModel(status=ResponseModel.Status.error)
    if not any([file, url, path]):
        answer.error = "No file, url or path provided"
        return fastapi.responses.JSONResponse(json.dumps(answer.dict, ensure_ascii=False), status_code=fastapi.status.HTTP_400_BAD_REQUEST)
    if not webhook_url:
        answer.comment = "No webhook_url provided\n"
    url_or_path = url
    if file:
        url_or_path = upload_to_file(TEMP_FOLDER, file)
    elif path:
        url_or_path = path
    task_create = TaskCreate(path_or_url=str(url_or_path))
    task = crud.create_task(db, task_create)
    db.commit()
    upload = crud.create_upload(db, UploadCreate(path_or_url=str(url_or_path)))
    db.commit()
    rabbit_task = rabbit_worker.RabbitTask(task_id=task.id)
    rabbit_task.in_args = {
        'task_id': str(task.id),
        'task': 'all',
        'file_path': task.path_or_url,
    }

    @logger.catch(reraise=True)
    def _callback(result_task: rabbit_worker.RabbitTask):
        inner_result = result_task.result
        description = inner_result.get('description', str())
        familiar_images: list[str] = [str(x) for x in inner_result.get('familiar_videos', list())]
        data = {
            'familiar_videos': familiar_images,
            'description': description,
        }
        try:
            task_answer = NNOutput.ResultDict.model_validate(data)
        except pydantic.ValidationError as e:
            logger.exception(f"Error while validating data: {e} with data {data}")
            return
        with db.begin():
            upload.task_answer = json.dumps(task_answer.model_dump(), ensure_ascii=False)
            db.add(upload)
        data_public: dict | Upload = to_public(upload, str(request.base_url))
        res_model = WebhookRequestModel()
        res_model_data = WebhookRequestModel.WebhookDataModel(upload_id=str(upload.id))
        task_answer_public = NNOutput.ResultDict.model_validate(json.loads(data_public.get('task_answer')))
        res_model_data.familiar_videos = task_answer_public.familiar_videos
        url_base = str(request.base_url)
        res_model_data.upload_url = f"{url_base}upload/{upload.id}/"
        res_model_data.webhook_url_request_id = webhook_request_id
        res_model.data = res_model_data
        if webhook_url is not None:
            send_model_to_webhook(res_model, webhook_url)
    rabbit_task.callback = _callback
    compact_publish_data(rabbit.rabbit_url, rabbit.queue, rabbit_task.in_args)
    rabbit.rabbit_consumer_thread.add_task(rabbit_task)
    answer.status = ResponseModel.Status.success
    return answer


@app.get('/upload/{upload_id}/', response_model=ResponseModel)
async def get_upload(request: Request, upload_id: str, db: Session = Depends(get_db)):
    answer = ResponseModel(status=ResponseModel.Status.error)
    upload = crud.get_upload(db, int(upload_id))
    if not upload:
        answer.error = f"Upload with id {upload_id} not found"
        return fastapi.responses.JSONResponse(answer, status_code=fastapi.status.HTTP_404_NOT_FOUND)

    try:
        answer.data = to_public(upload, str(request.base_url))
    except Exception as e:
        answer.error = f"Error while processing upload {upload.id}: {e}"
        return fastapi.responses.JSONResponse(answer.dict, status_code=fastapi.status.HTTP_500_INTERNAL_SERVER_ERROR)
    answer.status = ResponseModel.Status.success
    return fastapi.responses.JSONResponse(answer.dict, status_code=fastapi.status.HTTP_200_OK)


@app.get('/upload/{upload_id}/result/{result_num}')
async def get_upload(request: Request, upload_id: str, result_num: int, db: Session = Depends(get_db)):
    answer = ResponseModel(status=ResponseModel.Status.error)
    upload = crud.get_upload(db, int(upload_id))
    if not upload:
        answer.error = f"Upload with id {upload_id} not found"
        return fastapi.responses.JSONResponse(answer, status_code=fastapi.status.HTTP_404_NOT_FOUND)
    task_answer = upload.task_answer
    if task_answer is None:
        answer.error = f"Task answer for upload {upload_id} not found"
        return fastapi.responses.JSONResponse(answer.dict, status_code=fastapi.status.HTTP_404_NOT_FOUND)
    task_answer_dict = json.loads(task_answer)
    result_dict = rabbit_worker.NNOutput.ResultDict.from_dict(task_answer_dict)
    familiar_videos = result_dict.familiar_videos
    if len(familiar_videos) <= result_num or result_num < 1:
        answer.error = f"Task found but result_num={result_num} not found"
        return fastapi.responses.JSONResponse(answer.dict, status_code=fastapi.status.HTTP_404_NOT_FOUND)
    file_path_or_url = familiar_videos[result_num-1]
    if file_path_or_url.startswith('http'):
        return fastapi.responses.RedirectResponse(url=file_path_or_url)
    filepath = pathlib.Path(file_path_or_url)
    if not filepath.exists():
        answer.error = f"File {file_path_or_url} not found"
        return fastapi.responses.JSONResponse(answer.dict, status_code=fastapi.status.HTTP_404_NOT_FOUND)
    # send file
    return fastapi.responses.FileResponse(filepath, filename=filepath.name)

def to_public(upload: Upload, url: str) -> dict:
    task_answer = upload.task_answer
    if task_answer is not None:
        task_answer_dict = json.loads(task_answer)
        result_dict = NNOutput.ResultDict.model_validate(task_answer_dict, from_attributes=True)
        result_dict.familiar_videos = [f"{url}upload/{upload.id}/result/{i+1}" for i, x in enumerate(result_dict.familiar_videos)]
        task_answer = json.dumps(result_dict.model_dump(), ensure_ascii=False)
    return {
        'id': upload.id,
        'upload_date': upload.upload_date.strftime('%Y-%m-%d %H:%M:%S'),
        'webhook_url': upload.webhook_url,
        'webhook_url_request_id': upload.webhook_url_request_id,
        'task_answer': task_answer
    }

@app.get('/uploads', response_model=ResponseModel)
async def get_uploads(request: Request, db: Session = Depends(get_db)):
    answer = ResponseModel(status=ResponseModel.Status.error)
    uploads = crud.get_uploads(db)
    if not uploads:
        answer.error = "No uploads found"
        return fastapi.responses.JSONResponse(answer, status_code=fastapi.status.HTTP_404_NOT_FOUND)
    answer.status = ResponseModel.Status.success
    upload_dicts = []
    for upload in uploads:
        try:
            upload_dicts.append(to_public(upload, str(request.base_url)))
        except Exception as e:
            answer.error = f"Error while processing upload {upload.id}: {e}"
            return fastapi.responses.JSONResponse(answer, status_code=fastapi.status.HTTP_500_INTERNAL_SERVER_ERROR)
    answer.data = upload_dicts
    return fastapi.responses.JSONResponse(answer.dict, status_code=fastapi.status.HTTP_200_OK)

def fastapi_thread():
    port = 8050
    logger.info(f"Access web interface: http://127.0.0.1:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port)

def celery_process():
    os.chdir(base_folder.parent)
    subprocess.run(f'celery -A {base_folder.name}.celery_main.celery worker -P gevent'.split(' '))

def rabbit_thread():
    global rabbit_consumer_thread
    rabbit_url = os.getenv('RABBIT_URL', None)
    if rabbit_url is None:
        logger.error("Rabbit URL is not set. Exiting")
        return
    input_queue = os.getenv('BACKEND_INPUT_QUEUE', None)
    if input_queue is None:
        logger.error("Input queue is not set. Exiting")
        return
    global BACKEND_OUTPUT_QUEUE
    rabbit_consumer_thread = RabbitConsumerThread(rabbit_url, input_queue)
    rabbit_consumer_thread.run()

def load_rabbit_config():
    CONFIG_FOLDER_NAME = 'configs'
    configs_folder = pathlib.Path(__file__).parent / CONFIG_FOLDER_NAME
    levels = 3
    for level in range(levels):
        if os.path.exists(configs_folder):
            break
        configs_folder = configs_folder.parent.parent / CONFIG_FOLDER_NAME
    if not os.path.exists(configs_folder):
        logger.warning(f"Config folder \"{CONFIG_FOLDER_NAME}\" in {levels} top folders not found. "
                       f"Using system ENV variables")
    config = yaml.safe_load(pathlib.Path(configs_folder / 'config.yaml').read_text())
    for key, value in config.get('rabbit', dict()).items():
        if key in os.environ:
            continue
        os.environ[key] = str(value)
    global BACKEND_OUTPUT_QUEUE
    BACKEND_OUTPUT_QUEUE = os.getenv('BACKEND_OUTPUT_QUEUE', None)
    if BACKEND_OUTPUT_QUEUE is None:
        logger.error("BACKEND_OUTPUT_QUEUE is not set. Exiting")
        return
    global RABBIT_URL
    RABBIT_URL = os.getenv('RABBIT_URL', None)
    if RABBIT_URL is None:
        logger.error("RABBIT_URL is not set. Exiting")
        return

async def main():
    models.Base.metadata.create_all(bind=engine)
    env_file = pathlib.Path(__file__).parent / '.env'
    if env_file.exists():
        dotenv.load_dotenv(env_file)
    load_rabbit_config()
    flow_controller = FlowController(max_fail_count=1)
    flow_controller.add_process(target=celery_process, name="Celery")
    flow_controller.add_thread(target=fastapi_thread, name="FastAPI")
    flow_controller.add_thread(target=rabbit_thread, name="RabbitWorker")
    flow_controller.run_infinite()
    # uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    asyncio.run(main())