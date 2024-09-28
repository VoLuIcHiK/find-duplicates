import asyncio
import json
import os
import pathlib
import random
import threading
import time
from typing import Callable

import dotenv
import uvicorn
from fastapi import FastAPI, Body, HTTPException, Depends
from loguru import logger

from .flow_utils import FlowController
from .generated_models.video_link_request import VideoLinkRequest
from .generated_models.video_link_response import VideoLinkResponse
from .rabbit import connector
from .rabbit.custom_models import RabbitPipelineOut, RabbitPipelineIn
from .rabbit.rabbit_service import RabbitConsumerThread, RabbitTask
from .rabbit.rabbit_wrapper import RabbitPipelineUnit, RabbitWrapper

app = FastAPI(
    title="Video Duplicate Checker API",
    description="No description provided (generated by Openapi Generator "
                "https://github.com/openapitools/openapi-generator)",
    version="1.0.0", )

rabbit_url: str | None = None
input_queue: str | None = None
output_queue: str | None = None
rabbit_consumer: RabbitConsumerThread | None = None


def get_rabbit() -> RabbitPipelineUnit:
    r = RabbitPipelineUnit(rabbit_url, input_queue, output_queue)
    return r


def get_rabbit_consumer() -> RabbitConsumerThread:
    return rabbit_consumer


@app.post(
    "/check-video-duplicate",
    responses={
        200: {"model": VideoLinkResponse, "description": "Результат проверки"},
        400: {"description": "Неверный запрос"},
        500: {"description": "Ошибка сервера"},
    },
    tags=["API для проверки дубликатов видео"],
    summary="Проверка видео на дублирование",
    response_model_by_alias=True,
)
def check_video_duplicate_post(
        video_link: VideoLinkRequest = Body(None, description=""),
        rabbit: RabbitPipelineUnit = Depends(get_rabbit),
        rabbit_cons: RabbitConsumerThread = Depends(get_rabbit_consumer),
) -> VideoLinkResponse:
    rabbit_out = RabbitPipelineOut(video_link=video_link.link)
    rabbit.send_message(json.dumps(rabbit_out.model_dump()))
    ready_event = threading.Event()
    result_task: RabbitTask | None = None

    def set_ready(r: RabbitTask):
        nonlocal result_task
        result_task = r
        ready_event.set()

    rabbit_task = RabbitTask(task_id=video_link.link, in_args=rabbit_out.model_dump(), callback=set_ready)
    rabbit_cons.add_task(rabbit_task)
    if not ready_event.wait(20):
        raise HTTPException(status_code=500, detail="Timeout")
    nn_output = RabbitPipelineIn.model_validate(result_task.result)
    logger.info(json.dumps(nn_output.model_dump(), ensure_ascii=False))
    return VideoLinkResponse(
        is_duplicate=nn_output.is_duplicate,
        duplicate_for=nn_output.duplicate_for
    )


def load_rabbit():
    global rabbit_url
    rabbit_url = os.environ.get('RABBIT_URL')
    if rabbit_url is None:
        raise ValueError("RABBIT_URL is not set")
    global input_queue
    input_queue = os.environ.get('RABBIT_INPUT_QUEUE')
    if input_queue is None:
        raise ValueError("RABBIT_INPUT_QUEUE is not set")
    global output_queue
    output_queue = os.environ.get('RABBIT_OUTPUT_QUEUE')
    if output_queue is None:
        raise ValueError("RABBIT_OUTPUT_QUEUE is not set")


def rabbit_thread():
    global rabbit_consumer
    rabbit_consumer = RabbitConsumerThread(rabbit_url, input_queue)
    rabbit_consumer.run()




def main():
    env_file = pathlib.Path(__file__).parent / '.env'
    if env_file.exists():
        dotenv.load_dotenv(env_file)
    load_rabbit()
    f = FlowController(max_fail_count=3)
    f.add_thread(rabbit_thread, name="Rabbit")
    # f.add_thread(fast_answer_thread, name="FastAnswer") # TEST ONLY!!!
    port = 8054
    logger.info(f'Web access: http://127.0.0.1:{port}/docs')
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
