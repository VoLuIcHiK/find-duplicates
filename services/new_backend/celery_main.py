import asyncio
import time
import random

from celery import Celery
from loguru import logger
from websockets.utils import accept_key

from .config import settings

celery = Celery(
    __name__,
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    broker_connection_retry_on_startup=True,
)

@celery.task(acks_late=True)
def send_notification(device_token: str):
    logger.info("starting background task")
    print("HI")
    time.sleep(10)  # simulates slow network call to firebase/sns
    try:
        if random.random() > 0.5:
            a=11/0
        logger.info(f"notification sent {device_token}")
    except Exception as e:
        logger.error(f"exception while division {e}")

@celery.task(acks_late=True)
def send_file(file: bytes):
    pass

def main():
    celery.worker_main('worker --loglevel=DEBUG -P gevent'.split(' '))