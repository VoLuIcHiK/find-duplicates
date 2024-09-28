import contextlib

import contextlib
import json
import threading
from dataclasses import dataclass
from threading import Thread
from typing import Callable, Union

from loguru import logger
from pika.adapters.blocking_connection import BlockingConnection, BlockingChannel

from ..rabbit.connector import Connector
from ..rabbit.custom_models import RabbitPipelineIn


@dataclass
class RabbitTask:
    link: str
    callback: Union[Callable, None]
    in_args: dict
    result: dict

    def __init__(self,  task_id, in_args=None, callback=None, result=None):
        if task_id is None:
            raise ValueError("task_id is required")
        self.task_id = str(task_id)
        if in_args is None:
            in_args = {}
        self.in_args = dict(in_args)
        self.callback = callback
        if result is None:
            result = {}
        self.result = dict(result)


class RabbitConsumerThread(Thread):
    def __init__(self, rabbit_url: str, input_queue: str, daemon=True):
        super().__init__(daemon=daemon)
        self._rabbit_url = rabbit_url
        self._input_queue = input_queue
        self._lock = threading.Lock()
        self._tasks: dict[str, RabbitTask] = {}
        self._new_item_event = threading.Event()
        self._stop_flag_event = threading.Event()

    def add_task(self, rabbit_task: RabbitTask):
        with self._lock:
            self._tasks[rabbit_task.task_id] = rabbit_task
        self._new_item_event.set()

    @property
    def tasks(self) -> dict[str, RabbitTask]:
        return self._tasks.copy()

    @contextlib.contextmanager
    def run_in_thread(self):
        try:
            self.start()
            yield self
        finally:
            self.stop()
            self.join(1)


    def stop(self):
        self._stop_flag_event.set()

    @logger.catch(reraise=True)
    def run(self) -> None:
        con = Connector(rabbit_url=self._rabbit_url)
        while self._stop_flag_event.is_set() is False:
            try:
                with con as (connection, channel):
                    connection: BlockingConnection
                    channel: BlockingChannel
                    con.create_queue(channel, self._input_queue)
                    while self._stop_flag_event.is_set() is False:
                        no_items = False
                        with self._lock:
                            if len(self._tasks) <= 0:
                                no_items = True
                        channel.basic_qos(prefetch_count=1)
                        x = channel.basic_get(self._input_queue)
                        if x != (None, None, None):
                            task = self.filter_call(*x)
                            channel.basic_ack(x[0].delivery_tag)
                        if no_items:
                            logger.info("NO ITEMS")
                            self._new_item_event.wait()
                            self._new_item_event.clear()
                            logger.info("UNSET")
            except Exception as e:
                logger.error(e)
                logger.info("RECONNECT")

    def filter_call(self, ch, method, body):
        """
        Вызов функции обратного взаимодействия, передав сообщение из очереди
        :param ch: Канал
        :param method: Имя метода отправки
        :param body: Тело сообщения
        """
        task: RabbitTask | None = None
        try:
            logger.info("CALLBACK")
            logger.info(ch)
            logger.info(method)
            logger.info(body)
            body_str = body.decode('utf-8')
            body_json = json.loads(body_str)
            rabbit_in: RabbitPipelineIn = RabbitPipelineIn.model_validate(body_json)
            nn_output = rabbit_in.result
            logger.info(f"Parsed task NNOutput: {nn_output.model_dump()}")
            task_id = nn_output.video_link
            failed = False

            with self._lock:
                task = self._tasks.get(task_id)
                if task is None:
                    logger.warning(f"Task {task_id} not found. Skip.")
                    return
                task.result = nn_output.model_dump()

            callback = task.callback

            # process_status = str(body_json['status'])
            # if process_status.lower() == 'success':
            # ready_file_path = body_json['path']
            try:
                callback(task)
            except Exception as e:
                logger.exception(e)
            return task
        except Exception as e:
            logger.exception(e)
        finally:
            if task is not None:
                with self._lock:
                    self._tasks.pop(task.task_id)
                    logger.info(f"Task {task.task_id} done")
