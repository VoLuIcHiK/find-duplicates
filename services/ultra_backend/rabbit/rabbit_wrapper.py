import time

from ..rabbit.connector import compact_publish_data, compact_get_data


class RabbitWrapper:
    def __init__(self, rabbit_url: str):
        self.rabbit_url = rabbit_url

    def send_message(self, queue: str, message: str):
        compact_publish_data(self.rabbit_url, queue, message)

    def get_message(self, queue: str, block=False, timeout=0) -> str | None:
        while True:
            message = compact_get_data(self.rabbit_url, queue, block=block)
            if not message and block:
                time.sleep(0.01)
                continue
            return message


class RabbitPipelineUnit:
    def __init__(self, rabbit_url: str, input_queue: str | None = None, output_queue: str | None = None):
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.rabbit_url = rabbit_url

    def get_message(self, block=False, timeout=0) -> str | None:
        if self.input_queue is None:
            raise ValueError('input_queue is not set')
        return RabbitWrapper(self.rabbit_url).get_message(self.input_queue, block, timeout)

    def send_message(self, message: str):
        if self.output_queue is None:
            raise ValueError('output_queue is not set')
        RabbitWrapper(self.rabbit_url).send_message(self.output_queue, message)

