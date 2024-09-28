import json

from loguru import logger
from pika.channel import Channel
from pika.connection import Connection


class RabbitPublisher:

    def __init__(
            self,
            rabbit_channel: Channel,
            rabbit_connection: Connection,
            rabbit_output_queue: str
    ):
        assert rabbit_channel.is_open, 'Failed connection to RabbitMQ'

        self.rabbit_connection = rabbit_connection
        self.rabbit_channel = rabbit_channel
        self.rabbit_output_queue = rabbit_output_queue

    def publish(self, body) -> None:
        body = json.dumps(body, ensure_ascii=False)
        self.rabbit_channel.basic_publish(body=body.encode('utf-8'), exchange='', routing_key=self.rabbit_output_queue)
        logger.info(f'Publish msg to {self.rabbit_output_queue}')