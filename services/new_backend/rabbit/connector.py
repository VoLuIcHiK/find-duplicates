import time
from typing import Any

import pika
from loguru import logger
from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.channel import Channel
from pika.connection import Connection

from .publisher import RabbitPublisher


def compact_publish_data(rabbit_url: str, output_queue: str, data: dict):
    con = Connector(rabbit_url)
    with con as (connection, channel):
        connection: Connection
        channel: Channel
        con.create_queue(channel, output_queue)
        pub = RabbitPublisher(
            channel,
            connection,
            output_queue
        )
        pub.publish(data)

def compact_get_data(rabbit_url: str, input_queue) -> bytes:
    con = Connector(rabbit_url)
    with con as (connection, channel):
        connection: Connection
        channel: BlockingChannel
        channel.basic_qos(prefetch_count=1)
        x = channel.basic_get(input_queue)
        if x != (None, None, None):
            channel.basic_ack(x[0].delivery_tag)
            return x[2]


class Connector:
    def __init__(self, rabbit_url: str) -> None:
        self.port = None
        self._connection: Connection | None = None
        self.virtual_host = None
        self.username = None
        self.password = None
        self.ip = None
        self.url = rabbit_url
        self.__parse_url()

    def __parse_url(self):
        if self.url is None:
            raise ValueError('url is not set')

        protocol = self.url.split('//')[0].rstrip(':')
        without_protocol = self.url.split('//')[-1]
        if '@' in self.url:
            before_host = without_protocol.split('@')[0]
            self.username = before_host.split(':')[0]
            if len(before_host.split(':')) > 1:
                self.password = before_host.split(':')[1]
        after_username = without_protocol.split('@')[-1]
        host_with_port = after_username.split('/')[0]
        self.ip = host_with_port.split(':')[0]
        if len(host_with_port.split(':')) > 1:
            try:
                self.port = int(host_with_port.split(':')[1])
            except Exception as _:
                raise ValueError(f'Port is not a number: {host_with_port.split(":")[1]}')
        self.virtual_host = ''
        if len(after_username.split('/')) > 1:
            self.virtual_host = after_username.split('/')[1]

        logger.info(f"IP:{self.ip}, HOST:{self.virtual_host}, PORT:{self.port}, VIRT_HOST:{self.virtual_host}")
        logger.info(f"USER:{self.username}, PASS:{self.password}")

        if len(self.virtual_host) == 0:
            self.virtual_host = '/'

        logger.info('Envs are parsed')


    def __enter__(self):
        """
        :return: connection, channel, input_queue, output_queue
        """
        return self._connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection.is_open:
            self._connection.close()

    @staticmethod
    def create_queue(channel, queue_name):
        channel.queue_declare(
            queue=queue_name,
            durable=True,
            exclusive=False,  # если очередь уже существует,
            auto_delete=False,
            arguments={'x-queue-type=classic': 'classic'}
        )
        logger.info(f'Queue {queue_name} has been added')

    def _connect(self) -> tuple[BlockingConnection, BlockingChannel]:
        tries = 0
        while True:
            try:
                tries += 1
                logger.info(f'Trying to connect at {tries} time')
                connection = pika.BlockingConnection(
                    [
                        pika.ConnectionParameters(
                            host=self.ip,
                            port=self.port,
                            virtual_host=self.virtual_host,
                            credentials=pika.PlainCredentials(
                                username=self.username if self.username is not None
                                else pika.ConnectionParameters.DEFAULT_USERNAME,
                                password=self.password if self.password is not None
                                else pika.ConnectionParameters.DEFAULT_PASSWORD
                            ),
                        )
                    ],

                )
                self._connection = connection
                channel = connection.channel()
                logger.info('Connection successful')

                return connection, channel
            except Exception as e:
                logger.exception(e)
                logger.info(f'Connection failed. Waiting for a 5 seconds...')
                time.sleep(5)

