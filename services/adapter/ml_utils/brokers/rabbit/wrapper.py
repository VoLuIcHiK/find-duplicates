import configparser
import json
import os
import sys
import time
import traceback
from typing import Union, Optional, Any, Callable

import amqp

from ... import logger
from ..base import BaseWrapper
from .answer_template import RabbitAnswer


class RabbitWrapper(BaseWrapper):
    """
    Класс для подключения к Rabbit и подключения/создания очередей.
    Позволяет подключаться или очереди коллекции, отправлять или принимать данные 
    и осуществлять отправку в пайплайн.
    
    Для установки конфига через системный переменные:
    1. export RABBIT_URL=
    2. export OUTPUT_TOPIC=
    3. export INPUT_TOPIC=
    """
    
    def __init__(
        self,
        config_path: str = None,
        service_name: str = None,
        config: dict = {},
        url: Optional[str] = None,
        input_topic: Optional[str] = None,
        output_topic: Optional[str] = None,
        swap_topics: bool = False
    ) -> None:
        """
        Инициализировать конфигурации можно 3 способами 
        (указаны в порядке важности, верхние уровни перетирают значения нижних):

        1. Аргументами инициализации класса
        2. ini файлом конфигурации с указанием наименования сервиса
        3. Через системные переменные (os.env)
        
        Args:
            config_path (str, optional): Путь до ini файла. Defaults to None.
            service_name (str, optional): Наименование сервиса в ini. Defaults to None.
            config (dict, optional): Загруженный конфиг в виде словаря.
                Инициализация config_path + service_name эквивалентна config. Defaults to {}.
            url (str, optional): URL AMQP Rabbit с кредами для авторизации.
            ```python
                url = "amqp://user:pass@localhost:5672/"
            ```
            Defaults to None.
            input_topic (str, optional): Входная очередь. Defaults to None.
            output_topic (str, optional): Выходная очередь. Defaults to None.
            swap_topics (bool, optional): Нужно ли поменять местами топики.
            Это бывает удобно для подгрузки конфига и прослушивания выходов модели:
            ```python3
                rabbit = RabbitWrapper('../configs/resources.insert.ini', 'service', swap_topics=True)
                output_msgs = rabbit.listen()
            ```
            Defaults to False.
        """
        self.url = url
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.config = config
        
        if config_path and service_name and os.path.exists(config_path):
            self.config = configparser.ConfigParser()
            self.config.read(config_path)
            self.config = self.config[service_name]
        self._load_config()
        
        if swap_topics:
            self.input_topic, self.output_topic = self.output_topic, self.input_topic
        
        self._connect()
        
        if self.input_topic:
            self._create_topic(self.input_topic)
            logger.info(f'Input topic {self.input_topic} has been connected')
        
        if self.output_topic:
            self._create_topic(self.output_topic)
            logger.info(f'Output topic {self.output_topic} has been connected')
    
    
    def _load_config(self):
        if not self.url:
            self.url = self.config.get('RABBIT_URL', os.environ.get('RABBIT_URL', None))
            
        self.host, self.virtual_host = self.url.split('@')[1].split('/')
        _, self.username, self.password = self.url.split('@')[0].split(':')
        self.username = self.username.replace('//', '')
        
        if len(self.virtual_host) == 0:
            self.virtual_host = '/'
        
        if not self.input_topic:
            self.input_topic = self.config.get('INPUT_TOPIC', os.environ.get('INPUT_TOPIC', None)) 
        
        if not self.output_topic:    
            self.output_topic = self.config.get('OUTPUT_TOPIC', os.environ.get('OUTPUT_TOPIC', None))

        logger.info('Config has been loaded')
    
    
    def _create_topic(self, topic_name):
        self.channel.queue_declare(
            queue=topic_name,
            durable=True,
            exclusive=False,  # если очередь уже существует,
            auto_delete=False,
            arguments={'x-queue-type=classic': 'classic'}
        )
        logger.debug(f'Topic {topic_name} has been created')
    
    
    def _create_answer(self, time, payload: dict, result: Optional[dict]) -> None:
        return RabbitAnswer(time, payload, result)
    
    
    def _connect(self) -> Any:
        tries = 0
        while True:
            try:
                tries += 1
                logger.info(f'Trying to connect at {tries} time')
                self.connection = amqp.Connection(
                    host=self.host,
                    userid=self.username,
                    password=self.password,
                    virtual_host=self.virtual_host
                )

                self.connection.connect()
                self.channel = self.connection.channel()
                logger.info('Connection successful')
                break
            except Exception as e:
                logger.warning(f'Connection failed. Waiting for a 5 seconds...')
                time.sleep(5)

    
    def publish(self, data: Union[list[dict], dict], time: float = None, payload: dict = None) -> None:
        """
        Функция для подключения к выходной очереди, в которую нужно отправлять сообщения.

        Args:
            data (Union[list[dict], dict]): Данные для отправки в очередь.
            time (float, optional): Время обработки приходит из listen. Defaults to None.
            payload (dict, optional): Payload приходит из listen. Defaults to None.
        """
        assert self.output_topic, 'There is output topic needed'
        
        if not isinstance(data, list):
            data = [data]
        
        for item in data:
            if payload:
                answer = self._create_answer(time, payload, item).json
            else:
                answer = json.dumps(item)
            msg = amqp.basic_message.Message(body=answer)
            self.channel.basic_publish(msg, exchange='', routing_key=self.output_topic)
            logger.debug(f'Publish msg to {self.output_topic}')
    
    
    def listen(self, num = -1, pipeline: Optional[Callable] = None, ack: bool = False) -> None:
        """
        Функция для подключения ко входной очереди, которую нужно слушать.
        Если указан pipeline, то при получении сообщения, его body будет отправляться
        в callable pipeline.

        Args:
            num (int, optional): Сколько сообщений хотим прослушать Если -1, слушаем бесконечно.
                Defaults to -1.
            pipeline (Callable, optional): Функция, обрабатывающая данные в сообщении.
            Именованные аргументы body сообщения подаются в функцию без изменений:
            ```python
                message = self.channel.basic_get(queue=self.input_topic)
                # message.body == '{'video_id': '19936b03156acfd10a9b0cfd63c6a89b'}'
                payload = json.loads(message.body)
                # payload == {'video_id': '19936b03156acfd10a9b0cfd63c6a89b'}
                pipeline(**payload)
            ```
            
            Defaults to None.
            ack (bool, optional): Нужно ли отвечать на сообщения и удалять их из очереди.
                Если инициализирован pipeline, то ack = True. Defaults to False.
        """
        assert self.input_topic, 'There is input topic needed'
        
        if pipeline:
            ack = True
            logger.info(f'Consumer gets pipeline: {pipeline.__class__.__name__}')
        
        n = 0
        payloads = []
        logger.info(f'Start consuming on {self.input_topic}')
        while True:
            try:
                message = self.channel.basic_get(queue=self.input_topic)
                if message:
                    logger.debug(f'Got message')
                    if ack:
                        self.channel.basic_ack(delivery_tag=message.delivery_tag)
                        logger.debug(f'Acked on message')
                    payload = json.loads(message.body)
                    
                    if pipeline:
                        result, time = self._process_item(pipeline, **payload)
                        if self.output_topic:
                            self.publish(result, time, payload)
                    else:
                        payloads.append(payload)    
                        if n + 1 == num:
                            return payloads
                    n += 1
                elif not pipeline: 
                    return payloads
            except Exception as e:
                if e == KeyboardInterrupt:
                    self.connection.close()
                    sys.exit()
                logger.error(f'{traceback.format_exc()}')