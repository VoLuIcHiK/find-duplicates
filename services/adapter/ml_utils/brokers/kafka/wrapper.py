import configparser
import json
import os
import traceback
from typing import Union, Optional, Any, Callable, List, Dict

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.structs import TopicPartition

from ... import logger
from ..base import BaseWrapper
from .answer_template import KafkaAnswer


class KafkaWrapper(BaseWrapper):
    """
    НАХОДИТСЯ В БЕТА РЕЖИМЕ
    
    Класс для подключения к Kafka и подключения/создания очередей.
    Позволяет подключаться или очереди коллекции, отправлять или принимать данные 
    и осуществлять отправку в пайплайн.
    
    Для установки конфига через системный переменные:
    1. export KAFKA_URL=
    2. export OUTPUT_TOPIC=
    3. export INPUT_TOPIC=
    4. export GROUP_ID=
    5. export NUM_REPLICAS=
    """

    def __init__(
        self,
        config_path: str = None,
        service_name: str = None,
        config: Dict = {},
        bootstrap_servers: Optional[str] = None,
        input_topic: Optional[str] = None,
        input_partitions: Optional[List[int]] = None,
        output_topic: Optional[str] = None,
        output_partition: Optional[int] = None,
        swap_topics: bool = False,
        num_replicas: Optional[int] = None,
        consumer_kwargs: Dict = {},
        publisher_kwargs: Dict = {},
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
            bootstrap_servers (str, optional): URL кафки. Defaults to None.
            input_topic (str, optional): Входная очередь. Defaults to None.
            input_partitions (List[int], optional): Входные партиции для подписки. Defaults to None.
            output_topic (str, optional): Выходная очередь. Defaults to None.
            output_partitions (int, optional): Партиция для отправки сообщений. Defaults to None.
            swap_topics (bool, optional): Нужно ли поменять местами топики.
            Это бывает удобно для подгрузки конфига и прослушивания выходов модели:
            ```python3
                rabbit = RabbitWrapper('../configs/resources.insert.ini', 'service', swap_topics=True)
                output_msgs = rabbit.listen()
            ```
            Defaults to False.
            num_replicas (int, optional): Количество реплик для создания очереди. Defaults to False.
            consumer_kwargs (Dict): Дополнительные аргументы для KafkaConsumer (например group_id).
            publisher_kwargs (Dict): Дополнительные аргументы для KafkaPublisher.
        """
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.num_replicas = num_replicas
        self.config = config
        self.consumer_kwargs = consumer_kwargs
        self.publisher_kwargs = publisher_kwargs
        self.output_partition = output_partition
        
        if config_path and service_name and os.path.exists(config_path):
            self.config = configparser.ConfigParser()
            self.config.read(config_path)
            self.config = self.config[service_name]
        self._load_config()
        
        if swap_topics:
            self.input_topic, self.output_topic = self.output_topic, self.input_topic
        
        if self.input_topic:
            self._create_topic(self.input_topic)
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=json.loads,
                **self.consumer_kwargs
            )
            if input_partitions and isinstance(input_partitions, list):
                self.consumer.assign([TopicPartition(self.input_topic, i) for i in input_partitions])
            else:
                self.consumer.subscribe([self.input_topic])
            logger.info(f'Input topic {self.input_topic} has been connected')
        else:
            self.consumer = None
        
        if self.output_topic:
            self._create_topic(self.output_topic)
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers, 
                value_serializer=lambda x: x.encode('utf-8'),
                **self.publisher_kwargs
            )
            logger.info(f'Output topic {self.output_topic} has been connected')
        else:
            self.producer = None

    
    def _load_config(self):
        if not self.bootstrap_servers:
            self.bootstrap_servers = self.config.get('KAFKA_URL', os.environ.get('KAFKA_URL', None))
            
        if not self.input_topic:
            self.input_topic = self.config.get('INPUT_TOPIC', os.environ.get('INPUT_TOPIC', None)) 
            
        if not self.output_topic:
            self.output_topic = self.config.get('OUTPUT_TOPIC', os.environ.get('OUTPUT_TOPIC', None))
        
        if not self.num_replicas:
            self.num_replicas = int(self.config.get('NUM_REPLICAS', os.environ.get('NUM_REPLICAS', 1)))
        
        self.consumer_kwargs['group_id'] = self.consumer_kwargs.get('group_id', self.config.get('GROUP_ID', os.environ.get('GROUP_ID', None)))
        self.consumer_kwargs['auto_offset_reset'] = self.consumer_kwargs.get('auto_offset_reset', 'latest')

        logger.info('Config has been loaded')
    
    
    def _create_answer(self, time, payload: dict, result: Optional[dict]) -> None:
        return KafkaAnswer(time, payload, result)
    
    
    def _create_topic(self, name):
        admin_client = KafkaAdminClient(
            bootstrap_servers=self.bootstrap_servers
        )
        if name not in admin_client.list_topics():
            try:
                topic = NewTopic(name=name, num_partitions=self.num_replicas, replication_factor=1)
                admin_client.create_topics(new_topics=[topic], validate_only=False)
                logger.success(f'Topic {name} has been created')
            except TopicAlreadyExistsError as e:
                logger.warning(f'Topic {name} already exists')

    
    def publish(self, data: Union[list[dict], dict], time: Optional[float] = None, payload: Optional[dict] = None) -> None:
        """
        Функция для подключения к выходной очереди, в которую нужно отправлять сообщения.

        Args:
            data (Union[list[dict], dict]): Данные для отправки в очередь.
            time (float, optional): Время обработки приходит из listen. Defaults to None.
            payload (dict, optional): Payload приходит из listen. Defaults to None.
        """
        assert self.producer, 'There is producer needed'
        
        if not isinstance(data, list):
            data = [data]
        
        for item in data:
            if payload:
                answer = self._create_answer(time, payload, item).json
            else:
                answer = json.dumps(item)
            self.producer.send(self.output_topic, answer, partition=self.output_partition)
            self.producer.flush()
            logger.debug(f'Publish msg to {self.output_topic}')
    
    
    def listen(self, num = -1, pipeline: Optional[Callable] = None, consumer_timeout_ms: float = float('inf')) -> None:
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
                # message == '{'video_id': '19936b03156acfd10a9b0cfd63c6a89b'}'
                payload = msg.value
                # payload == {'video_id': '19936b03156acfd10a9b0cfd63c6a89b'}
                pipeline(**payload)
            ```
            
            Defaults to None.
            consumer_timeout_ms (float, optional): Через какое время после отсутствия сообщений
                прерывать консьюмера. Defaults to float('inf').
        """
        assert self.consumer, 'There is consumer needed'
        
        if pipeline:
            logger.info(f'Consumer gets pipeline: {pipeline.__class__.__name__}')
        
        self.consumer.config['consumer_timeout_ms'] = consumer_timeout_ms
        
        payloads = []
        logger.info(f'Start consuming on {self.input_topic}')
        for n, msg in enumerate(self.consumer):
            try:
                if self.consumer_kwargs['group_id']:
                    self.consumer.commit()
                logger.debug(f'Got and commited message')
                payload = msg.value
                
                if pipeline:
                    result, time = self._process_item(pipeline, **payload)
                    if self.producer:
                        self.publish(result, time, payload)
                else:
                    payloads.append(payload)    
                    if n + 1 == num:
                        return payloads
            except:
                logger.error(f'{traceback.format_exc()}')
        
        return payloads
