import configparser
import os
from typing import Optional, Any

import tritonclient.grpc as grpcclient
import tritonclient.http as httpclient

from .. import logger


class TritonWrapper:
    """
    Класс для подключения к Triton и отправки/обработки запросов.
    
    Для установки конфига через системный переменные:
    1. export TRITON_URL=
    2. export TRITON_CONNECT_TYPE=
    3. export TRITON_VERBOSE=
    4. export TRITON_NAME=
    5. export TRITON_VERSION=
    6. export TRITON_INPUT_NAMES=
    7. export TRITON_MAX_BATCH_SIZE=
    8. export TRITON_INPUT_DTYPES=
    9. export TRITON_OUTPUT_NAMES=
    """
    
    def __init__(
        self,
        config_path: str = None,
        service_name: str = None,
        config: dict = {},
        config_prefix: str = '',
        url: Optional[str] = None,
        connect_type: Optional[str] = None,
        verbose: bool = False,
        model_name: Optional[str] = None,
        model_version: Optional[str] = None,
        max_batch_size: Optional[int] = None,
        input_names: Optional[list[str]] = None,
        input_dtypes: Optional[list[str]] = None,
        output_names: Optional[list[str]] = None,
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
            config_prefix (str, optional): Префикс названия модели в конфиге. Если в конфиге
            содержится несколько моделей, то параметры модели будут читаться с добавкой prefix.
            ```
            config_prefix = 'RESNET'
            # Тогда получаем, что конфиг модели будет выглядеть вот так:
            TRITON_RESNET_NAME = triton_model_name
            TRITON_RESNET_VERSION = 1
            TRITON_RESNET_INPUT_NAMES = pixel_values
            TRITON_RESNET_MAX_BATCH_SIZE = 8
            TRITON_RESNET_INPUT_DTYPES = FP32
            TRITON_RESNET_OUTPUT_NAMES = last_hidden_state
            ```
            
            Defaults to ''.
            url (str, optional): Сервер тритона. Defaults to None.
            connect_type (str, optional): grpc | http. Defaults to None.
            verbose (bool, optional): Выводить ли инфу о инференсе. Defaults to False.
            model_name (str, optional): Наименование модели в тритоне. Defaults to None.
            model_version (str, optional): Версия модели. Defaults to None.
            max_batch_size (int, optional): Максимальный батч модели в тритоне. Defaults to None.
            input_names (list[str], optional): Имена входов в модель. Defaults to None.
            input_dtypes (list[str], optional): Типы входов в модель. Defaults to None.
            output_names (list[str], optional): Имена выходов модели. Defaults to None.
        """
        self.url = url
        self.connect_type = connect_type
        self.verbose = verbose
        self.model_name = model_name
        self.model_version = model_version
        self.max_batch_size = max_batch_size
        self.input_names = input_names
        self.input_dtypes = input_dtypes
        self.output_names = output_names
        self.config_prefix = '_' + config_prefix if config_prefix != '' else config_prefix
        self.config = config
        
        if config_path and service_name and os.path.exists(config_path):
            self.config = configparser.ConfigParser()
            self.config.read(config_path)
            self.config = self.config[service_name]
        self._load_config()
        
        assert self.connect_type in ['grpc', 'http'], "Connect type must be 'grpc' or 'http'"
        self._init_client()
    
    
    def _load_config(self) -> None:
        if not self.url:
            self.url = self.config.get('TRITON_URL', os.environ.get('TRITON_URL', None))
        if not self.connect_type:
            self.connect_type = self.config.get('TRITON_CONNECT_TYPE', os.environ.get('TRITON_CONNECT_TYPE', None))
        if not self.verbose:
            self.verbose = self.config.getboolean('TRITON_VERBOSE', os.environ.get('TRITON_VERBOSE', False))
        
        
        if not self.model_name:
            self.model_name = self.config.get(
                f'TRITON{self.config_prefix}_NAME',
                os.environ.get(f'TRITON{self.config_prefix}_NAME', None)
            )
        if not self.model_version:
            self.model_version = self.config.get(
                f'TRITON{self.config_prefix}_VERSION',
                os.environ.get(f'TRITON{self.config_prefix}_VERSION', '-1')
            )
        if not self.max_batch_size:
            self.max_batch_size = int(self.config.get(
                f'TRITON{self.config_prefix}_MAX_BATCH_SIZE',
                os.environ.get(f'TRITON{self.config_prefix}_MAX_BATCH_SIZE', None)
            ))
        if not self.input_dtypes:
            self.input_dtypes = self.config.get(
                f'TRITON{self.config_prefix}_INPUT_DTYPES',
                os.environ.get(f'TRITON{self.config_prefix}_INPUT_DTYPES', None)
            ).split(',')
        if not self.input_names:
            self.input_names = self.config.get(
                f'TRITON{self.config_prefix}_INPUT_NAMES',
                os.environ.get(f'TRITON{self.config_prefix}_INPUT_NAMES', None)
            ).split(',')
        if not self.output_names:
            self.output_names = self.config.get(
                f'TRITON{self.config_prefix}_OUTPUT_NAMES',
                os.environ.get(f'TRITON{self.config_prefix}_OUTPUT_NAMES', None)
            ).split(',')

        logger.info('Config has been loaded')
    
    
    def _init_client(self) -> Any:
        if self.connect_type == 'grpc':
            self.client_type = grpcclient
        elif self.connect_type == 'http':
            self.client_type = httpclient
        
        self.client = self.client_type.InferenceServerClient(url=self.url, verbose=self.verbose)
        logger.info('Client has been initialized')
    
    
    def _postprocess(self, results) -> list:
        return [results.as_numpy(out_name) for out_name in self.output_names]
    
    
    def _preprocess(self, *data) -> list:
        inputs = []
        for i, input_name, dtype in zip(
            range(len(data)),
            self.input_names,
            self.input_dtypes
        ):
            inputs.append(self.client_type.InferInput(input_name, [*data[i].shape], dtype))
            inputs[i].set_data_from_numpy(data[i])
        return inputs
    
    
    def _forward(self, *inputs) -> Any:
        result = self.client.infer(
            self.model_name,
            model_version=self.model_version,
            inputs=inputs
        )
        return result
    
    
    def __call__(self, *data) -> list:
        """
        Вызов всех элементов пайплайна в порядке:
        1. Препроцессинг (установка входов)
        2. Отправка запроса и получение результата
        1. Постпроцессинг (установка выходов)

        Returns:
            list: Выходы модели в заданной конфигом последовательности
        """
        inputs = self._preprocess(*data)
        results = self._forward(*inputs)
        outputs = self._postprocess(results)
        return outputs