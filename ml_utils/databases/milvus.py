import configparser
import os
from typing import Optional, Union

from pandas import DataFrame
from pymilvus import Collection, CollectionSchema, connections, utility

from .. import logger


class MilvusWrapper:
    """
    Класс для подключения к Milvus и операций с даннами.
    Позволяет инициализировать или создавать коллекции, вставлять данные и осуществлять поиск.
    
    Для установки конфига через системный переменные:
    1. export MILVUS_HOST=
    2. export MILVUS_PORT=
    3. export MILVUS_ALIAS=
    4. export MILVUS_USER=
    5. export MILVUS_PASS=
    """
    
    def __init__(
        self,
        config_path: str = None,
        service_name: str = None,
        config: dict = {},
        host: Optional[str] = None,
        port: Optional[Union[str, int]] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        alias: Optional[str] = None
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
            host (str, optional): Хост (адрес без https/http) Milvus. Defaults to None.
            port (Union[str, int], optional): Порт подключения. Defaults to None.
            user (str, optional): Пользователь для авторизации. Defaults to None.
            password (str, optional): Пароль пользователя. Defaults to None.
            alias (str, optional): Короткое наименование текущего подключения к базе. Defaults to None.
        """
        self.host = host
        self.port = port
        self.alias = alias
        self.user = user
        self.password = password
        self.config = config
        
        if config_path and service_name and os.path.exists(config_path):
            self.config = configparser.ConfigParser()
            self.config.read(config_path)
            self.config = self.config[service_name]
        self._load_config()
    
    
    def _load_config(self):
        if not self.host:
            self.host = self.config.get('MILVUS_HOST', os.environ.get('MILVUS_HOST', None))
        if not self.port:
            self.port = int(self.config.get('MILVUS_PORT', os.environ.get('MILVUS_PORT', None)))
        if not self.alias:
            self.alias = self.config.get('MILVUS_ALIAS', os.environ.get('MILVUS_ALIAS', None))
        if not self.user:
            self.user = self.config.get('MILVUS_USER', os.environ.get('MILVUS_USER', None)) 
        if not self.password:
            self.password = self.config.get('MILVUS_PASS', os.environ.get('MILVUS_PASS', None)) 

        logger.info('Config has been loaded')
    
    
    def connect(self):
        secure = False
        if int(self.port) == 443:
            secure = True
        
        connections.connect(
            alias=self.alias, 
            host=self.host, 
            port=self.port,
            secure=secure,
            user=self.user,
            password=self.password
        )
        logger.info('Milvus has been connected')
    
    
    def init_collection(
        self,
        collection_name: str,
        schema: Optional[CollectionSchema] = None,
        num_shards: int = 2,
        alias: str = 'default'
    ) -> None:
        """
        Функция для инициализации или подключения к коллекции. Если коллекция `collection_name`
        уже существует в Milvus то произойдет подключение, иначе будет запрошена схема `schema`
        для инициализации новой коллекции
        
        Args:
            collection_name (str): Наименование коллекции.
            schema (CollectionSchema, optional): Схема коллекции. В ней указываются все поля коллекции,
                размеры полей, типы данных. Defaults to None.
            num_shards (int, optional): На сколько частей делить коллекицю. Каждая часть увеличивает
                пропускную способность вставки, так как они рассчитаны на работу в параллели. Defaults to 2.
            alias (str, optional): Дополнительное название (короткое имя коллекции по которому можно подключаться).
                Если 'default', то alias не присвоится. Defaults to 'default'.
        """
        if utility.has_collection(collection_name):
            self.collection = Collection(
                name=collection_name,
            )
            logger.info(f'Collection {collection_name} has been connected')
        else:
            assert schema, f"""Collection {collection_name} doesn't exist.
                               You need to pass schema to create new collection"""

            self.collection = Collection(
                name=collection_name,
                schema=schema,
                using=alias,
                num_shards=num_shards
            )
            logger.info(f'New collection {collection_name} has been added')
    
    
    def insert(
        self,
        data: Union[list, DataFrame, dict], 
        partition_name: Optional[str] = None,
        timeout: Optional[float] = None
    ) -> Optional[list[int]]:
        """Функция вставки векторов в коллекцию

        Args:
            data (Union[list, DataFrame, dict]): Данные для вставки в коллекцию.
            
            Могут быть в виде списка списков в той же последовательности параметров коллеккции:
            ```python
                data = [
                    [str(i) for i in range(2000)],
                    [[random.random() for _ in range(100)] for _ in range(2000)]
                ]
                milvus_wrapper.insert(data)
            ```
                
            В виде словаря параметров единичного вектора:
            ```python
                data = {
                    'video_name': str(i),
                    'features': [random.random() for _ in range(100)]
                }
                milvus_wrapper.insert(data)
            ```
                
            Или в виде DataFrame:
            ```python
                data = {
                    'video_name': [str(i) for i in range(2000)],
                    'features': [[random.random() for _ in range(100)] for _ in range(2000)]
                }
                data = pd.DataFrame.from_dict(data)
                milvus_wrapper.insert(data)
            ```
                
            partition_name (str, optional): Партиция для вставки. Defaults to None.
            timeout (float, optional): Тайминг ожидания ответа от сервера. Если None
                то ожидаем, пока не получим ответ. Defaults to None.

        Returns:
            Optional[list[int]]: Список primary_keys.
        
        """
        try:
            res = self.collection.insert(
                data,
                partition_name=partition_name,
                timeout=timeout
            )
            if res.err_count > 0:
                logger.error(f'Errors: {res.err_count}')
            return list(res.primary_keys)
        except Exception as e:
            logger.exception(e)
    
    
    def vector_search(
        self, 
        features, 
        output_fields=['video_id'],
        anns_field='features',
        nprobe=32,
        limit=10,
        metric_type="COSINE"
    ) -> list[list[dict]]:
        """
        Функция для поиска ближайших векторов к запросу в коллекции - векторный поиск.

        Args:
            features (_type_): Вектора, для которых хотим найти ближайшие
            output_fields (list, optional): Поля, которые хотим получить для ближайших векторов.
                Defaults to ['video_id'].
            anns_field (str, optional): Наименование поля вектора, по которому производим поиск.
                Defaults to 'features'.
            nprobe (int, optional): Количество просматриваемых кластеров (ближайших центроид) при поиске.
                Defaults to 32.
            limit (int, optional): Количество ближайших векторов, которое будет найдено для каждого
                вектора в запросе. Defaults to 10.
            metric_type (str, optional): Метрика близости в запросе. Должна совпадать с метрикой в индексе.
                Defaults to "COSINE".

        Returns:
            list[list[dict]]: Список совпадений для каждого запроса. Размер - [n_query, limit].
        """
        search_params = {
            "metric_type": metric_type,
            "params": {"nprobe": nprobe}
        }

        results = self.collection.search(
            data=features, 
            param=search_params,
            limit=limit,
            anns_field=anns_field,
            output_fields=output_fields,
            consistency_level="Bounded"
        )
        
        parsed_results = []
        for hits in results:
            result =  []
            for hit in hits:
                result.append(hit.to_dict())
            parsed_results.append(result)
        return parsed_results
            