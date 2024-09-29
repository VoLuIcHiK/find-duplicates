import configparser
import os
import sys
import tempfile
from pathlib import Path
from typing import *

sys.path.append('adapter')

import albumentations as A
import numpy as np
import requests
from audio_fingerprint.shazam import compare_fingerprints, fingerprint_file
from loguru import logger
from ml_utils import MilvusWrapper, TritonWrapper, VideoDataloader
from pymilvus import CollectionSchema, DataType, FieldSchema
from src.utils import duplicates, filter_by_threshold
import pickle

logger.add(f"{__file__.split('/')[-1].split('.')[0]}.log", rotation="50 MB")

class Model:
    
    def __init__(self, config: dict | None = None) -> None:
        
        self.milvus = MilvusWrapper(config=config)
        self.milvus.connect()
        self.milvus.init_collection(
            config['collection_name'],
            schema=self.create_schema()
        )
        
        if len(self.milvus.collection.indexes) == 0:
            self.milvus.collection.create_index(
                field_name = "features", 
                index_params = {
                        "metric_type": "COSINE",
                        "index_type": "IVF_FLAT",
                        "params": {"nlist": 128}
                    },
                index_name="qwer"
            )
        self.milvus.collection.load()
        
        self.audio_store = {}
        
        
        self.timesformer = TritonWrapper(config=config, config_prefix='TIMESFORMER')
        self.transform = lambda x: A.Compose([
            A.SmallestMaxSize(224),
            A.CenterCrop(224, 224),
            A.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))
        ])(image=x)['image']
        
        self.video_threshold = float(config['video_threshold']) # порог близости видео
        self.audio_threshold = float(config['audio_threshold']) # порог близости аудио
        
        self.mode = config['mode'] # Тип работы адаптера - сравнение и вставка или сохранение фичей
        
        self.videos_folder = Path(config['videos_folder'])
        self.pickles_folder = Path(config['pickles_folder'])
        audio_store_path = Path(config['audio_store'])
        
        if os.path.exists(audio_store_path):
            self.audio_store = pickle.load(open(audio_store_path, 'rb'))
        logger.info(f'Len of audio {len(self.audio_store)}')
        
        os.makedirs(self.pickles_folder, exist_ok=True)
    
    def create_schema(self, description='Piracy features') -> CollectionSchema:
        """
        Метод создания схемы для коллекции в Milvus. 
        Необходим в случае, если коллекция отсутствует.
        """
        vid = FieldSchema(
            name="id",
            dtype=DataType.INT64,
            is_primary=True,
            auto_id=True
        )

        name = FieldSchema(
            name="video_id",
            dtype=DataType.VARCHAR,
            max_length=200,
            default_value="Unknown"
        )

        features = FieldSchema(
            name="features",
            dtype=DataType.FLOAT_VECTOR,
            dim=768
        )

        schema = CollectionSchema(
            fields=[vid, name, features],
            description=description,
            enable_dynamic_field=True
        )
        return schema
    
    
    def create_data_rows(self, features, video_id) -> list:
        """
        Метод для генерации строк для вставки в Milvus.
        """
        data = [
            [video_id for i in range(len(features))],
            features
        ]
        return data

    
    def download_video(self, link) -> str:
        """
        Метод для скачивания видео с s3 или любой другой ссылки. 
        Результат записывается во временные файл, возвращается путь до скачанного видео.

        Args:
            link (str): Ссылка на скачивание файла

        Returns:
            str: Путь к скачанному файлу
        """
        logger.info(f'Downloading {link}...')
        try:
            response = requests.get(link)
        except Exception as e:
            logger.error(f'Unable to download file {link}; error: {e}')
            raise e
        
        filepath = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4').name
        try:
            with open(filepath, 'wb') as f:
                f.write(response.content)
        except Exception as e:
            logger.error(f'Error saving file {link}; error: {e}')
            raise e
        
        logger.success(f'Downloaded {link}')
        return str(filepath)
    
    
    def get_audio_scores(self, candidate_video_scores: dict, video_path: str | Path) -> tuple[dict, list | tuple[()]]:
        """
        Метод для получения схожести аудиодорожек после видеосравнения.
        Если в видеодорожке нет аудио - помечаем его как 2.0 для дальнейшей проверки.

        Args:
            candidate_video_scores (dict): _description_
            video_path (str | Path): _description_

        Returns:
            tuple[dict, list | tuple[()]]: Словарь, аналогичный candidate_video_scores, 
                и закодированные точки в алгоритм сравнения аудио.
        """
        candidate_audio_scores = {}
        try:
            query_fingerprint = fingerprint_file(video_path)
        except:
            query_fingerprint = ()
        
        for vid_id, _ in candidate_video_scores.items():
            candidat_fingerprint = self.audio_store[vid_id] # фичи кандидата
            if len(candidat_fingerprint) == 0 or len(query_fingerprint) == 0:
                candidate_audio_scores[vid_id] = 2.0
                continue
            score = compare_fingerprints(
                query_fingerprint, 
                candidat_fingerprint
            )
            candidate_audio_scores[vid_id] = score
        
        return candidate_audio_scores, query_fingerprint
    
    
    def __call__(self, video_link: str, **kwargs):
        """
        Метод для обработки входящего сообщения из очереди.

        Args:
            video_link (str): id видео на платформе или локальный путь

        Returns:
            dict: словарь ответа. В брокере будет переведен в json
        """
        try:
            if not 'http' in video_link:
                video_path = video_link
                video_id = str(Path(video_path).stem)
                
            else:
                video_path = self.download_video(video_link)
                video_id = video_link.split('/')[-1].split('.')[0]

            logger.info(f'Start procesing {video_id}')
            if self.mode == 'similarity':
                similarity_data = []
                insert_features = []
                
                dataloader = VideoDataloader(video_path, transforms=self.transform)
                
                #! Это нужно для более быстрого локального запуска
                npy_path = self.pickles_folder / f"{video_id}.npy"
                if os.path.exists(npy_path):
                    features = np.load(npy_path)
                    insert_features = features
                    similarity_data.extend(self.milvus.vector_search(features))
                    logger.success('Feature loaded sucessed')
                else:
                    #! Основная часть с подгрузкой видео на лету
                    for n, batch in enumerate(dataloader):
                        batch = batch[None].transpose(0, 1, 4, 2, 3)
                        last_hidden_state = self.timesformer(batch)[0]
                        feature = last_hidden_state[:, 0]
                        feature = feature / np.linalg.norm(feature, axis=-1, keepdims=True)
                        similarity_data.extend(self.milvus.vector_search(feature))
                        insert_features.append(feature)
                        break
                    insert_features = np.concatenate(insert_features)
                    logger.success('Feature requests sucessed')
                
                candidate_video_scores = filter_by_threshold(
                    similarity_data,
                    self.video_threshold
                )

                candidate_audio_scores, query_fingerprint = self.get_audio_scores(
                    candidate_video_scores,
                    video_path
                )
                
                is_duplicate, is_hard, duplicate_for = duplicates(
                    candidate_video_scores, 
                    candidate_audio_scores,
                    self.video_threshold,
                    self.audio_threshold
                )
                
                logger.info(f'Is duplicate - {is_duplicate}')
                
                if not is_duplicate:
                    self.audio_store[video_id] = query_fingerprint
                    self.milvus.insert(
                        self.create_data_rows(
                            insert_features,
                            video_id
                    ))
                    logger.success(f'Inserting sucessful')
                    
                
                result = {
                    'video_link': video_link,
                    'is_duplicate': is_duplicate,
                    'is_hard': is_hard,
                    'duplicate_for': duplicate_for
                }
                return result
            
            elif self.mode == 'save':
                features = []
                
                dataloader = VideoDataloader(
                    video_path, 
                    transforms=self.transform
                )
                
                for n, batch in enumerate(dataloader):
                    batch = batch[None].transpose(0, 1, 4, 2, 3)
                    last_hidden_state = self.timesformer(batch)[0]
                    feature = last_hidden_state[:, 0]
                    feature = feature / np.linalg.norm(feature, axis=-1, keepdims=True)
                    features.append(feature)
                    break
                features = np.concatenate(features)
                logger.success('Feature requests sucessed')
                
                filepath =  self.pickles_folder / f'{video_id}.npy'
                np.save(filepath, features)
                logger.success(f'Save {video_id} sucessful')
                
                result = {'path': str(filepath)}

        except Exception as e:
            if os.path.exists(video_path):
                # os.remove(video_path)
                logger.info(f'Video {video_path} has been deleted')
            raise e
        
        if os.path.exists(video_path):
            # os.remove(video_path)
            logger.info(f'Video {video_path} has been deleted')

        return result


if __name__ == '__main__':
    config = configparser.ConfigParser()
    # config.read('/home/borntowarn/projects/borntowarn/find-duplicates/configs/resources.ini')
    # config = config['adapter_local']
    config.read(f'configs/resources.ini')
    config = config['adapter_docker']
    
    model = Model(config=config)
    
    broker = config['broker']
    match broker:
        case 'rabbit':
            from ml_utils import RabbitWrapper
            broker = RabbitWrapper(config=config)
        case 'kafka':
            from ml_utils import KafkaWrapper
            broker = KafkaWrapper(
                config=config,
                consumer_kwargs={"max_poll_interval_ms": 500 * 3600 * 1000, "enable_auto_commit": False}
            )
    
    broker.listen(pipeline=model)
    
    # Локальный запуск
    # import time
    # t1 = time.time()
    # path = '/home/borntowarn/projects/borntowarn/train_data_yappy/train_dataset/{}.mp4'
    # obj = path.format('992321a1-06b2-4f2c-a2d1-aa7f37a14da1')
    # result = model(obj)
    # print(time.time() - t1)
    # print(result)