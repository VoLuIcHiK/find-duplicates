import configparser
import os
# os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
# os.environ["CUDA_VISIBLE_DEVICES"] = "1"
import traceback
from collections import defaultdict
from pathlib import Path
from typing import *
import json
import tempfile
import sys
sys.path.append('adapter')

import albumentations as A
import numpy as np
from loguru import logger
from ml_utils import (MilvusWrapper, TritonWrapper, VideoDataloader)
from pymilvus import CollectionSchema, DataType, FieldSchema

from src.utils import (extract_spectrogram, filter_by_threshold, intersection,
                       is_drop_frame, seconds2timestamp, unite, business_logic, duplicates)
import requests

logger.add(f"{__file__.split('/')[-1].split('.')[0]}.log", rotation="50 MB")

class Model:
    
    def __init__(self, config: dict | None = None) -> None:
        
        self.milvus = MilvusWrapper(config=config)
        self.milvus.connect()
        self.milvus.init_collection(config['collection_name'], schema=self.create_schema())
        
        if len(self.milvus.collection.indexes) == 0:
            self.milvus.collection.create_index(
                field_name="features", 
                index_params={
                    "metric_type":"COSINE",
                    "index_type":"IVF_FLAT",
                    "params":{"nlist":128}
                    },
                index_name="qwer"
            )
        
        self.milvus.collection.load()
        
        self.timesformer = TritonWrapper(config=config, config_prefix='TIMESFORMER')
        
        self.transform = lambda x: A.Compose([
            A.SmallestMaxSize(224),
            A.CenterCrop(224, 224),
            A.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))
        ])(image=x)['image']
        
        self.threshold = float(config['threshold']) # порог близости видео
        self.seq_len = int(config['seq_len']) # количество кадров в 1 сегменте
        self.stride = int(config['stride'])
        self.mode = config['mode'] # Тип работы адаптера - вставка или сравнение
        
        self.videos_folder = Path(config['videos_folder'])
        self.pickles_folder = Path(config['pickles_folder'])
        
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
        
        position = FieldSchema(
            name="position",
            dtype=DataType.INT64
        )

        features = FieldSchema(
            name="features",
            dtype=DataType.FLOAT_VECTOR,
            dim=768
        )

        schema = CollectionSchema(
            fields=[vid, name, features, position],
            description=description,
            enable_dynamic_field=True
        )
        return schema
    
    
    def create_data_rows(self, features, video_id, start_id, position) -> list:
        """
        Метод для генерации строк для вставки в Milvus. Каждому эмбеддингу соответствует
        свой временной промежуток, поэтому к названию видео прибавляется добавка вида "_n",
        которая содержит видео с (n * 8) по (n * 8 + 8) секунд.  
        """
        data = [
            [video_id + f'_{i}' for i in range(start_id, start_id + len(features))], # videoID_clipNUM
            features,
            [position] * len(features)
        ]
        return data
    
    
    def audio_postprocess(self, video_id, video_path, data) -> Any | list:
        """
        Метод сравнения аудиодорожек. Извлекает спектрограммы необходимых кусочков 2-х видео,
        далее с помощью Ausil производится извлечение эмбеддингов и получается финальное значения сравнения.
        """
        pass
        return result
    
    
    def video_postprocess(self, data) -> list:
        """
        Метод, объединяющий несколько пересекающихся совпадающих отрезков сравнения в один большой.
        """
        logger.info('Start video processing')
        while True:
            segments = []
            video2index = defaultdict(set)
            for hit in data:
                union = False
                video_id = hit[1]
                
                for segment in video2index[video_id]:
                    if intersection(segments[segment][0], hit[0]) and intersection(segments[segment][2], hit[2]):
                        segments[segment][0] = unite(segments[segment][0], hit[0])
                        segments[segment][2] = unite(segments[segment][2], hit[2])
                        
                        next_count = segments[segment][-1][1] + 1
                        prev_count = segments[segment][-1][1]
                        segments[segment][-1] = ((segments[segment][-1][0] * prev_count + hit[-1][0]) / next_count, next_count)
                        union = True
                        
                
                if not union:
                    segments.append(hit)
                    video2index[video_id].add(len(segments) - 1)
            
            if len(segments) == len(data):
                break
            else:
                data = segments.copy()
        logger.info('End video processing')
        return segments
    
    
    def download_video(self, link):
        logger.info(f'Downloading {link}...')
        name = link.split('.')[-2].split('/')[-1]
        try:
            response = requests.get(link)
        except Exception as e:
            logger.error(f'Unable to download file {link}; error: {e}')
        
        filepath = tempfile.NamedTemporaryFile(delete=False, suffix='.mp4').name
        try:
            # filepath = self.videos_folder / f'{name}-new.mp4'
            with open(filepath, 'wb') as f:
                f.write(response.content)
        except Exception as e:
            logger.error(f'Error saving file {link}; error: {e}')
        
        logger.success(f'Downloaded {name}')
        return str(filepath)
    
    
    def __call__(self, video_link: str, **kwargs):
        """
        Метод для обработки входящего сообщения из очереди.

        Args:
            video_id (str): id видео на платформе или локальный путь

        Returns:
            dict: словарь ответа. В брокере будет переведен в json
        """
        try:
            if not 'http' in video_link:
                video_path = video_link
            else:
                video_path = self.download_video(video_link)

            
            if self.mode == 'test':
                insert_features = []
                dataloader = VideoDataloader(video_path, self.seq_len, self.timesformer.max_batch_size,
                                         stride=8, transforms=self.transform)
                dataloader.is_frame_drop = is_drop_frame(dataloader.frame_shape, video_link)
                
                for n, batch in enumerate(dataloader):
                    batch = batch.transpose(0, 1, 4, 2, 3)
                    last_hidden_state = self.timesformer(batch)[0]
                    feature = last_hidden_state[:, 0]
                    feature = feature / np.linalg.norm(feature, axis=-1, keepdims=True)
                    insert_features.append(feature)
                logger.success('Feature requests sucessed')
                
                indexes = []
                insert_features = np.concatenate(insert_features)
                indexes.extend(self.milvus.insert(self.create_data_rows(insert_features, video_link, 0)))
                logger.success(f'Inserting sucessful')
                
                result = {'indexes': list(map(str, indexes)), 'segments': len(indexes)}
            
            elif self.mode == 'similarity':
                similarity_data = []
                insert_features = []
                
                dataloader = VideoDataloader(video_path, self.seq_len, self.timesformer.max_batch_size,
                                         stride=8, transforms=self.transform)
                
                npy_path = Path(video_path).parent.parent / 'train_pickles' / f"{str(Path(video_path).stem)}.npy"
                # npy_path = Path(video_path).with_suffix('.npy')
                if os.path.exists(npy_path):
                    features = np.load(npy_path)
                    insert_features = features
                    similarity_data.extend(self.milvus.vector_search(features))
                    dataloader.skipped_frames = []
                    dataloader.appended_frames = [range(100)]
                else:
                    for n, batch in enumerate(dataloader):
                        batch = batch.transpose(0, 1, 4, 2, 3)
                        last_hidden_state = self.timesformer(batch)[0]
                        feature = last_hidden_state[:, 0]
                        feature = feature / np.linalg.norm(feature, axis=-1, keepdims=True)
                        similarity_data.extend(self.milvus.vector_search(feature))
                        insert_features.append(feature)
                logger.success('Feature requests sucessed')
                
                data = filter_by_threshold(
                    similarity_data,
                    self.threshold,
                    self.seq_len,
                    self.stride,
                    dataloader.skipped_frames,
                    dataloader.appended_frames
                )
                logger.info('Data has been filtered by threshold')
                segments = self.video_postprocess(data)
                # filtered_segments = self.audio_postprocess(video_id, video_path, segments)
                filtered_segments = segments
                filtered_segments = business_logic(filtered_segments)

                result = seconds2timestamp(filtered_segments)
                result.pop(Path(video_path).stem, None)
                
                is_duplicate, is_hard, duplicate_for = duplicates(result)
                
                if not is_duplicate:
                    # insert_features = np.concatenate(insert_features)
                    # logger.error(insert_features.shape)
                    self.milvus.insert(self.create_data_rows(insert_features, video_link, 0, 123))
                    logger.success(f'Inserting sucessful')
                    
                logger.info('Segments have been converted to timestamps')
                result = {
                    'video_link': video_link,
                    'is_duplicate': is_duplicate,
                    'is_hard': is_hard,
                    'duplicate_for': duplicate_for
                }
                return result
            
            elif self.mode == 'save':
                features = []
                dataloader = VideoDataloader(video_path, self.seq_len, self.timesformer.max_batch_size,
                                         stride=8, transforms=self.transform)
                
                for n, batch in enumerate(dataloader):
                    batch = batch.transpose(0, 1, 4, 2, 3)
                    last_hidden_state = self.timesformer(batch)[0]
                    feature = last_hidden_state[:, 0]
                    feature = feature / np.linalg.norm(feature, axis=-1, keepdims=True)
                    features.append(feature)
                logger.success('Feature requests sucessed')
                
                features = np.concatenate(features)
                name = Path(video_path).stem
                filepath =  self.pickles_folder / f'{name}.npy'
                np.save(filepath, features)
                logger.success(f'Save sucessful')
                
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
    # obj = path.format('dda04107-4a60-4335-a37a-c078ae1b7880')
    # result = model(obj)
    # print(time.time() - t1)
    # print(result)