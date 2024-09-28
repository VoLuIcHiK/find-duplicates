import configparser
import os
# os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
# os.environ["CUDA_VISIBLE_DEVICES"] = "1"
import traceback
from collections import defaultdict
from pathlib import Path
from typing import *
import json

import albumentations as A
import numpy as np
from loguru import logger
from ml_utils import (MilvusWrapper, TritonWrapper, VideoDataloader)
from pymilvus import CollectionSchema, DataType, FieldSchema

from src.utils import (extract_spectrogram, filter_by_threshold, intersection,
                       is_drop_frame, seconds2timestamp, unite, business_logic)
import requests

logger.add(f"{__file__.split('/')[-1].split('.')[0]}.log", rotation="50 MB")

class Model:
    
    def __init__(self, config: dict | None = None) -> None:
        
        self.milvus = MilvusWrapper(config=config)
        self.milvus.connect()
        self.milvus.init_collection(config['collection_name'], schema=self.create_schema())
        
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
    
    
    def create_data_rows(self, features, video_id, start_id) -> list:
        """
        Метод для генерации строк для вставки в Milvus. Каждому эмбеддингу соответствует
        свой временной промежуток, поэтому к названию видео прибавляется добавка вида "_n",
        которая содержит видео с (n * 8) по (n * 8 + 8) секунд.  
        """
        data = [
            [video_id + f'_{i}' for i in range(start_id, start_id + len(features))], # videoID_clipNUM
            features
        ]
        return data
    
    
    def audio_postprocess(self, video_id, video_path, data) -> Any | list:
        """
        Метод сравнения аудиодорожек. Извлекает спектрограммы необходимых кусочков 2-х видео,
        далее с помощью Ausil производится извлечение эмбеддингов и получается финальное значения сравнения.
        """
        if len(data) == 0:
            return data
        
        logger.info('Start audio processing')
        
        result = []
        for n, ((cur_start, cur_stop), comp_id, (comp_start, comp_stop)) in enumerate(data):
            try:
                curr_spec = extract_spectrogram(
                    video_path,
                    offset=cur_start,
                    duration=cur_stop-cur_start
                )
                logger.success(f'Query spectrogram has been extracted')
            except Exception as e:
                logger.error(f'Cant process input audio. Return input data...')
                logger.error(f'{traceback.format_exc()}')
                return data
        
            features = []
            batch_size = self.ausil1.max_batch_size
            for i in range(int(np.ceil(len(curr_spec) / batch_size))):
                batch = curr_spec[i * batch_size : (i + 1) * batch_size]
                features.append(self.ausil1(batch)[0])
            curr_feat = np.concatenate(features, axis=0)
            curr_feat = curr_feat / np.linalg.norm(curr_feat, axis=-1, keepdims=True)
            logger.success(f'Ausil_1 query inference successed')
            
            if comp_id == video_id:
                logger.info(f'Got identical video_id: {comp_id}. Adding it to result data...')
                result.append(data[n])
                continue
            
            try:
                compared_audio = self.redash.download_audios_by_video_ids(
                    comp_id,
                    'audio_tmp',
                    height=144,
                    start=comp_start,
                    time=comp_stop - comp_start
                )[0]
                compared_spec = extract_spectrogram(
                    compared_audio,
                    offset=comp_start,
                    duration=comp_stop-comp_start
                )
                logger.success(f'Target spectrogram has been extracted')
            except Exception as e:
                logger.warning(f'Target spectrogram was not extracted. Adding video to result, skipping...')
                result.append(data[n])
                logger.error(f'{traceback.format_exc()}')
                if os.path.exists(compared_audio):
                    os.remove(compared_audio)
                    logger.info(f'Target audio {compared_audio} has been deleted')
                continue
            
            features = []
            batch_size = self.ausil1.max_batch_size
            for i in range(int(np.ceil(len(compared_spec) / batch_size))):
                batch = compared_spec[i * batch_size : (i + 1) * batch_size]
                features.append(self.ausil1(batch)[0])
            compared_feat = np.concatenate(features, axis=0)
            compared_feat = compared_feat / np.linalg.norm(compared_feat, axis=-1, keepdims=True)
            logger.success(f'Ausil_1 target inference successed')
            
            x = curr_feat[:self.ausil2.max_batch_size]
            y = compared_feat[:self.ausil2.max_batch_size]
            similarity_result = self.ausil2(x, y)[0][0][0]
            logger.success(f'Ausil_2 similarity inference successed')
            
            logger.info(f'Audio similarity {video_id} {(cur_start, cur_stop)} -> {comp_id} {(comp_start, comp_stop)} = {similarity_result}')
            if similarity_result > 0.65:
                result.append(data[n])
            
            if os.path.exists(compared_audio):
                os.remove(compared_audio)
                logger.info(f'Target audio {compared_audio} has been deleted')
        logger.info('End audio processing')
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
        
        try:
            filepath = self.videos_folder / f'{name}-new.mp4'
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
                
                dataloader = VideoDataloader(video_path, self.seq_len, self.timesformer.max_batch_size,
                                         stride=8, transforms=self.transform)
                
                npy_path = Path(video_path).parent.parent / 'train_pickles' / f"{str(Path(video_path).stem)}.npy"
                # npy_path = Path(video_path).parent.parent / 'train_pickles' / f"1.npy"
                if os.path.exists(npy_path):
                    features = np.load(npy_path)
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
                        # features.append(feature)
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
                
                is_duplicate = True
                duplicate_for = 'qwerqwerwer'
                
                logger.info('Segments have been converted to timestamps')
                result = {
                    'video_link': video_link,
                    'is_duplicate': is_duplicate,
                    'duplicate_for': duplicate_for
                }
            
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
    config.read(f'configs/resources.ini')
    # config.read(f'piracy_detection/configs/resources.ini')
    config = config['adapter']
    
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
    # obj = path.format('0a2ccbf4-016f-46b6-92bb-677e618885b0')
    # result = model(obj)
    # print(time.time() - t1)
    # print(result)