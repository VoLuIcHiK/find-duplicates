import argparse
import json
import os

from loguru import logger
from pathlib import Path

os.environ['LOGURU_LEVEL'] = 'INFO'

parser = argparse.ArgumentParser()
parser.add_argument('--broker', type=str, required=False, default='rabbit')


if __name__ == '__main__':
    args = parser.parse_args()
    
    root = Path('/home/borntowarn/projects/borntowarn/train_data_yappy/train_dataset')
    videos = open('all_videos.txt').readlines()
    videos = [{'video_link': str(root / f"{i.strip()}.mp4")} for i in videos]
    
    
    match args.broker:
        case 'kafka':
            from ml_utils import KafkaWrapper
            broker = KafkaWrapper('configs/resources.ini', 'service', swap_topics=True)
        case 'rabbit':
            from ml_utils import RabbitWrapper
            broker = RabbitWrapper('../../configs/resources.ini', 'adapter_local', swap_topics=True)
        case _:
            raise Exception('Broker can be only "kafka" or "rabbit"')
    
    broker.publish(videos)
    logger.success(f'Published {len(videos)} videos')