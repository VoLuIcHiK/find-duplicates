import os
from ml_utils import RabbitWrapper

from loguru import logger
from pathlib import Path

os.environ['LOGURU_LEVEL'] = 'INFO'

if __name__ == '__main__': 
    root = Path('/home/borntowarn/projects/borntowarn/train_data_yappy/train_dataset')
    
    videos = open('train.txt').readlines()
    videos = [{'video_link': str(root / f"{i.strip()}.mp4")} for i in videos]
    
    broker = RabbitWrapper(
        '../../configs/resources.ini',
        'adapter_local',
        swap_topics=True
    )
    
    broker.publish(videos)
    logger.success(f'Published {len(videos)} videos')