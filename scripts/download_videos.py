import pandas as pd
import requests
import os
from pathlib import Path
from multiprocessing import Process
from pathlib import Path
from tqdm import tqdm
from loguru import logger

log_level = "DEBUG"
log_format = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS zz}</green> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}):</yellow> <b>{message}</b>"
logger.add("file.log", level=log_level, format=log_format, colorize=False, backtrace=True, diagnose=True)


ROOT = Path('/home/free4ky/Downloads/train_data_yappy/train_dataset_dl')
df = pd.read_csv('~/Downloads/train_data_yappy/train.csv')
file_names = df['link'].apply(lambda x: x.split('/')[-1])
d_files = Path('/home/free4ky/Downloads/train_data_yappy/train_dataset').rglob('*')
d_files = [d.name for d in d_files]
to_dl = list(set(file_names) ^ set(d_files))
to_dl = [f'https://s3.ritm.media/yappy-db-duplicates/{n}' for n in to_dl]


def download_video(links, pos, dest_folder:Path):
    os.makedirs(dest_folder, exist_ok=True)
    pbar = tqdm(desc=f"URL search...", total=len(links), position=pos)
    for l in links:
        try:
            response = requests.get(l)
        except Exception as e:
            logger.error(f'Unable to download file {l}; error: {e}')
        try:
            name = l.split('/')[-1]
            with open(dest_folder / name, 'wb') as f:
                f.write(response.content)
        except Exception as e:
            logger.error(f'Error saving file {l}')
        
        pbar.update(1)


if __name__ == '__main__':
    part_size = 3
    bs = len(to_dl)//part_size
    processes = []
    for i in range(part_size):
        part_links = to_dl[i*bs: i*bs+bs]
        p = Process(target = download_video, args=(part_links, i, ROOT), daemon=True)
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()
