from pathlib import Path
import os
from tqdm.notebook import tqdm
import pickle
import sys
from multiprocessing import Process
sys.path.append('..')

from services.adapter.audio_fingerprint.shazam import fingerprint_file

data_root = Path('/home/borntowarn/projects/borntowarn/train_data_yappy') / 'train_dataset'

def pkl_aud(pathes, n):
    root = Path('/home/borntowarn/projects/borntowarn/train_data_yappy')
    frames_root = Path('/home/borntowarn/projects/borntowarn/train_data_yappy') / 'train_pickles_audios'
    
    for obj in tqdm(pathes, position=n, total = len(pathes)):
        # pass
        if os.path.exists(frames_root / Path(obj).with_suffix('.pkl')):
            continue

        try:
            query_fingerprint = fingerprint_file(data_root / obj)
        except:
            query_fingerprint = ()
        
        pickle.dump(query_fingerprint, open(frames_root / Path(obj).with_suffix('.pkl'), 'wb'))


if __name__ == '__main__':
    to_dl = os.listdir(data_root)

    part_size = 8
    bs = len(to_dl) // part_size
    processes = []
    
    for i in range(part_size):
        part_links = to_dl[i*bs: i*bs+bs]
        p = Process(target = pkl_aud, args=(part_links, i,), daemon=True)
        p.start()
        processes.append(p)

    for p in processes:
        p.join()