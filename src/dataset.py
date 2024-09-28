
import os.path as osp
import pickle as pk
import time

import albumentations as A
import cv2
import numpy as np
import torch
from torch.utils.data import Dataset
from typing import *
from pathlib import Path


class VideoDataset(Dataset):
    def __init__(self,
                 data_root,
                 query_file = None,
                 database_file = None,
                 clip_len = 8,
                 out_size = 224,
                 ):
        if database_file and query_file:
            self.load_annotations(query_file, database_file)
            self.data_prefix = data_root
        else:
            self.video_infos = self.get_videos(data_root)
            self.data_prefix = ''
        self.clip_len = clip_len
        self.fps_interval = 1
        self.transform = A.Compose([
            A.SmallestMaxSize(224),
            A.CenterCrop(224, 224),
            A.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))
        ])

    
    def get_files(self, path: str, extensions: List[str]) -> List[str]:
        """
        Возвращает абсолютные пути ко всем файлам в папке и подпапках

        Args:
            path (str): Корневой каталог
            extensions (List[str]): Расширения для поиска файлов

        Returns:
            List[str]: Список абсолютных путей
        """
        all_files = []
        dp = Path(path)
        for ext in extensions:
            all_files.extend(dp.rglob(ext))
        return all_files

    def get_videos(self, path: str, extensions: List[str] = ['*.mp4', '*.mov']) -> List[str]:
        """
        Возвращает абсолютные пути ко всем изображениям в папке и подпапках.
        Если изображение битое обрабатывается исключение.

        Args:
            path (str): Корневой каталог
            extensions (List[str]): Расширения для поиска изображений.
                Defaults to ['*.mp4', '*.mov'].

        Returns:
            List[str]: Список абсолютных путей
        """
        all_files = []
        for video in self.get_files(path, extensions):
            try:
                str_path = str(video)
                cap = cv2.VideoCapture(str_path)
                cap.release()
                all_files.append([video.stem, str_path])
            except Exception as e:
                print(f'Некорректное видео: {str_path}')
        return all_files
    
    
    def load_annotations(self, query_file, database_file):
        self.queries = np.loadtxt(query_file, dtype=str)
        self.queries = np.expand_dims(self.queries, axis=0) if self.queries.ndim == 1 else self.queries
        self.queries = self.queries.tolist()
        self.queries_ids = [x[0] for x in self.queries]
        
        
        self.database = np.loadtxt(database_file, dtype=str)
        self.database = np.expand_dims(self.database, axis=0) if self.database.ndim == 1 else self.database
        self.database = self.database.tolist()
        self.database_ids = [x[0] for x in self.database]

        self.video_infos = self.queries + self.database 
        self.all_db =  self.database_ids
        print(f"read {len(self.queries)} query, {len(self.database)} database")
    
    
    def load_video(self, video_path):
        frames = self.load_video_cv2(video_path, self.fps_interval)
        n_batch = int(len(frames) / float(self.clip_len) + 0.5) 
        n_batch = max(n_batch, 1)
        n_frames_expect = self.clip_len * n_batch
        frames = frames[:n_frames_expect]
        if len(frames) > 0 and len(frames) < n_frames_expect:
            pad_frames = [np.zeros(frames[-1].shape).astype(np.uint8) for _ in range(n_frames_expect - len(frames))]
            frames = np.concatenate([frames, pad_frames], axis=0)
        if len(frames) == 0:
            print(video_path)
            raise ValueError
        return frames
    
    
    def load_video_cv2(self, video, fps_ratio=1, all_frames=False):
        cv2.setNumThreads(4)
        cap = cv2.VideoCapture(video)
        fps = cap.get(cv2.CAP_PROP_FPS)
        
        if fps > 144 or fps is None: 
            fps = 25
        
        fps *= fps_ratio
        frames = []
        count = 0
        skip_first = True
        while cap.isOpened():
            ret = cap.grab()
            if int(count % round(fps)) == 0 or all_frames:
                if skip_first and ret == False:
                    skip_first=False
                    continue
                    
                ret, frame = cap.retrieve()
                if isinstance(frame, np.ndarray):
                    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    frames.append(frame)
                else:
                    break
            count += 1
        cap.release()
        return np.array(frames)

    
    def __len__(self):
        return len(self.video_infos)
    
    
    def __getitem__(self, idx):
        uid, video_path = self.video_infos[idx]
        if self.data_prefix is not None:
            video_path = osp.join(self.data_prefix, video_path)
            t1 = time.time()
            frames = self.load_video(video_path)
            t2 = time.time() - t1
            frames = np.array([self.transform(image=frame)['image'] for frame in frames])
        frames = torch.from_numpy(frames).reshape((-1, 8, 224, 224, 3)).permute(0, 4, 1, 2, 3).float() # expect  (n_clips, n_frames, n_channels, height, width)
        return frames, idx, t2, len(frames), uid

    def get_uid_by_idx(self, idx):
        return self.video_infos[idx][0]