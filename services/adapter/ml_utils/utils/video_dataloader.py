from pathlib import Path
from typing import Callable, Optional, Union

import cv2
import numpy as np


class VideoDataloader:
    def __init__(
        self, 
        video: Union[Path, str],
        transforms: Optional[Callable] = None,
    ) -> None:
        self.video = video
        self.transforms = transforms
        
        self.cap = cv2.VideoCapture(video)
        self.fps = self.cap.get(cv2.CAP_PROP_FPS)
        self.length = int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))
        
        self.need_indexes = np.floor(np.linspace(0, self.length - 1, 8))
    
    def _read_frames(self) -> None:
        # Добираем кадры до нужного количества или пока не дойдем до конца
        while self.success:
            self.success = self.cap.grab()
            if self.frame_count in self.need_indexes and self.success:
                
                self.success, frame = self.cap.retrieve()
                if self.success:
                    if not isinstance(frame, np.ndarray):
                        continue
                    
                    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    if self.transforms:
                        frame = self.transforms(frame)
                    self.frames.append(frame)
                else:
                    break
            self.frame_count += 1
    
    def _is_video_opened(self) -> bool:
        return self.cap.isOpened() and self.success

    
    def __iter__(self):
        self.success = True # Состояние считывания следующего кадра
        self.frame_count = 0 # Текущее количество всех пройденных кадров
        self.frames = [] # Набранные кадры
        
        return self
    
    def __next__(self) -> np.ndarray:
        if self._is_video_opened():
            self._read_frames()
            return np.stack(self.frames)
        
        self.cap.release()
        raise StopIteration