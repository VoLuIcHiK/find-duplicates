from pathlib import Path
from typing import Union, Optional, Callable

import cv2
import numpy as np


class VideoDataloader:
    def __init__(
        self, 
        video: Union[Path, str],
        sequence_length: int,
        batch_size: int,
        all_frames: bool = False,
        stride: int = 1,
        frames_per_second: int = 1,
        transforms: Optional[Callable] = None,
        is_frame_drop: Optional[Callable] = None,
        dtype: type = np.float32
    ) -> None:
        """
        Даталоадер для видео, возвращающий батч последовательностей длиной sequence_length.
        Есть возможность загрузки скользящим окном таким образом, что каждый кадр считается
        и декодируется только 1 раз.
        
        Частота считывания кадров указана параметром frames_per_second.
        В секунду будет декодироваться frames_per_second кадров.
        
        На декодированный кадр возможно применить Transform.
        
        Последний батч добивается до требуемой длины с помощью паддинга в виде черного
        изображения только если собственных кадров в нем не меньше половины.
        Примеры (1 - собственный кадр, 0 - паддинг):
            [1, 1, 1, 1, 0, 0, 0, 0]
            [1, 1, 1, 1, 1, 1, 0, 0]
            [1, 1, 1, 0, 0]
        Такого быть не может:
            [1, 1, 1, 0, 0, 0, 0, 0]
            [1, 1, 0, 0, 0]

        Args:
            video (Union[Path, str]): Путь к видео
            sequence_length (int): Длина каждой последовательности
            batch_size (int): Требуемый размер батча
            all_frames (bool, optional): Если True то читаем все кадры. Defaults to False.
            stride (int, optional):  Шаг скользящего окна. Defaults to 1.
            frames_per_second (int, optional): Частота считывания кадров. Defaults to 1.
            transforms (Callable, optional): Применяемые трансформации. Defaults to None.
            is_frame_drop (Callable, optional): Функция, отбрасывающая кадры по условию.
                Должна получать только frame в виде np.array и возвращать True(отбрасываем)
                или False(не отбрасываем) при каждом вызове. Defaults to None.
            dtype (type, optional): Тип возвращаемого изоражения. Defaults to np.float32.
        """
        
        self.video = video
        self.sequence_length = sequence_length
        self.batch_size = batch_size
        self.all_frames = all_frames
        self.frames_per_second = frames_per_second
        self.transforms = transforms
        self.is_frame_drop = is_frame_drop
        self.stride = stride
        self.dtype = dtype
        
        self.cap = cv2.VideoCapture(video)
        self.fps = self.cap.get(cv2.CAP_PROP_FPS)
        self.frame_shape = (
            int(self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT)),
            int(self.cap.get(cv2.CAP_PROP_FRAME_WIDTH)),
            3
        )
        
        if self.fps > 144 or self.fps is None: 
            self.fps = 25
        self.fps = round(self.fps) // frames_per_second
        
        self.skipped_frames = []
        self.appended_frames = []
    
    
    def _batch_calc(self, frames) -> int:
        return int(np.ceil((len(frames) - self.sequence_length + 1) / self.stride))
    
    
    def _create_padding_sequence(self) -> np.ndarray:
        # Необходимое количество кадров паддинга которое мы должны 
        # добавить в текущие кадры, чтобы сформировать батч
        max_pad_for_seq = min(self.max_pad, self.batch_size - (self.current_batch // self.stride))
        pad_frames = np.zeros(
            (max_pad_for_seq, *self.frames[0].shape),
            dtype=self.dtype
        )
        
        # Паддинг последним кадром
        for i in range(len(pad_frames)):
            pad_frames[i] = self.frames[-1]
            
        # if self.transforms:
        #     pad_frames = np.array([self.transforms(frame) for frame in pad_frames])
        # Уменьшаем количество паддингов, 
        # так как мы уже использовали часть возможного
        self.max_pad -= len(pad_frames)
        return pad_frames
    
    
    def _add_paddings(self) -> None:
        # Добавляем паддинги для последних последовательнстей, чтобы правильно сформировать батчи
        if self.current_batch < self.batch_size and self._should_add_padding():
            self.cap.release() # Закрываем видео, так как все кадры забраны, остается только паддинг
            pad_frames = self._create_padding_sequence()
            self.frames = np.concatenate([self.frames, pad_frames], axis=0)
            self.current_batch = self._batch_calc(self.frames)
    
    
    def _drop_frame_check(self, frame) -> Optional[np.ndarray]:
        if self.is_frame_drop:
            is_drop = self.is_frame_drop(frame)
            if is_drop:
                self.skipped_frames.append(self.frame_count // self.fps)
                self.frame_count += 1
                return None
        self.appended_frames.append(self.frame_count // self.fps)
        return frame
    
    
    def _read_frames(self) -> None:
        # Добираем кадры до нужного количества или пока не дойдем до конца
        while self.current_batch < self.batch_size and self.success:
            self.success = self.cap.grab()
            if int(self.frame_count % round(self.fps)) == 0 or self.all_frames:
                if self.skip_first and self.success == False:
                    self.skip_first=False
                    continue
                    
                self.success, frame = self.cap.retrieve()
                if self.success:
                    frame = self._drop_frame_check(frame)
                    if not isinstance(frame, np.ndarray):
                        continue
                    
                    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    if self.transforms:
                        frame = self.transforms(frame)
                    self.frames.append(frame)
                else:
                    break
            self.frame_count += 1
            self.current_batch = self._batch_calc(self.frames)
    
    
    def _create_strided_sequences(self) -> np.ndarray:
        # Возвращаем последовательность кадров сразу с окнами шага stride
        result = np.array(self.frames, dtype=self.dtype)
        strides = list(result.strides)
        strides.insert(0, strides[0] * self.stride)
        shape = (self.current_batch, self.sequence_length, *self.frames[0].shape)
        # Лютый трюк с окнами позволяет из массива кадров вернуть
        # набор окон с фиксированными заданными шагами
        return np.lib.stride_tricks.as_strided(result, shape, strides=strides)
    
    
    def _is_video_opened(self) -> bool:
        return self.cap.isOpened() and self.success
    
    
    def _should_add_padding(self) -> bool:
        #! hotfix добавляем паддинги, если есть только 1/4 от размера батча
        return (len(self.frames) % self.sequence_length) >= self.stride // 4 and self.max_pad > 0 and len(self.frames) > 0
    
    
    def __iter__(self):
        self.skip_first = True # На всякий случай скипаем вероятный битый кадр
        self.success = True # Состояние считывания следующего кадра
        self.frame_count = 0 # Текущее количество всех пройденных кадров
        self.frames = [] # Набранные кадры
        
        # ОбщееМаксимальный возможный паддинг черными кадрами
        # не должен превышать половину длины последовательность 
        # self.max_pad = self.sequence_length // 2
        self.max_pad = 6 #! hotfix
        return self
    
    def __next__(self) -> np.ndarray:
        if self._is_video_opened() or self._should_add_padding():
            self.frames = self.frames[self.batch_size * self.stride:]
            self.current_batch = self._batch_calc(self.frames)
            
            self._read_frames()
            self._add_paddings()
            
            if len(self.frames) >= self.sequence_length:
                return self._create_strided_sequences()
        
        self.cap.release()
        raise StopIteration