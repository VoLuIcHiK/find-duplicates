import time
import traceback
from abc import ABC, abstractmethod
from typing import *

from loguru import logger

from .answer import BaseAnswer


class BaseWrapper(ABC):
    
    
    @abstractmethod
    def _load_config(self) -> None:
        pass
    
    
    @abstractmethod
    def _create_topic(self) -> None:
        pass
    
    
    @abstractmethod
    def _create_answer(self) -> BaseAnswer:
        pass
    
    
    @abstractmethod
    def publish(self) -> None:
        pass
    
    
    @abstractmethod
    def listen(self) -> None:
        pass
    
    
    def _process_item(self, pipeline, **payload) -> tuple[dict, float]:
        try:
            logger.info('Start processing an item')
            start_time = time.time()
            result = pipeline(**payload)
            process_time = time.time() - start_time
            logger.info(f'Item has been processed in {process_time}s')
        except Exception as e:
            result = None
            process_time = None
            logger.error(f'{traceback.format_exc()}')
        return result, process_time
    
    