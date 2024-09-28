import json
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import *

@dataclass
class BaseAnswer(ABC):
    time: Optional[float]
    inputs: Optional[dict]
    result: Optional[dict]
    json: str = field(init=False)
    
    @abstractmethod
    def __post_init__(self) -> None:
        pass