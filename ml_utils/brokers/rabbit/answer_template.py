import json
from dataclasses import dataclass
from datetime import datetime

from ..base import BaseAnswer


@dataclass
class RabbitAnswer(BaseAnswer):
    def __post_init__(self):
        self.json = json.dumps(
            {
                'inputs': self.inputs,
                'process_time': self.time,
                'current_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'result': self.result
            },
            ensure_ascii=False
        )