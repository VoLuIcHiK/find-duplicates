import json
from dataclasses import dataclass

from ..base import BaseAnswer

@dataclass
class KafkaAnswer(BaseAnswer):
    def __post_init__(self):
        self.json = json.dumps(
            {
                'inputs': self.inputs,
                'process_time': self.time,
                'result': self.result
            },
            ensure_ascii=False
        )