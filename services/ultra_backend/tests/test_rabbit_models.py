import json
from unittest import TestCase

from loguru import logger

from ..rabbit import custom_models


class TestCustomModels(TestCase):
    def test_rabbit_pipeline_in(self):
        """
        Проверка входных данных на создание модели из них
        """
        # Create a custom model
        custom_model_dict = {
            "inputs": {
                "video_link": "https://s3.ritm.media/yappy-db-duplicates/dda04107-4a60-4335-a37a-c078ae1b7880.mp4"},
            "process_time": 0.8173038959503174,
            "current_time": "2024-09-28 13:31:22",
            "result": {
                "video_link": "https://s3.ritm.media/yappy-db-duplicates/dda04107-4a60-4335-a37a-c078ae1b7880.mp4",
                "is_duplicate": True
            }
        }
        custom_model = custom_models.RabbitPipelineIn.model_validate(custom_model_dict)
        logger.info(json.dumps(custom_model.model_dump(), ensure_ascii=False))

