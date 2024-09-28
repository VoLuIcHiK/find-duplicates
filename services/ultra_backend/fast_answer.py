import json
import os
import pathlib
import random
import time

import dotenv
from loguru import logger

from .rabbit import connector
from .rabbit.custom_models import RabbitPipelineOut, RabbitPipelineIn
from .rabbit.rabbit_wrapper import RabbitWrapper


def fast_answer_thread():
    rabbit_url = os.environ.get('RABBIT_URL')
    if rabbit_url is None:
        raise ValueError("RABBIT_URL is not set")
    input_queue = os.environ.get('RABBIT_INPUT_QUEUE')
    if input_queue is None:
        raise ValueError("RABBIT_INPUT_QUEUE is not set")
    output_queue = os.environ.get('RABBIT_OUTPUT_QUEUE')
    if output_queue is None:
        raise ValueError("RABBIT_OUTPUT_QUEUE is not set")
    rabbit_wrapper = RabbitWrapper(rabbit_url)
    while True:

        data = rabbit_wrapper.get_message(output_queue, block=True)
        if data:
            logger.info(data)
            data_dict = json.loads(data)
            nn_in = RabbitPipelineOut.model_validate(data_dict)
            is_duplicate = random.random() > 0.5
            duplicate_for = nn_in.video_link if is_duplicate else None
            nn_output = RabbitPipelineIn(video_link=nn_in.video_link, is_duplicate=is_duplicate,
                                         duplicate_for=duplicate_for)
            connector.compact_publish_data(rabbit_url, input_queue,
                                           json.dumps(nn_output.model_dump(), ensure_ascii=False))
        time.sleep(0.01)


def main():
    env_file = pathlib.Path(__file__).parent / '.env'
    if env_file.exists():
        dotenv.load_dotenv(env_file)
    fast_answer_thread()


if __name__ == '__main__':
    main()
