import contextlib
import json
import multiprocessing
import pathlib
import threading
import time
from unittest import TestCase

import fastapi
import requests
import uvicorn
from loguru import logger
from sqlalchemy import create_engine, StaticPool
from sqlalchemy.orm import sessionmaker

from .. import main
from ..db.database import Base
from ..db.main import get_db
from ..models import ResponseModel, WebhookRequestModel
from ..rabbit.ысруьфы import NNInput, NNOutput
from ..rabbit_worker import RabbitConsumerThread, RabbitTask
from ..rabbit.connector import compact_publish_data, compact_get_data
from fastapi.testclient import TestClient

from ...backend.models import Upload


class TestRabbitConsumerThread(TestCase):
    def test_run(self):
        rabbit_url = 'amqp://guest:guest@localhost:5672'
        queue1 = 'test_IN_queue'
        rabbit_consumer_thread = RabbitConsumerThread(daemon=True, rabbit_url=rabbit_url, input_queue=queue1)
        rabbit_consumer_thread.start()
        res: dict = dict()

        def _callback(x: RabbitTask):
            nonlocal res
            res = x.result
            rabbit_consumer_thread.stop()

        nn_in = NNInput(task_id='9', file_path='test')
        task = RabbitTask(nn_in.task_id, in_args=nn_in, callback=_callback)
        rabbit_consumer_thread.add_task(task)
        #
        nn_out_result = NNOutput.ResultDict(familiar_videos=['1', '2', '3'], description='test')
        nn_out = NNOutput(inputs=nn_in, result=nn_out_result)
        compact_publish_data(rabbit_url, queue1, nn_out.model_dump())
        #
        rabbit_consumer_thread.join(3)
        self.assertIn('familiar_videos', res)
        self.assertDictEqual(nn_out_result.model_dump(), res)


class TestServer(uvicorn.Server):
    def install_signal_handlers(self):
        pass

    @contextlib.contextmanager
    def run_in_thread(self):
        thread = threading.Thread(target=self.run)
        thread.start()
        try:
            while not self.started:
                time.sleep(1e-3)
            yield
        finally:
            self.should_exit = True
            thread.join()


rabbit_consumer_thread: RabbitConsumerThread | None = None


class TestUpload(TestCase):
    SQLALCHEMY_DATABASE_URL = "sqlite://"

    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    rabbit_url = 'amqp://guest:guest@localhost:5672'

    @staticmethod
    def override_get_db():
        db = None
        try:
            db = TestUpload.TestingSessionLocal()
            yield db
        finally:
            db.close()

    queue1 = 'test_OUT_queue'
    queue2 = 'test_IN_queue'

    @staticmethod
    def override_rabbit():
        global rabbit_consumer_thread
        rabbit_consumer_thread = RabbitConsumerThread(daemon=True, rabbit_url='', input_queue='')
        info = main.RabbitConnectionInfo(rabbit_url=TestUpload.rabbit_url, queue=TestUpload.queue1,
                                    rabbit_consumer=rabbit_consumer_thread)
        return info

    def test_upload_file(self):
        while compact_get_data(self.rabbit_url, self.queue1):
            continue
        main.app.dependency_overrides[get_db] = self.override_get_db
        main.app.dependency_overrides[main.get_rabbit] = self.override_rabbit
        client = TestClient(main.app)
        base_folder = pathlib.Path(__file__).parent
        base_file = pathlib.Path(__file__)
        response = client.post('/upload', data={'path': str(base_file.absolute())})
        self.assertEqual(response.status_code, 201)
        logger.info(response.json())
        self.assertIn('status', response.json())
        #
        val = compact_get_data(self.rabbit_url, self.queue1)
        logger.info(val)
        self.assertIsNotNone(val)

    def test_upload_callback(self):
        webhook_url = 'http://localhost:8051/test'
        rabbit_cons = RabbitConsumerThread(daemon=True, rabbit_url=self.rabbit_url, input_queue=TestUpload.queue2)

        def override_rabbit2():
            info = main.RabbitConnectionInfo(rabbit_url=TestUpload.rabbit_url, queue=TestUpload.queue1,
                                        rabbit_consumer=rabbit_cons)
            return info

        main.app.dependency_overrides[main.get_rabbit] = override_rabbit2
        client = TestClient(main.app)
        test_webhook = fastapi.FastAPI()
        data: dict | None = None

        @test_webhook.post('/test')
        async def post_webhook(request: fastapi.Request):
            nonlocal data
            if data is None:
                try:
                    data_dict = await request.json()
                    data = dict(data_dict)
                except Exception as e:
                    logger.exception(f"Error parsing json: {e}")

        uvicorn_server = TestServer(uvicorn.Config(test_webhook, host='localhost', port=8051))
        while compact_get_data(self.rabbit_url, self.queue1):
            continue
        base_folder = pathlib.Path(__file__).parent
        base_file = pathlib.Path(__file__)
        with uvicorn_server.run_in_thread():
            response = client.post(
                '/upload',
                data={
                    'path': str(base_file.absolute()),
                    'webhook_url': webhook_url,
                    'webhook_request_id': 'test42'
                })
            self.assertEqual(response.status_code, 201)
            logger.info(response.json())
            self.assertIn('status', response.json())
            #
            val = compact_get_data(self.rabbit_url, self.queue1)
            nn_in = NNInput.from_dict(json.loads(val.decode('utf-8')))
            logger.info(json.dumps(nn_in.model_dump(), ensure_ascii=False))
            #
            nn_out = NNOutput(inputs=nn_in, result=NNOutput.ResultDict(familiar_videos=['1', '2', '3'],
                                                                       description='test'))
            compact_publish_data(self.rabbit_url, self.queue2, nn_out.model_dump())
            #
            with rabbit_cons.run_in_thread():
                # wait data max 3 seconds
                time_start = time.time()
                while data is None and time.time() - time_start < 3:
                    time.sleep(1e-3)
                self.assertIsNotNone(data)
                logger.info(data)
            # nn_out = NNOutput.load(val)

    def test_upload_and_check_file(self):
        while compact_get_data(self.rabbit_url, self.queue1):
            continue
        rabbit_cons_thread = RabbitConsumerThread(daemon=True, rabbit_url=self.rabbit_url, input_queue=self.queue2)

        def _override_rabbit():
            info = main.RabbitConnectionInfo(rabbit_url=TestUpload.rabbit_url, queue=TestUpload.queue1,
                                        rabbit_consumer=rabbit_cons_thread)
            return info

        main.app.dependency_overrides[get_db] = self.override_get_db
        main.app.dependency_overrides[main.get_rabbit] = _override_rabbit
        client = TestClient(main.app)
        base_folder = pathlib.Path(__file__).parent
        base_file = pathlib.Path(__file__)
        response = client.post('/upload', data={
            'path': str(base_folder.absolute()),
            'webhook_url': 'http://localhost:8051/test',
            'webhook_request_id': 'test42'})
        self.assertEqual(response.status_code, 201)
        logger.info(response.json())
        self.assertIn('status', response.json())
        #
        val = compact_get_data(self.rabbit_url, self.queue1)
        logger.info(val)
        self.assertIsNotNone(val)
        nn_in = NNInput.from_dict(json.loads(val.decode('utf-8')))
        logger.info(json.dumps(nn_in.model_dump(), ensure_ascii=False))
        #
        client = TestClient(main.app)
        test_webhook = fastapi.FastAPI()
        data: dict | None = None

        @test_webhook.post('/test')
        async def post_webhook(request: fastapi.Request):
            logger.info("Test webhook got message!")
            nonlocal data
            if data is None:
                try:
                    data_dict = await request.json()
                    data = dict(data_dict)
                except Exception as e:
                    logger.exception(f"Error parsing json: {e}")
        host = 'localhost'
        port = 8051
        uvicorn_server = TestServer(uvicorn.Config(test_webhook, host=host, port=port))
        with uvicorn_server.run_in_thread():

            nn_out = NNOutput(inputs=nn_in, result=NNOutput.ResultDict(familiar_videos=[str(base_file.absolute())],
                                                                       description='test'))
            compact_publish_data(self.rabbit_url, self.queue2, nn_out.model_dump())
            #
            with rabbit_cons_thread.run_in_thread():
                # wait data max 3 seconds
                time_start = time.time()
                while len(rabbit_cons_thread.tasks) > 0 and time.time() - time_start < 3:
                    time.sleep(1e-3)
                self.assertIsNone(compact_get_data(self.rabbit_url, self.queue1))

            response = WebhookRequestModel.model_validate(data)
            familiar_videos = response.data.familiar_videos
            self.assertEqual(1, len(familiar_videos))
            video_url = str(familiar_videos[0])
            video_url = video_url.replace('http://testserver/', '')
            resp = client.get('/uploads')
            # self.assertEqual(str(base_file.absolute()), str(video_path))
            data = None
                # nn_out = NNOutput.load(val)
