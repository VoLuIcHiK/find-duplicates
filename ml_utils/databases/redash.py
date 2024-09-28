import configparser
import json
import os
import socket
import time
from pathlib import Path
from subprocess import PIPE, run
from typing import Optional, Union, Any, Generator, List, Dict, Set
from urllib.request import urlretrieve

import requests

from .. import logger

# Если нет доступа к серверу по ссылке,
# то прерываемся через 3 секунды
socket.setdefaulttimeout(3)


def file_sizeof(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


class RedashQuery:
    """
    Класс, описывающий запрос к Redash
    """
    def __init__(self, query_id: int, max_age: Optional[int] = None, **parameters) -> None:
        """
        Args:
            query_id (int): Id запроса в Redash
            max_age (int, optional): Допустимое время в секундах с прошлого кэшированного запроса в Redash.
                None для моментального запроса прошлых кэшированных данных.
                0 для обновления таблицы и получения новых данных.
                Пример: Если установить данный параметр = 3600, то данные будут сначала искаться
                в кэше последнего часа, а если они не найдены, то будет произведен новый запрос к БД.
                Defaults to None.
            **parameters: Именованные параметры для конкретного запроса
        """
        self.query_id = query_id
        self.payload = dict(parameters=parameters)
        if max_age != None:
            self.payload.update({'max_age': max_age})


class RedashWrapper:
    """
    Класс для обработки аудио/видео/запросов из Redash.
    Позволяет получать информацию о канали или видео, скачивать их в определенном качестве и извлекать аудио.

    Для установки конфига через системный переменные:
    1. export REDASH_TOKEN=
    2. export FH_USER=
    3. export FH_PASS=
    """
    
    def __init__(
        self,
        config_path: str = None,
        service_name: str = None,
        config: dict = {},
        job_timeout: int = 10,
        token: str = None,
        fh_user: str = None,
        fh_pass: str = None
    ) -> None:
        """
        Инициализировать конфигурации можно 3 способами 
        (указаны в порядке важности, верхние уровни перетирают значения нижних):

        1. Аргументами инициализации класса
        2. ini файлом конфигурации с указанием наименования сервиса
        3. Через системные переменные (os.env)
        
        Args:
            config_path (str, optional): Путь до ini файла. Defaults to None.
            service_name (str, optional): Наименование сервиса в ini. Defaults to None.
            config (dict, optional): Загруженный конфиг в виде словаря.
                Инициализация config_path + service_name эквивалентна config. Defaults to {}.
            job_timeout (int, optional): Таймаут ожидания ответа от Redash. Defaults to 10.
            token (str, optional): Токен Redash. Defaults to None.
            fh_user (str, optional): Пользователь fileheap. Defaults to None.
            fh_pass (str, optional): Пароль fileheap. Defaults to None.

        Raises:
            ValueError: _description_
        """
        self.TOKEN = token
        self.FH_USER = fh_user
        self.FH_PASS = fh_pass
        self.config = config
        
        if config_path and service_name and os.path.exists(config_path):
            self.config = configparser.ConfigParser()
            self.config.read(config_path)
            self.config = self.config[service_name]
        self._load_config()
        
        if not self.TOKEN:
            raise ValueError('Cant get REDASH_TOKEN')
        
        self.sess = requests.Session()
        self.sess.headers.update({'Authorization': f'Key {self.TOKEN}'})
        self.fh_auth = (self.FH_USER, self.FH_PASS)
        
        
        self.url = 'https://redash.rutube.ru/'
        self.fh_url = 'https://fh.rutube.ru/file/{}/redirect/'
        self.job_url = 'https://redash.rutube.ru/api/jobs/{}'
        self.result_url = 'https://redash.rutube.ru//api/queries/{}/results'
        self.json_url = 'https://redash.rutube.ru//api/queries/{}/results/{}.json'
        self.job_timeout = job_timeout
    
    
    def _load_config(self):
        if not self.TOKEN:
            self.TOKEN = self.config.get('REDASH_TOKEN', os.environ.get('REDASH_TOKEN', None))
        if not self.FH_USER:
            self.FH_USER = self.config.get('FH_USER', os.environ.get('FH_USER', None))
        if not self.FH_PASS:
            self.FH_PASS = self.config.get('FH_PASS', os.environ.get('FH_PASS', None)) 

        logger.info('Config has been loaded')
    
    
    def poll_job_result(self, query: RedashQuery) -> Optional[list[dict]]:
        """
        Функция для обработки запроса из Redash.

        Args:
            query (RedashQuery): Сущность запроса для обработки

        Returns:
            Optional[list[dict]]: Результат обработки. Непосредственно строки из Redash запроса.
        """
        response = self.sess.post(
            self.result_url.format(query.query_id),
            data=json.dumps(query.payload)
        )
        job = response.json().get('job', None)
        
        if job:
            start_time = time.time()
            # Пока работа не выполнена или не превышен таймаут
            while job['status'] not in (3,4) and (time.time() - start_time < self.job_timeout):
                time.sleep(0.5)
                response = self.sess.get(self.job_url.format(job['id']))
                job = response.json()['job']

            if job['status'] == 3:
                attempts = 0
                while attempts < 10:
                    response = self.sess.get(self.json_url.format(query.query_id, job['query_result_id']))
                    if response.status_code == 200: 
                        return response.json()['query_result']['data']['rows']
                    attempts += 1
                logger.error('Cant get answer from job')
            else:
                logger.error(f'Cant poll job. Error: {response.json()["job"]["error"]}')
        elif response.status_code == 200:
            return response.json()['query_result']['data']['rows']
        else:
            logger.error(f'Cant get response from Redash: {json.loads(response.content)["message"]}')


    def extract_audio_from_video(
        self, 
        video_path: Union[Path, str],
        audio_filename: Union[Path, str],
        start: int = 0,
        time: Optional[int] = None,
    ) -> Optional[Union[Path, str]]:
        """
        Функция для извлечения аудио из видео.
        Пути извлечения/сохранения должны существовать.

        Args:
            video_path (Union[Path, str]): Путь или URL к видео, из которого необходимо извлечь аудио.
            audio_filename (Union[Path, str]): Путь сохранения аудио.
            start (int): Начальная точка видео в секундах с которого извлекаем аудио. Default to 0.
            time (int, optional): Длительность извлечения считая от начала аудио.
                None для извлечения полной длительности. Defaults to None.

        Returns:
            Optional[Union[Path, str]]: Путь к извлеченному аудио
        """
        if os.path.exists(audio_filename):
            logger.warning('Audio exists, it wont`t be downloaded')
            return audio_filename 
        
        command = ['ffmpeg', '-y', '-i', str(video_path), '-ss', str(start), str(audio_filename)]
        if time:
            command = command[:-1] + ['-t', str(time)] + command[-1:]
        
        result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
        
        if result.returncode == 0:
            logger.success(f'Audio has been extracted from {video_path}')
        else:
            last_line = result.stderr.strip().split('\n')[-1]
            if 'does not contain any stream' in last_line:
                logger.warning(f'Video {video_path} has no audio')
            else:
                logger.warning(f'Cant extract/download audio from {video_path}')
            return

        return audio_filename


    def get_filenames_by_video_id(
        self,
        video_id: str,
        query_id: int = 10212,
        max_age: Optional[int] = None
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Функция для получения всех filename в fileheap для video_id.

        Args:
            video_id (str): Id запрашиваемого видео
            query_id (int, optional): Id запроса в redash. Defaults to 10212.
            max_age (int, optional): См описание RedashQuery. Defaults to None.

        Returns:
            Optional[List[Dict[str, Any]]]: Список параметров видео из БД или None, если таковых не существует
        """
        query = RedashQuery(query_id, max_age, video_id=video_id)
        rows = self.poll_job_result(query)
        
        if rows:
            if not len(rows):
                logger.error(f"Video {video_id} doesn't exist")
            else:
                logger.success(f'Got rows for {video_id}')
              
        return rows
    
    
    
    def _get_filename_by_quality(self, rows: list[dict], height: int = 1000) -> Generator[str, Any, None]:
        """
        Функция для перебора filename для всех возможных разрешений видео.
        Сначала будет возвращать видео с качеством(height) равным или меньшим заданному.
        Далее будет возвращать видео с качеством выше требуемого.
        Напимер: [1280, 720, 640, 320, 144, 1920, 2560, 3960]

        Args:
            rows (list[dict]): Список, с возможными качествами видео. Можно брать из `get_filenames_by_video_id`.
            height (int, optional): Минимальная высота видео в пикселях. Defaults to 1000.

        Yields:
            Generator[str]: Итератор по filename
        """
        rows = sorted(rows, key=lambda x: x['height'], reverse=True)
        
        quality_number = 0
        while quality_number < (len(rows) - 1) and rows[quality_number]['height'] > height:
            quality_number += 1
        
        if rows[quality_number]['height'] < height:
            logger.warning(f'Cannot get filename with quality > {height}')
        
        rows = rows[quality_number:] + rows[:quality_number][::-1]
        for row in rows:
            width, height = row["width"], row["height"]
            logger.success(f'Got filename with quality {width}x{height}')
            yield row['filename']


    def download_audio_by_filename(
        self,
        filename: Union[Path, str],
        audio_filename: Union[Path, str],
        start: int = 0,
        time: Optional[int] = None,
    ) -> Optional[Union[Path, str]]:
        """
        Функция для скачивания аудио по filename видео в fileheap.

        Args:
            filename (Union[Path, str]): filename для видео
            audio_filename (Union[Path, str]): Путь к записываемому файлу.
            start (int, optional): Начальная точка видео в секундах с которого извлекаем аудио. Default to 0.
            time (int, optional): Длительность извлечения считая от начала аудио.
                None для извлечения полной длительности. Defaults to None.
            
        Returns:
            Optional[Union[Path, str]]: Путь к записываемому файлу или None, если ошибка
        """
        fh_pass_correct = self.FH_PASS and self.FH_PASS != ''
        fh_user_correct = self.FH_USER and self.FH_USER != ''
        assert fh_pass_correct and fh_user_correct, 'FH_USER and FH_PASS needed for downloading'
        
        audio_filename = str(audio_filename)
        if os.path.exists(audio_filename):
            logger.warning(f'Audio exists, it wont`t be downloaded')
            return audio_filename

        response = requests.get(
            self.fh_url.format(filename),
            allow_redirects=False,
            auth=self.fh_auth
        )
        
        if response.status_code == 302:
            url = response.headers['Location']
        
        try:
            audio_filename = self.extract_audio_from_video(url, audio_filename, start=start, time=time)
            if not audio_filename:
                logger.warning('Cant downloaded audio. Skipping...')
            size = file_sizeof(os.path.getsize(audio_filename))
            logger.success(f'Audio with size {size} has been successfully loaded')
            return audio_filename
        except Exception as e:
            logger.warning(f'Cannot download from {url}. Skipping...')
                    
        if audio_filename and os.path.exists(audio_filename):
            os.remove(audio_filename)


    def download_video_by_filename(
        self, 
        filename: Union[Path, str], 
        video_filename: Union[Path, str] = 'tmp_video.mp4',
        check_video: bool = True,
    ) -> Optional[Union[Path, str]]:
        """
        Функция для скачивания видео по его filename.

        Args:
            filename (Union[Path, str]): Filename для видео
            video_filename (Union[Path, str], optional): Путь к записываемому файлу. Defaults to 'tmp_video.mp4'.
            check_video (bool, optional): Необходимо ли открыть и считать 1 кадр для проверки корректности загрузки.
                Требует установки OpenCV. Defaults to True.

        Returns:
            Optional[Union[Path, str]]: Путь к записываемому файлу
        """
        fh_pass_correct = self.FH_PASS and self.FH_PASS != ''
        fh_user_correct = self.FH_USER and self.FH_USER != ''
        assert fh_pass_correct and fh_user_correct, 'FH_USER and FH_PASS needed for downloading'
        
        video_filename = str(video_filename)
        if os.path.exists(video_filename):
            logger.warning('Video exists. It won`t be downloaded')
            return video_filename 
        
        if check_video:
            try:
                import cv2
            except Exception as e:
                logger.warning('Cant import OpenCV. Video checking disabled')
                check_video = False
        
        response = requests.get(
            self.fh_url.format(filename),
            allow_redirects=False,
            auth=self.fh_auth
        )
        
        if response.status_code == 302:
            url = response.headers['Location']
        
        try:
            logger.info(f'Downloading from {url}')
            _ = urlretrieve(url, video_filename)
            if check_video:
                vid = cv2.VideoCapture(video_filename)
                success, image = vid.read()
                if not success:
                    os.remove(video_filename)
                    logger.warning('Cant open downloaded video. Skipping...')
            size = file_sizeof(os.path.getsize(video_filename))
            logger.success(f'Video with size {size} has been successfully loaded and opened')
            return video_filename
        except Exception as e:
            logger.warning(f'Cannot download from {url}. Skipping...')
        
        if video_filename and os.path.exists(video_filename):
            os.remove(video_filename)


    def download_audios_by_video_ids(
        self,
        video_ids: Union[list[str], str],
        path: Union[Path, str],
        height: int = 1000,
        audio_ext: str = 'wav',
        start: int = 0,
        time: Optional[int] = None
    ) -> list[Optional[Union[Path, str]]]: 
        """
        Функция для извлечения и сохранения в папку аудио из требуемых video_id.

        Args:
            video_ids (Union[list[str], str]): Видео, из которых необходимо извлечь аудио
            path (Union[Path, str]): Папка или файл сохранения аудио
            height (int, optional): Качество скачивания видео (его минимальный height). Defaults to 1000.
            audio_ext (str, optional): Расширение для сохраняемого аудио. Defaults to 'wav'.
            start (int): Начальная точка видео в секундах с которого извлекаем аудио. Default to 0.
            time (int, optional): Длительность извлечения считая от начала аудио.
                None для извлечения полной длительности. Defaults to None.

        Returns:
            list[Optional[Union[Path, str]]]: Список с извлеченными аудио
        """
        if isinstance(video_ids, str):
            video_ids = [video_ids]
        
        path = Path(path)
        
        if path.suffix != f'.{audio_ext}':
            os.makedirs(path, exist_ok=True)

        audios = []
        for video_id in video_ids:
            if path.suffix == f'.{audio_ext}':
                audio_filename = path
            else:
                audio_filename = path / f'{video_id}.{audio_ext}'
            audio = None
            try:
                rows = self.get_filenames_by_video_id(video_id)
                for filename in self._get_filename_by_quality(rows, height=height):
                    audio = self.download_audio_by_filename(filename, audio_filename, start=start, time=time)
                    if audio:
                        break
            except Exception as e:
                logger.exception(e)
            audios.append(audio)
        
        return audios


    def download_videos_by_video_ids(
        self,
        video_ids: Union[list[str], str],
        path: Union[Path, str],
        height: int = 1000,
        extract_audio: bool = False,
        audio_ext: str = 'wav',
    ) -> tuple[list[Optional[Union[Path, str]]], list[Optional[Union[Path, str]]]]:
        """
        Функция для скачивания видео в папку по требуемым video_id.

        Args:
            video_ids (Union[list[str], str]): Видео, которые необходимо скачать
            path (Union[Path, str]): Папка или файл сохранения видео
            height (int, optional): Качество скачивания видео (его минимальный height). Defaults to 1000.
            extract_audio (bool, optional): Нужно ли извлекать аудио отдельно. Defaults to False.
            audio_ext (str, optional): Расширение для сохраняемого аудио. Defaults to 'wav'.

        Returns:
            Optional[tuple[list, list], list]: Список с сохраненными видео
                или 2 списка (videos, audios) если необходимо было извлечение аудио
        """
        if isinstance(video_ids, str):
            video_ids = [video_ids]
        
        path = Path(path)
        
        if path.suffix != '.mp4':
            os.makedirs(path, exist_ok=True)
        
        audios = []
        videos = []
        for video_id in video_ids:
            if path.suffix == '.mp4':
                video_filename = path
            else:
                video_filename = path / f'{video_id}.mp4'
            video, audio = None, None
            try:
                rows = self.get_filenames_by_video_id(video_id)
                for filename in self._get_filename_by_quality(rows, height=height):
                    video = self.download_video_by_filename(filename, video_filename)
                    if video:
                        if extract_audio:
                            audio_filename = video_filename.with_suffix(f'.{audio_ext}')
                            audio = self.extract_audio_from_video(video, audio_filename, time=None)
                        break
            except Exception as e:
                logger.exception(e)
            videos.append(video)
            audios.append(audio)
        
        if extract_audio:
            return videos, audios
        
        return videos


    def get_video_infos_from_channel(
            self,
            author_id: Union[int, str],
            num: Optional[int] = None,
            columns: List[str] = ['id'],
            mode: str = 'num',
            query_id: int = 8785,
            max_age: Optional[int] = None,
            exclude_videos: Union[Set, List] = set()
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Функция для получения информации о нескольких видео.

        Args:
            author_id (Union[int, str]): Идентификатор пользователя (канала).
            num (int, optional): Необходимое количество видео или суммарное количество 
                часов видео в зависимости от режима работы. None для получения всех строк. Defaults to None.
            columns (List[str], optional): Необходимые колонки из бд. Defaults to ['id'].
            mode (str, optional): Режим работы [num, hours]. Определяет от чего будет зависеть количество выданных строк.
                Например с num=600 и mode='hours' будут выведены все строки до суммарной длительности 600.
                Например с num=400 и mode='num' будет выведено всего 400 строк. Defaults to 'num'.
            query_id (int, optional): Id запроса в redash. Defaults to 8785.
            max_age (int, optional): См описание RedashQuery. Defaults to None.
            exclude_videos (Union[Set, List], optional): Список идентификаторов видео, которые не нужно выводить. Defaults to set().

        Returns:
            Optional[List[Dict[str, Any]]]: Список словарей, 
                каждый словарь = отдельное видео с запрашиваемыми колонкамии из бд
        """
        assert mode in ['num', 'hours'], ' Режим работы может быть только "hours" или "num"'
        
        query = RedashQuery(query_id, max_age, author_id=str(author_id))
        videos = self.poll_job_result(query)
        videos = [video for video in videos if video['id'] not in exclude_videos]
        
        result = []
        for video in videos:
            result.append({column: video.get(column) for column in columns})
        
        if mode == 'num':
            logger.success(f'Got {len(result[:num])} videos from channel')
            return result[:num]
        elif mode == 'hours':
            idx = 0
            total_time = 0
            while (not num or (total_time / 3600) < num) and idx < len(videos):
                total_time += videos[idx].get('duration', 0) / 1000
                idx += 1
            logger.success(f'Got {total_time / 3600:.2f} hours videos from channel')
            return result[:idx]
        else:
            logger.error(f"Mode must be 'num' or 'hours' but got {mode}")

    
    def get_video_infos(
        self,
        video_ids: Union[list, str],
        columns: list[str] = ['id'],
        query_id: int = 9605,
        max_age: Optional[int] = None
    ) -> Optional[dict[dict]]:
        """
        Функция для получения информации о видео.

        Args:
            video_ids (Union[list, str]): Id необходимых видео
            columns (list[str]): Необходимые колонки с информацией из БД
            query_id (int, optional): Id запроса в redash. Defaults to 9605.
            max_age (int, optional): См описание RedashQuery. Defaults to None.

        Returns:
            Optional[dict[dict]]: Словарь с запрашиваемой информацией о видео
        """
        if isinstance(video_ids, str):
            video_ids = [video_ids]
        
        result = {}
        query = RedashQuery(query_id, max_age, video_ids="','".join(video_ids))
        videos = self.poll_job_result(query)
        for video in videos:
            video_dict = {column: video.get(column) for column in columns}
            result[video.get('id')] = video_dict
        return result