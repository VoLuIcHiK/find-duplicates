import multiprocessing
import threading
import time
from typing import Callable, Iterable, Mapping

from loguru import logger


class FlowNotStartingException(Exception):
    """
    Ошибка, которая вызывается, если поток или процесс не запускается.
    """
    pass


class FlowControllerHasTooManyErrors(Exception):
    """
    Ошибка, которая вызывается, если количество ошибок превышает максимальное количество ошибок.
    """
    pass


Flow = multiprocessing.Process | threading.Thread


class FlowInfo:
    """
    Класс для хранения информации о функции внутри потока (thread) или процесса.
    """
    def __init__(self, num_flow: int, name: str, *, func: Callable, args: Iterable, kwargs: Mapping):
        self.num_flow = num_flow
        self.name = name
        self.func = func
        self.args = args
        self.kwargs = kwargs


class FlowController:
    """
    Написанный класс для управления потоками и процессами.
    """
    def __init__(self, max_fail_count=10):
        """
        :param max_fail_count: Максимальное количество ошибок, после которого потоки будут перезапущены.
        """
        self._run_thread: threading.Thread | None = None
        self._lock = multiprocessing.RLock()
        self._flows_num = 0
        self._flows: dict[FlowInfo, Flow] = dict()
        self._max_fail_count = max_fail_count

    @property
    def flows(self) -> dict[FlowInfo, Flow]:
        """
        :return: Возвращает словарь с потоками и процессами.
        """
        return self._flows.copy()

    @logger.catch(reraise=True)
    def loop_check(self):
        """
        Проверяет потоки и процессы на состояния работы.
        Если поток или процесс завершился, то он перезапускается.
        Если количество ошибок превышает максимальное количество ошибок(max_fail_count),
        то будет вызвана ошибка контроллера.
        """
        total_fail_count = 0
        while True:
            for flowInfo, flow in self._flows.copy().items():
                if not flow.is_alive():
                    logger.info(f"Found dead Flow #{flowInfo.num_flow} \"{flowInfo.name}\"")
                    total_fail_count += 1
                    if total_fail_count >= self._max_fail_count:
                        if self._max_fail_count == 0:
                            continue
                        raise FlowControllerHasTooManyErrors(f"ThreadControllers has too many fails! Stop")
                    run_count = 0
                    while True:
                        logger.info(f"Restarting Flow \"{flowInfo.name}\"")
                        run_count += 1
                        if run_count > 3:
                            logger.error(f"Flow {flowInfo.num_flow} failed to start 3 times!")
                            raise FlowNotStartingException(
                                f"Flow {flowInfo.num_flow} failed to start 3 times!")
                        try:
                            f = self._flows.get(flowInfo)
                            self._try_force_close(f)
                            if type(f) is multiprocessing.Process:
                                self.add_process(flowInfo.func, flowInfo.args, flowInfo.kwargs, name=flowInfo.name)
                            elif type(f) is threading.Thread:
                                f.join()
                                self.add_thread(flowInfo.func, flowInfo.args, flowInfo.kwargs, name=flowInfo.name)
                            self._flows.pop(flowInfo)
                        except AssertionError as e:
                            # Logger.logger.exception(e)
                            raise
                        except Exception as e:
                            logger.exception(e)
                            continue
                        break
            time.sleep(1)

    def run_infinite(self, block=True):
        """
        Запускает поток, который будет проверять потоки и процессы на состояния работы.
        :param block: Если True, то метод будет ждать завершения потока контроля.
        """
        self._run_thread = threading.Thread(target=self.loop_check, daemon=True)
        self._run_thread.start()
        if block:
            self.join()

    def join(self):
        """
        Ожидание завершения потока контроля.
        """
        while True:
            if not self._run_thread.is_alive():
                break
            self._run_thread.join(1)

    @staticmethod
    def _try_force_close(f: Flow):
        """
        Пытается принудительно завершить процесс или поток.
        :param f: Поток или процесс
        """
        if type(f) is multiprocessing.Process:
            FlowController.kill_process(f)

    @staticmethod
    def kill_process(p: multiprocessing.Process):
        """
        Принудительно завершает процесс.
        :param p: Процесс
        """
        try:
            if p.is_alive():
                p.terminate()
                p.join()
        except Exception as _:
            pass

    def add_process(self, target: Callable, args: Iterable = None, kwargs: Mapping = None, name="Unnamed"):
        """
        Добавляет функцию как процесс в контроллер.
        :param target: Функция
        :param args: Аргументы к функции
        :param kwargs: Ключевые аргументы к функции
        :param name: Имя процесса
        """
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = dict()

        self._flows_num += 1
        funcData = FlowInfo(self._flows_num, func=target, args=args, kwargs=kwargs, name=name)
        process = multiprocessing.Process(target=funcData.func, args=args, kwargs=kwargs, daemon=True)

        process.start()
        self._flows[funcData] = process

        logger.info(f"Process #{funcData.num_flow} \"{funcData.name}\" started!")

    def add_thread(self, target: Callable, args: Iterable = None, kwargs: Mapping = None, name="Unnamed"):
        """
        Добавляет функцию как поток (thread) в контроллер.
        :param target: Функция
        :param args: Аргументы к функции
        :param kwargs: Ключевые аргументы к функции
        :param name: Имя потока
        """
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = dict()

        self._flows_num += 1
        funcData = FlowInfo(self._flows_num, func=target, args=args, kwargs=kwargs, name=name)
        thread = threading.Thread(target=funcData.func, args=args, kwargs=kwargs, daemon=True)

        thread.start()
        self._flows[funcData] = thread

        logger.info(f"Thread #{funcData.num_flow} \"{funcData.name}\" started!")
