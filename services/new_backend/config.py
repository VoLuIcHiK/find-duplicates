import os
from inspect import isfunction
from typing import Callable


class Settings:
    PROJECT_NAME:str = "Awesome app"
    PROJECT_VERSION: str = "0.0.1"
    CELERY_BROKER_URL: str = "amqp://localhost:5672"
    CELERY_RESULT_BACKEND: str = 'db+sqlite:///results.sqlite'

    @staticmethod
    def special_values():
        return list(filter(lambda x: not(str(x).startswith('__') and str(x).endswith('__'))
                                     and not isfunction(getattr(Settings, x)), dir(Settings)))

    @staticmethod
    def override_system(s: 'Settings'):
        for key in s.special_values():
            os_val = os.environ.get(key, None)
            if os_val:
                setattr(Settings, key, os_val)
        return Settings

settings: Settings = Settings()
Settings.override_system(settings)