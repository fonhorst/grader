from typing import List, Optional
from pydantic import BaseSettings


class AppSettings(BaseSettings):
    """Общие настройки сервиса"""

    # в будущем тут скорее всего будет что-то связанное с проектом
    APP_NAME = "GEOWSM"


class GeoWSMSettings:
    """Класс-синглтон с параметрами сервиса"""

    __instance: Optional["GeoWSMSettings"] = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = super(GeoWSMSettings, cls).__new__(cls)
        return cls.__instance

    def __init__(self):
        """Инициализация подклассов настроек"""
        self.app = AppSettings()


settings = GeoWSMSettings()