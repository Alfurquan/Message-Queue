# core/storage/base.py
from abc import ABC, abstractmethod
from typing import Any, List

class Storage(ABC):
    @abstractmethod
    def save(self, key: str, data: Any) -> None:
        pass

    @abstractmethod
    def load(self, key: str) -> Any:
        pass
