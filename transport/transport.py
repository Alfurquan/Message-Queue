from abc import ABC, abstractmethod

class Transport(ABC):
    @abstractmethod
    def send(self, data: dict) -> dict:
        pass

    @abstractmethod
    def close(self):
        pass