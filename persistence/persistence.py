import os
from typing import Dict, List
from core.message import Message
from persistence.storage import Storage

class Persistence:
    def __init__(self, storage: Storage):
        self.storage = storage

    def _path(self, topic: str, file: str) -> str:
        return os.path.join(topic, file)

    def save_messages(self, topic: str, messages: List[Message]):
        self.storage.save(self._path(topic, "messages"), [m.to_dict() for m in messages])

    def load_messages(self, topic: str) -> List[Message]:
        return self.storage.load(self._path(topic, "messages")) or []

    def save_offsets(self, topic: str, offsets: Dict[str, Dict[str, int]]):
        self.storage.save(self._path(topic, "offsets"), offsets)

    def load_offsets(self, topic: str) -> Dict[str, Dict[str, int]]:
        return self.storage.load(self._path(topic, "offsets")) or {}

    def save_subscribers(self, topic: str, subscribers: List[str]):
        self.storage.save(self._path(topic, "subscribers"), subscribers)

    def load_subscribers(self, topic: str) -> List[str]:
        return self.storage.load(self._path(topic, "subscribers")) or []

    def save_consumer_groups(self, topic: str, groups: List[str]):
        self.storage.save(self._path(topic, "consumer_groups"), groups)

    def load_consumer_groups(self, topic: str) -> List[str]:
        return self.storage.load(self._path(topic, "consumer_groups")) or []

    def save_topics(self, topics: List[str]):
        self.storage.save("topics", topics)

    def load_topics(self) -> List[str]:
        return self.storage.load("topics") or []
