from typing import Dict, List
from core.message import Message
from persistence.storage import Storage

class Persistence:
    def __init__(self, storage: Storage):
        self.storage = storage

    def save_messages(self, topic: str, messages: List[Message]):
        self.storage.save(f"{topic}_messages", [m.to_dict() for m in messages])

    def load_messages(self, topic: str) -> List[Message]:
        return self.storage.load(f"{topic}_messages") or []

    def save_offsets(self, topic: str, offsets: Dict[str, Dict[str, int]]):
        self.storage.save(f"{topic}_offsets", offsets)

    def load_offsets(self, topic: str) -> Dict[str, Dict[str, int]]:
        return self.storage.load(f"{topic}_offsets") or {}

    def save_subscribers(self, topic: str, subscribers: List[str]):
        self.storage.save(f"{topic}_subscribers", subscribers)

    def load_subscribers(self, topic: str) -> List[str]:
        return self.storage.load(f"{topic}_subscribers") or []

    def save_consumer_groups(self, topic: str, groups: List[str]):
        self.storage.save(f"{topic}_consumer_groups", groups)

    def load_consumer_groups(self, topic: str) -> List[str]:
        return self.storage.load(f"{topic}_consumer_groups") or []

    def save_topics(self, topics: List[str]):
        self.storage.save("topics", topics)

    def load_topics(self) -> List[str]:
        return self.storage.load("topics") or []