import json
from threading import Lock
from typing import List, Dict, Optional
from core.message import Message
from logger.logger import Logger
from persistence.persistence import Persistence

class Topic:
    """
    Represents a topic in a message broker.
    """
    def __init__(self, name: str, logger: Logger, persistence: Persistence):
        self.name = name
        self.messages: List[Message] = []
        self.group_offsets: Dict[str, Dict[str, int]] = {}
        self.lock = Lock()
        self.logger = logger
        self.persistence = persistence
        self._load_state()

    def _load_state(self):
        """
        Loads the state of the topic from persistence.
        This includes loading messages and consumer group offsets.
        """
        
        raw_msgs = self.persistence.load_messages(self.name)
        if raw_msgs:
            self.messages = [Message.from_dict(msg) for msg in raw_msgs]
            self.logger.info(f"Loaded {len(self.messages)} messages for topic {self.name} from persistence.")

        raw_offsets = self.persistence.load_offsets(self.name)
        if raw_offsets:
            self.group_offsets = raw_offsets
            self.logger.info(f"Loaded consumer group offsets for topic {self.name} from persistence.")

    def _save_offsets(self):
        """
        Save only offsets of the current topic.
        """
        all_offsets = self.persistence.load_offsets(self.name)
        all_offsets = self.group_offsets
        self.persistence.save_offsets(self.name, all_offsets)

    def _save_messages(self):
        """
        Save the messages of this topic.
        """
        self.persistence.save_messages(self.name, self.messages)

    def publish(self, message: Message):
        """
        Publishes a message to the topic.
        """
        with self.lock:
            self.messages.append(message)
            self._save_messages()
            self.logger.info(f"Message {message.id} published to topic {self.name}.")

    def poll(self, group: str, consumer_id: str) -> Optional[Message]:
        """
        Retrieves the next message for the consumer in the group.
        """
        with self.lock:
            self.group_offsets.setdefault(group, {})
            offset = self.group_offsets[group].get(consumer_id, 0)

            if offset >= len(self.messages):
                return None

            msg = self.messages[offset]
            self.group_offsets[group][consumer_id] = offset + 1
            self._save_offsets()

            self.logger.info(f"Consumer {consumer_id} polled message {msg.id} from topic {self.name} (group: {group})")
            return msg