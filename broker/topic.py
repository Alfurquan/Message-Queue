from queue import Queue
from typing import Dict
from .message import Message
from logger.logger import Logger

class Topic:
    """
    Represents a topic in a message broker.
    """
    def __init__(self, name: str, logger: Logger):
        self.name = name
        self.queue = Queue()
        self.logger = logger
    
    def publish(self, message: Message):
        """
        Publishes a message to the topic.
        """
        self.queue.put(message)
        self.logger.info(f"Message {message.id} published to topic {self.name}.")
    
    def poll(self) -> Message:
        """
        Retrieves and removes a message from the topic.
        """
        if not self.queue.empty():
            message = self.queue.get()
            self.logger.info(f"Message {message.id} polled from topic {self.name}.")
            return message
        else:
            self.logger.info(f"No messages in topic {self.name}.")
            return None

