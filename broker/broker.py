from typing import Dict
from broker.topic import Topic
from broker.message import Message
from logger.logger import Logger

class Broker:
    """
    A class to represent a broker.
    It manages topics and provides methods to publish and poll messages.
    """
    def __init__(self, logger: Logger):
        self.topics: Dict[str, Topic] = {}
        self.logger = logger

    def create_topic(self, name: str) -> Topic:
        """
        Creates a new topic if it does not already exist.
        param name: The name of the topic to create.
        return: The created topic.
        """
        if name not in self.topics:
            topic = Topic(name, self.logger)
            self.topics[name] = topic
            self.logger.info(f"Topic {name} created.")
            return topic
        else:
            self.logger.warning(f"Topic {name} already exists.")
            return self.topics[name]

    def publish(self, topic_name: str, payload: str):
        """
        Publishes a message to a topic.
        param topic_name: The name of the topic to publish to.
        param payload: The message payload to publish.
        """
        if topic_name not in self.topics:
            self.create_topic(topic_name)
        msg = Message.create(payload)
        self.topics[topic_name].publish(msg)
        self.logger.info(f"Published message to topic {topic_name}: {msg}")

    def poll(self, topic_name: str) -> Message:
        """
        Polls a message from a topic.
        param topic_name: The name of the topic to poll from.
        return: The polled message, or None if no messages are available.
        """
        if topic_name not in self.topics:
            self.logger.error(f"Topic not found: {topic_name}")
            return None
        msg = self.topics[topic_name].poll()
        if msg:
            self.logger.info(f"Polled message from {topic_name}: {msg}")
        return msg