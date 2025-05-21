from typing import Dict, List, Optional
from core.topic import Topic
from core.message import Message
from logger.logger import Logger
from persistence.persistence import Persistence

class Broker:
    """
    A class to represent a broker.
    It manages topics and provides methods to publish and poll messages.
    """
    def __init__(self, logger: Logger, persistence: Persistence):
        """
        Initializes the Broker with a logger and persistence layer.
        Loads existing state from persistence.
        
        param logger: Logger instance for logging.
        param persistence: Persistence instance for loading and saving state.
        """
        self.topics: Dict[str, Topic] = {}
        self.logger = logger
        self.persistence = persistence
        self.subscribers: Dict[str, List[str]] = {}
        self.consumer_groups: Dict[str, str] = {}

        self.load_state()

    def load_state(self):
        """
        Loads the entire state from persistence: topics, subscribers, consumer groups.
        """
        topic_names = self.persistence.load_topics()

        for name in topic_names:
            topic = Topic(name, self.logger, self.persistence)
            self.topics[name] = topic

            # Load subscribers for this topic
            topic_subscribers = self.persistence.load_subscribers(name)
            for consumer_id in topic_subscribers:
                self.subscribers.setdefault(consumer_id, [])
                if name not in self.subscribers[consumer_id]:
                    self.subscribers[consumer_id].append(name)

            # Load consumer groups for this topic (dict: consumer_id -> group)
            topic_consumer_groups = self.persistence.load_consumer_groups(name) or {}
            self.consumer_groups.update(topic_consumer_groups)

        self.logger.info(f"Loaded state with topics: {list(self.topics.keys())}")

    def save_state(self):
        """
        Saves the entire broker state to persistence: topics, subscribers, consumer groups.
        """
        # Save topic list
        self.persistence.save_topics(list(self.topics.keys()))

        # Save subscribers and consumer groups per topic
        for topic_name in self.topics.keys():
            # Gather all subscribers subscribed to this topic
            topic_subscribers = [cid for cid, topics in self.subscribers.items() if topic_name in topics]

            # Gather consumer groups for subscribers of this topic
            topic_consumer_groups = {cid: self.consumer_groups[cid] for cid in topic_subscribers if cid in self.consumer_groups}

            self.persistence.save_subscribers(topic_name, topic_subscribers)
            self.persistence.save_consumer_groups(topic_name, topic_consumer_groups)

        self.logger.info("Saved broker state.")

    def create_topic(self, name: str) -> Topic:
        """
        Creates a new topic if it does not already exist.
        param name: The name of the topic to create.
        return: The created topic.
        """
        if name not in self.topics:
            topic = Topic(name, self.logger, self.persistence)
            self.topics[name] = topic
            self.logger.info(f"Topic {name} created.")
            self.save_state()  # Save after topic creation
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

    def poll(self, topic_name: str, consumer_id: str) -> Optional[Message]:
        """
        Polls a message from a topic.
        param topic_name: The name of the topic to poll from.
        param consumer_id: The id of the consumer polling the message.
        return: The polled message, or None if no messages are available.
        """
        if topic_name not in self.topics:
            self.logger.error(f"Topic not found: {topic_name}")
            return None
        
        group = self.consumer_groups.get(consumer_id)
        if not group:
            self.logger.error(f"Consumer {consumer_id} is not part of any group.")
            return None
        
        msg = self.topics[topic_name].poll(group, consumer_id)
        if msg:
            self.logger.info(f"Polled message from {topic_name} for consumer {consumer_id}: {msg}")
        return msg
    
    def subscribe(self, consumer_id: str, topic_name: str, group: str) -> Optional[str]:
        """
        Subscribes a consumer to a topic.
        param consumer_id: The id of the consumer to subscribe.
        param topic_name: The name of the topic to subscribe to.
        param group: The consumer group name.
        return: The name of the topic.
        """
        if topic_name not in self.topics:
            self.logger.error(f"Topic not found: {topic_name}")
            return None

        self.subscribers.setdefault(consumer_id, [])
        if topic_name not in self.subscribers[consumer_id]:
            self.subscribers[consumer_id].append(topic_name)

        self.consumer_groups[consumer_id] = group

        self.logger.info(f"Consumer {consumer_id} subscribed to topic {topic_name} in group {group}.")

        self.save_state()

        return topic_name

    def has_topic(self, topic_name: str) -> bool:
        """
        Checks if a topic exists.
        param topic_name: The name of the topic to check.
        return: True if the topic exists, False otherwise.
        """
        return topic_name in self.topics