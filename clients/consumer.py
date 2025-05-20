from broker.broker import Broker

class Consumer:
    """
    A class to represent a consumer.
    It retrieves messages from a broker.
    """
    def __init__(self, broker: Broker):
        self.broker = broker

    def poll(self, topic: str):
        """
        Polls a message from a topic.
        param topic: The name of the topic to poll from.
        return: The polled message, or None if no messages are available.
        """
        return self.broker.poll(topic)
