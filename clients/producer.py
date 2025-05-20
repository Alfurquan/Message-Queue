from broker.broker import Broker

class Producer:
    """
    A class to represent a producer.
    It sends messages to a broker.
    """
    def __init__(self, broker: Broker):
        self.broker = broker

    def send(self, topic: str, message: str):
        """
        Sends a message to a topic.
        param topic: The name of the topic to send to.
        param message: The message to send.
        """
        self.broker.publish(topic, message)