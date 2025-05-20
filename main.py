from broker.broker import Broker
from clients.consumer import Consumer
from clients.producer import Producer
from logger.console_logger import ConsoleLogger


def main():
    """
    Entry point for the application.
    """
    logger = ConsoleLogger(name="MyApp").get_logger()
    broker = Broker(logger=logger)
    producer = Producer(broker)
    consumer = Consumer(broker)

    topic = "test-topic"

    # Publish messages
    producer.send(topic, "Hello 1")
    producer.send(topic, "Hello 2")

    # Poll messages
    print(consumer.poll(topic))
    print(consumer.poll(topic))
    print(consumer.poll(topic))
    

if __name__ == "__main__":
    main()

