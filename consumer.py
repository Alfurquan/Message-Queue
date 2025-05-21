import socket
import json
import time
import uuid

from logger.console_logger import ConsoleLogger
from config.config import get_port_config
from interfaces.consumer import Consumer


def main():
    port_config = get_port_config()
    consumer = Consumer(host=port_config['host'], port=port_config['port'])
    consumer.connect()
    logger = ConsoleLogger(name="consumer").get_logger()
    topics = input("Enter topics (comma separated) to subscribe to: ")
    consumer_id = str(uuid.uuid4())

    response = consumer.subscribe(topics=topics, consumer_id=consumer_id)

    if 'error' in response:
        logger.error(f"Error subscribing to topics: {response['error']}")
    else:
        logger.info(f"Subscribed to topics: {topics}")

    while True:
        for topic in topics.split(','):
            response = consumer.consume(topic=topic, consumer_id=consumer_id)
            if 'error' in response:
                logger.error(f"Error consuming message: {response['error']}")
                continue
            msg = response.get('message')
            if msg:
                logger.info(f"[RECEIVED] {msg}")
            else:
                logger.info(f"[NO MESSAGE] No message in topic {topic}.")

        time.sleep(10)
           
if __name__ == "__main__":
    main()
