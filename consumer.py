import time

from logger.console_logger import ConsoleLogger
from interfaces.consumer import Consumer


def main():
    consumer = Consumer.connect()
    logger = ConsoleLogger(name="consumer").get_logger()
    topics = input("Enter topics (comma separated) to subscribe to: ")

    response = consumer.subscribe(topics=topics)

    if 'error' in response:
        logger.error(f"Error subscribing to topics: {response['error']}")
    else:
        logger.info(f"Subscribed to topics: {topics}")

    while True:
        for topic in topics.split(','):
            response = consumer.consume(topic=topic)
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
