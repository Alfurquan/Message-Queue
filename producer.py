import socket
import json

from logger.console_logger import ConsoleLogger
from config.config import get_port_config
from interfaces.producer import Producer

def main():
    port_config = get_port_config()
    producer = Producer(port=port_config['port'], host=port_config['host'])
    producer.connect()
    logger = ConsoleLogger(name="producer").get_logger()
    
    while True:
        topics = input("Enter topics (comma separated) to publish to: ")
        msg = input("Enter message: ")
        response = producer.publish(topics, msg) 
        if 'error' in response:
            logger.error(f"Error publishing message: {response['error']}")
        else:
            logger.info(f"Message published to topics: {topics}")

        cont = input("Do you want to continue? (y/n): ").strip().lower()
        if cont != 'y':
            break

if __name__ == "__main__":
    main()
