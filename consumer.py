import socket
import json
import time
import uuid

from logger.console_logger import ConsoleLogger
from config.config import get_port_config

class Consumer:
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port

    def connect(self):
        self.sock = socket.create_connection((self.host, self.port))

    def disconnect(self):
        self.sock.close()

    def consume(self, topic, consumer_id):
        request = {'action': 'poll', 'topic': topic, 'consumer_id': consumer_id}
        self.sock.sendall(json.dumps(request).encode('utf-8'))
        data = self.sock.recv(4096)
        if not data:
            return None
        response = json.loads(data.decode('utf-8'))
        return response.get('message')

    def subscribe(self, topics, consumer_id):
        request = {'action': 'subscribe', 'topics': topics, 'consumer_id': consumer_id}
        self.sock.sendall(json.dumps(request).encode('utf-8'))
        data = self.sock.recv(4096)
        if not data:
            return None
        response = json.loads(data.decode('utf-8'))
        return response.get('status')

def main():
    port_config = get_port_config()
    consumer = Consumer(host=port_config['host'], port=port_config['port'])
    consumer.connect()
    logger = ConsoleLogger(name="consumer").get_logger()
    topics = input("Enter topics (comma separated) to subscribe to: ")
    consumer_id = str(uuid.uuid4())

    consumer.subscribe(topics=topics, consumer_id=consumer_id)
    while True:
        for topic in topics.split(','):
            logger.info(f"Topic {topic}.")
            msg = consumer.consume(topic=topic, consumer_id=consumer_id)
            if msg:
                logger.info(f"[RECEIVED] {msg}")
            else:
                logger.info(f"[NO MESSAGE] No message in topic {topic}.")

        time.sleep(10)
           
if __name__ == "__main__":
    main()
