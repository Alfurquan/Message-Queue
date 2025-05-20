import socket
import pickle
import time

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

    def consume(self, topic):
        request = {'action': 'poll', 'topic': topic}
        self.sock.sendall(pickle.dumps(request))
        response = pickle.loads(self.sock.recv(4096))
        return response.get('message')

def main():
    port_config = get_port_config()
    consumer = Consumer(host=port_config['host'], port=port_config['port'])
    consumer.connect()
    logger = ConsoleLogger(name="consumer").get_logger()
    topic = input("Enter topic to consume from: ")
    while True:
        msg = consumer.consume(topic)
        if msg:
            logger.info(f"[RECEIVED] {msg}")
        else:
            logger.info("No message. Retrying...")
        time.sleep(2)

if __name__ == "__main__":
    main()
