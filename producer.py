import socket
import json

from logger.console_logger import ConsoleLogger
from config.config import get_port_config

class Producer:
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port

    def connect(self):
        self.sock = socket.create_connection((self.host, self.port))

    def disconnect(self):
        self.sock.close()

    def publish(self, topics, message):
        request = {'action': 'publish', 'topics': topics, 'message': message}
        self.sock.sendall(json.dumps(request).encode('utf-8'))
        data = self.sock.recv(4096)
        if not data:
            return None
        response = json.loads(data.decode('utf-8'))
        return response.get('status')

def main():
    port_config = get_port_config()
    producer = Producer(port=port_config['port'], host=port_config['host'])
    producer.connect()
    
    
    while True:
        topics = input("Enter topics (comma separated) to publish to: ")
        msg = input("Enter message: ")
        producer.publish(topics, msg) 
        cont = input("Do you want to continue? (y/n): ").strip().lower()
        if cont != 'y':
            break

if __name__ == "__main__":
    main()
