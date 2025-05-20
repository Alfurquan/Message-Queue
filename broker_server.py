import socket
import threading
import pickle
from threading import Lock
from broker.broker import Broker
from logger.console_logger import ConsoleLogger

class BrokerServer:
    def __init__(self, host: str, port: int, broker: Broker):
        self.host = host
        self.port = port
        self.broker = broker
        self.lock = Lock()
        self.logger = broker.logger

    def handle_client(self, conn, addr):
        self.logger.info(f"Connection from {addr}")
        with conn:
            while True:
                try:
                    data = conn.recv(4096)
                    if not data:
                        self.logger.info(f"Connection closed by {addr}")
                        break

                    request = pickle.loads(data)
                    action = request.get('action')
                    topic = request.get('topic')

                    if action == 'publish':
                        message = request.get('message')
                        with self.lock:
                            self.broker.publish(topic, message)
                        conn.sendall(pickle.dumps({'status': 'ok'}))

                    elif action == 'poll':
                        with self.lock:
                            msg = self.broker.poll(topic)
                        payload = msg.payload if msg else None
                        conn.sendall(pickle.dumps({'message': payload}))

                    else:
                        self.logger.warning(f"Unknown action '{action}' from {addr}")
                        conn.sendall(pickle.dumps({'error': 'Unknown action'}))

                except Exception as e:
                    self.logger.error(f"Error handling client {addr}: {e}")
                    break

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            self.logger.info(f"Broker server listening on {self.host}:{self.port}...")
            while True:
                conn, addr = s.accept()
                thread = threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True)
                thread.start()


if __name__ == "__main__":
    logger = ConsoleLogger(name="broker-server").get_logger()

    # Create broker instance
    broker = Broker(logger=logger)

    # Start broker server
    server = BrokerServer(host='localhost', port=5000, broker=broker)
    server.start()
