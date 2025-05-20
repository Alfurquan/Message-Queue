import socket
import threading
import json
from threading import Lock
from broker.broker import Broker
from logger.console_logger import ConsoleLogger


class Server:
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

                    request = json.loads(data)
                    response = self.process_request(request)
                    conn.sendall(json.dumps(response).encode('utf-8'))

                except Exception as e:
                    self.logger.error(f"Error handling client {addr}: {e}")
                    error_response = {'error': 'Internal server error'}
                    conn.sendall(json.dumps(error_response).encode('utf-8'))

    def process_request(self, request):
        action = request.get('action')

        if action == 'publish':
            return self._handle_publish(request)
        elif action == 'subscribe':
            return self._handle_subscribe(request)
        elif action == 'poll':
            return self._handle_poll(request)
        else:
            self.logger.warning(f"Unknown action '{action}'")
            return {'error': 'Unknown action'}

    def _handle_publish(self, request):
        topics = request.get('topics', '')
        message = request.get('message')
        if not topics or not message:
            return {'error': 'Missing topics or message'}

        topic_list = [t.strip() for t in topics.split(',') if t.strip()]
        with self.lock:
            for topic in topic_list:
                self.broker.publish(topic, message)

        return {'status': 'ok'}

    def _handle_subscribe(self, request):
        topics = request.get('topics', '')
        consumer_id = request.get('consumer_id')
        if not topics or not consumer_id:
            return {'error': 'Missing topics or consumer_id'}

        topic_list = [t.strip() for t in topics.split(',') if t.strip()]
        with self.lock:
            for topic in topic_list:
                if not self.broker.has_topic(topic):
                    return {'error': f"Topic '{topic}' not found"}
                self.broker.subscribe(consumer_id, topic)

        return {'status': 'ok'}

    def _handle_poll(self, request):
        topic = request.get('topic')
        consumer_id = request.get('consumer_id')
        if not topic or not consumer_id:
            return {'error': 'Missing topic or consumer_id'}

        with self.lock:
            if not self.broker.has_topic(topic):
                return {'error': f"Topic '{topic}' not found"}

            msg = self.broker.poll(topic, consumer_id)
            payload = msg.payload if msg else None

        return {'message': payload}

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            self.logger.info(f"Server listening on {self.host}:{self.port}...")
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    logger = ConsoleLogger(name="broker-server").get_logger()
    broker = Broker(logger=logger)
    server = Server(host='localhost', port=5000, broker=broker)
    server.start()
