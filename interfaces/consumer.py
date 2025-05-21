import json
import socket


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
        return json.loads(data.decode('utf-8'))
        

    def subscribe(self, topics, consumer_id):
        request = {'action': 'subscribe', 'topics': topics, 'consumer_id': consumer_id}
        self.sock.sendall(json.dumps(request).encode('utf-8'))
        data = self.sock.recv(4096)
        if not data:
            return None
        return json.loads(data.decode('utf-8'))