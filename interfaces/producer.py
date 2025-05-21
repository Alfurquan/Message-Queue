import socket
import json

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
        return json.loads(data.decode('utf-8'))