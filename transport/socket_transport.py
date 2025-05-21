import socket
import json
from transport.transport import Transport

class SocketTransport(Transport):
    def __init__(self, host='localhost', port=5000):
        self.host = host
        self.port = port
        self.sock = socket.create_connection((self.host, self.port))

    def send(self, data: dict) -> dict:
        self.sock.sendall(json.dumps(data).encode('utf-8'))
        response = self.sock.recv(4096)
        return json.loads(response.decode('utf-8'))

    def close(self):
        self.sock.close()