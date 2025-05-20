import json
import socket

class JsonSocket:
    def __init__(self, conn: socket.socket):
        self.conn = conn

    def send(self, obj: dict):
        try:
            message = json.dumps(obj).encode('utf-8')
            self.conn.sendall(message)
        except Exception as e:
            raise IOError(f"Failed to send data: {e}")

    def receive(self) -> dict:
        try:
            data = self.conn.recv(4096)
            if not data:
                return None
            return json.loads(data.decode('utf-8'))
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON received")
        except Exception as e:
            raise IOError(f"Failed to receive data: {e}")