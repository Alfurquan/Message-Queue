import socket
import threading
import logging
from persistence.file_storage import FileStorage
from persistence.persistence import Persistence
from server.json_socket import JsonSocket
from server.request_handler import RequestHandler
from core.broker import Broker

class Server:
    def __init__(self, logger, host='localhost', port=5000):
        self.logger = logger
        self.logger.setLevel(logging.INFO)
        self.handler = RequestHandler(Broker(logger = logger, persistence = Persistence(FileStorage())), self.logger)
        self.host = host
        self.port = port

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.host, self.port))
            server_socket.listen()
            self.logger.info(f"Server listening on {self.host}:{self.port}")

            while True:
                client_conn, client_addr = server_socket.accept()
                self.logger.info(f"Accepted connection from {client_addr}")
                thread = threading.Thread(target=self.handle_client, args=(client_conn, client_addr), daemon=True)
                thread.start()

    def handle_client(self, client_conn, client_addr):
        json_socket = JsonSocket(client_conn)
        try:
            while True:
                try:
                    request = json_socket.receive()
                    if request is None:
                        self.logger.info(f"Client {client_addr} disconnected")
                        break

                    self.logger.debug(f"Received request: {request}")
                    response = self.handler.handle(request)
                    json_socket.send(response)

                except ValueError as e:
                    self.logger.warning(f"Invalid request from {client_addr}: {e}")
                    json_socket.send({"error": str(e)})

                except Exception as e:
                    self.logger.exception(f"Unhandled error from {client_addr}: {e}")
                    json_socket.send({"error": "Server error"})
                    break
        finally:
            client_conn.close()
            self.logger.info(f"Closed connection from {client_addr}")
