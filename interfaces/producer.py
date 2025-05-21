from config.config import get_port_config
from transport.socket_transport import SocketTransport

class Producer:
    def __init__(self, *args, **kwargs):
        raise RuntimeError("Use `Producer.connect()` to create an instance.")

    @classmethod
    def _create(cls, transport):
        self = object.__new__(cls)
        self.transport = transport
        return self

    @classmethod
    def connect(cls):
        config = get_port_config()
        host = config['host']
        port = config['port']
        transport = SocketTransport(host, port)
        return cls._create(transport)

    def publish(self, topics, message):
        request = {'action': 'publish', 'topics': topics, 'message': message}
        return self.transport.send(request)

    def disconnect(self):
        self.transport.close()
