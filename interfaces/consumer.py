# consumer.py
from config.config import get_port_config
from transport.socket_transport import SocketTransport


class Consumer:
    def __init__(self, *args, **kwargs):
        raise RuntimeError("Use `Consumer.connect()` to create an instance.")

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

    def subscribe(self, topics, consumer_id):
        request = {'action': 'subscribe', 'topics': topics, 'consumer_id': consumer_id}
        return self.transport.send(request)

    def consume(self, topic, consumer_id):
        request = {'action': 'poll', 'topic': topic, 'consumer_id': consumer_id}
        return self.transport.send(request)

    def disconnect(self):
        self.transport.close()
