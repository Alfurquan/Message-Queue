# consumer.py
import uuid

from config.config import get_port_config
from transport.socket_transport import SocketTransport

class Consumer:
    def __init__(self, *args, **kwargs):
        raise RuntimeError("Use `Consumer.connect()` to create an instance.")

    @classmethod
    def _create(cls, transport, consumer_id):
        self = object.__new__(cls)
        self.transport = transport
        self.consumer_id = consumer_id
        return self

    @classmethod
    def connect(cls):
        port_config = get_port_config()
        host = port_config['host']
        port = port_config['port']
        
        transport = SocketTransport(host, port)
        consumer_id = str(uuid.uuid4())
        return cls._create(transport, consumer_id)

    def subscribe(self, topics):
        request = {'action': 'subscribe', 'topics': topics, 'consumer_id': self.consumer_id}
        return self.transport.send(request)

    def consume(self, topic):
        request = {'action': 'poll', 'topic': topic, 'consumer_id': self.consumer_id}
        return self.transport.send(request)

    def disconnect(self):
        self.transport.close()
