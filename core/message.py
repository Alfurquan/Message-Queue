from dataclasses import dataclass
import time
from typing import Dict
import uuid

@dataclass
class Message:
    id: str
    payload: str
    timestamp: float

    @staticmethod
    def create(payload: str) -> 'Message':
        return Message(
            id=str(uuid.uuid4()),
            payload=payload,
            timestamp=time.time()
        )

    def to_dict(self) -> Dict:
        return {
            'id': self.id,
            'payload': self.payload,
            'timestamp': self.timestamp
        }

    @staticmethod
    def from_dict(data: Dict):
        return Message(
            payload=data['payload'],
            id=data['id'],
            timestamp=data.get('timestamp')
        )

    def __repr__(self):
        return f"Message(id={self.id}, payload={self.payload})"
