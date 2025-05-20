from dataclasses import dataclass
import time
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
