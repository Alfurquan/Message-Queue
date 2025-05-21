# core/storage/file_backend.py
import os
import json
from typing import Any, List
from persistence.storage import Storage

class FileStorage(Storage):
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def _file_path(self, key: str) -> str:
        return os.path.join(self.data_dir, *key.split("/")) + ".json"

    def save(self, key: str, data: Any) -> None:
        path = self._file_path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(data, f)

    def load(self, key: str) -> Any:
        path = self._file_path(key)
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            return json.load(f)
