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
        return os.path.join(self.data_dir, f"{key}.json")

    def _append_path(self, key: str) -> str:
        return os.path.join(self.data_dir, f"{key}.jsonl")

    def save(self, key: str, data: Any) -> None:
        with open(self._file_path(key), "w") as f:
            json.dump(data, f)

    def load(self, key: str) -> Any:
        path = self._file_path(key)
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            return json.load(f)

    def append(self, key: str, item: Any) -> None:
        with open(self._append_path(key), "a") as f:
            f.write(json.dumps(item) + "\n")

    def load_all(self, key: str) -> List[Any]:
        path = self._append_path(key)
        if not os.path.exists(path):
            return []
        with open(path, "r") as f:
            return [json.loads(line.strip()) for line in f]
