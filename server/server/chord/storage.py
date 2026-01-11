import threading
import json
import os
import logging

logger = logging.getLogger('socialnet.server.storage')

class Storage:
    def __init__(self):
        self.lock = threading.Lock()
        self.db_file = 'database.json'
        
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, 'r') as f:
                    self.data = json.load(f)
            except (json.JSONDecodeError, IOError):
                logger.info("Empty database")
                self.data = {}
        else:
            logger.info("Empty database")
            self.data = {}

    def _save(self):
        try:
            with open(self.db_file, 'w') as f:
                json.dump(self.data, f, indent=2)
        except IOError as e:
            logger.error(f"Error saving database: {e}")

    def put(self, key, value):
        with self.lock:
            self.data[key] = value
            self._save()

    def get(self, key):
        with self.lock:
            return self.data.get(key)

    def delete(self, key):
        with self.lock:
            self.data.pop(key, None)
            self._save()

    def exists(self, key):
        with self.lock:
            return key in self.data
        
    def items(self):
        with self.lock:
            return dict(self.data)