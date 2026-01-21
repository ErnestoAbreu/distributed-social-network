import threading
import json
import os
import time
import logging

logger = logging.getLogger('socialnet.server.storage')

META_VER_PREFIX = "__meta_ver__"
META_DEL_PREFIX = "__meta_del__"

def meta_ver_key(key: str) -> str:
    return f"{META_VER_PREFIX}{key}"

def meta_del_key(key: str) -> str:
    return f"{META_DEL_PREFIX}{key}"

def is_meta_key(key: str) -> bool:
    return key.startswith(META_VER_PREFIX) or key.startswith(META_DEL_PREFIX)

def base_key_from_meta(key: str) -> str:
    if key.startswith(META_VER_PREFIX):
        return key[len(META_VER_PREFIX):]
    if key.startswith(META_DEL_PREFIX):
        return key[len(META_DEL_PREFIX):]
    return key

class Storage:
    def __init__(self):
        self.lock = threading.Lock()
        self.db_file = 'database.json'
        
        if os.path.exists(self.db_file):
            try:
                with open(self.db_file, 'r') as f:
                    loaded = json.load(f)

                self.data = loaded.get('data', {})
            except (json.JSONDecodeError, IOError):
                logger.info("Empty database")
                self.data = {}
        else:
            logger.info("Empty database")
            self.data = {}

    def _save(self):
        try:
            with open(self.db_file, 'w') as f:
                json.dump({"data": self.data}, f, indent=2)
        except IOError as e:
            logger.error(f"Error saving database: {e}")

    def _now_version(self) -> int:
        return int(time.time() * 1000)

    def get_version(self, key: str) -> int:
        key = base_key_from_meta(key)
        with self.lock:
            raw = self.data.get(meta_ver_key(key))

        try:
            return int(raw) if raw is not None else 0
        except Exception:
            return 0
        
    def get_deleted_version(self, key: str) -> int:
        key = base_key_from_meta(key)
        with self.lock:
            raw = self.data.get(meta_del_key(key))

        try:
            return int(raw) if raw is not None else 0
        except Exception:
            return 0

    def put(self, key, value, version = None):
        key = base_key_from_meta(key)
        if version is None:
            version = self._now_version()

        with self.lock:
            self.data[key] = value
            self.data[meta_ver_key(key)] = str(int(version))
            # Clear deleted version if present
            self.data.pop(meta_del_key(key), None)
            self._save()

    def get(self, key):
        with self.lock:
            return self.data.get(key)

    def delete(self, key, version = None):
        key = base_key_from_meta(key)
        if version is None:
            version = self._now_version()
        
        with self.lock:
            self.data.pop(key, None)
            # Version meta is no longer relevant once deleted
            self.data.pop(meta_ver_key(key), None)
            # Record deleted version
            self.data[meta_del_key(key)] = str(int(version))
            self._save()

    def purge(self, key):
        key = base_key_from_meta(key)
        with self.lock:
            self.data.pop(key, None)
            self.data.pop(meta_ver_key(key), None)
            self.data.pop(meta_del_key(key), None)
            self._save()

    def exists(self, key):
        with self.lock:
            return key in self.data
        
    def items(self):
        with self.lock:
            return dict(self.data)
        
    def base_items(self):
        with self.lock:
            return {k: v for k, v in self.data.items() if not is_meta_key(k)}
    
    def deleted_items(self):
        with self.lock:
            deleted = {k: v for k, v in self.data.items() if k.startswith(META_DEL_PREFIX)}

        out = {}
        for mk, raw in deleted.items():
            bk = base_key_from_meta(mk)
            if not bk:
                continue

            try:
                out[bk] = int(raw)
            except Exception:
                out[bk] = 0
        
        return out