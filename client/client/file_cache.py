import os
import json
from pathlib import Path

class FileCache:
    cache_dir = Path('cache')
    cache_dir.mkdir(exist_ok=True)

    posts_dir =cache_dir/'posts'
    posts_dir.mkdir(exist_ok=True)

    @staticmethod
    def _get_cache_path(key):
        return FileCache.cache_dir/f'{key}.json'
    
    @staticmethod
    async def set(key, data):
        cache_file = FileCache._get_cache_path(key)
        cache_file.parent_mkdir(parents=True, exist_ok=True)
        with open(cache_file, 'w') as f:
            json.dump(data, f)

    @staticmethod
    async def get(key):
        cache_file = FileCache._get_cache_path(key)
        if not cache_file.exists():
            return None
        
        with open(cache_file, 'r') as f:
            return json.load(f)
        
    @staticmethod
    async def delete(key):
        cache_file = FileCache._get_cache_path(key)
        if cache_file.exists():
            os.remove(cache_file)

    @staticmethod
    async def clear():
        for cache_file in FileCache.cache_dir.glob('**/*.json'):
            os.remove(cache_file)