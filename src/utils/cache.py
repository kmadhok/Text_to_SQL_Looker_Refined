"""Caching utilities for metadata and other data."""

import json
import logging
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class SimpleCache:
    """Simple file-based cache with TTL support."""
    
    def __init__(self, cache_dir: str, default_ttl: int = 3600):
        """Initialize cache with directory and default TTL."""
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.default_ttl = default_ttl
        self.memory_cache: Dict[str, Dict[str, Any]] = {}
    
    def get(self, key: str, ttl: Optional[int] = None) -> Optional[Any]:
        """Get value from cache."""
        ttl = ttl or self.default_ttl
        
        # Check memory cache first
        if key in self.memory_cache:
            cached_data = self.memory_cache[key]
            if time.time() - cached_data['timestamp'] < ttl:
                return cached_data['value']
            else:
                # Expired, remove from memory
                del self.memory_cache[key]
        
        # Check file cache
        cache_file = self.cache_dir / f"{key}.json"
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    cached_data = json.load(f)
                
                if time.time() - cached_data['timestamp'] < ttl:
                    # Load into memory cache
                    self.memory_cache[key] = cached_data
                    return cached_data['value']
                else:
                    # Expired, remove file
                    cache_file.unlink()
            
            except Exception as e:
                logger.warning(f"Error reading cache file {cache_file}: {e}")
                try:
                    cache_file.unlink()
                except:
                    pass
        
        return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache."""
        ttl = ttl or self.default_ttl
        timestamp = time.time()
        
        cached_data = {
            'value': value,
            'timestamp': timestamp,
            'ttl': ttl
        }
        
        # Store in memory
        self.memory_cache[key] = cached_data
        
        # Store in file
        cache_file = self.cache_dir / f"{key}.json"
        try:
            with open(cache_file, 'w') as f:
                json.dump(cached_data, f, indent=2, default=str)
        except Exception as e:
            logger.warning(f"Error writing cache file {cache_file}: {e}")
    
    def delete(self, key: str) -> None:
        """Delete key from cache."""
        # Remove from memory
        if key in self.memory_cache:
            del self.memory_cache[key]
        
        # Remove file
        cache_file = self.cache_dir / f"{key}.json"
        if cache_file.exists():
            try:
                cache_file.unlink()
            except Exception as e:
                logger.warning(f"Error deleting cache file {cache_file}: {e}")
    
    def clear(self) -> None:
        """Clear all cached data."""
        # Clear memory cache
        self.memory_cache.clear()
        
        # Clear file cache
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                cache_file.unlink()
            except Exception as e:
                logger.warning(f"Error deleting cache file {cache_file}: {e}")
    
    def cleanup_expired(self, ttl: Optional[int] = None) -> None:
        """Remove expired cache entries."""
        ttl = ttl or self.default_ttl
        current_time = time.time()
        
        # Cleanup memory cache
        expired_keys = []
        for key, cached_data in self.memory_cache.items():
            if current_time - cached_data['timestamp'] >= ttl:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.memory_cache[key]
        
        # Cleanup file cache
        for cache_file in self.cache_dir.glob("*.json"):
            try:
                with open(cache_file, 'r') as f:
                    cached_data = json.load(f)
                
                if current_time - cached_data['timestamp'] >= ttl:
                    cache_file.unlink()
            
            except Exception as e:
                logger.warning(f"Error processing cache file {cache_file}: {e}")
                # If we can't read it, it's probably corrupted, so remove it
                try:
                    cache_file.unlink()
                except:
                    pass