import time
import collections

class CacheHelper:
   def __init__(self, cache_time=300):
       self._refresh_time=collections.defaultdict(int)
       self._cached=collections.defaultdict(int)
       
   def _cache_helper(self, func):
       if time.time() > self._refresh_time[func.__name__]:
          self._cached[func.__name__]=func()
          self._refresh_time[func.__name__]=time.time()

       return self._cached[func.__name__]
