import sys, os

sys.path.append("../Common")
import CacheHelper

class StorageInfo(CacheHelper.CacheHelper):
   def __init__(self, base_path, quota, cache_time=30):
       CacheHelper.CacheHelper.__init__(self, cache_time)
       self._worker=StorageInfoWorker(base_path, quota)

   def get_free_space(self):
       return self._cache_helper(self._worker.get_free_space)

   def get_used_space(self):
       return self._cache_helper(self._worker.get_used_space)

   def get_segment_list(self):
       return self._cache_helper(self._worker.get_segment_list)

   def get_info(self):
       return {'free_space': self.get_free_space(),
               'used_space': self.get_used_space(),
               'segments': self.get_segment_list()}

class StorageInfoWorker:
   def __init__(self, base_path, quota):
       self._base_path=base_path
       self._quota=int(quota)  #for now assume quota is in bytes

   def get_free_space(self):
       return self._quota-self.get_used_space()

   def get_used_space(self):
       _size = 0
       for _root, _dirs, _files in os.walk(self._base_path):
           for _file in _files:
               _filename = os.path.join(_root, _file)
               _size += os.path.getsize(_filename)
       print(self._base_path)
       print(_size)
       return _size

   def get_segment_list(self):
       _segments = []
       for _root, _dirs, _files in os.walk(self._base_path):
          for _file in _files:
              _segments.append(_file)

       return _segments
