import hashlib
import os

class SegmentStorage:
   def __init__(self, base_path, quota):
       self._base_path=base_path
       self._quota=quota

       if not os.path.exists(self._base_path):
          os.mkdir(self._base_path)

   def _segment_location(self, segmentID):
       _deep=5
       _length=5

       _path=self._base_path
       for _i in range(_deep):
           _start=_i*_length
           _end=_start+_length
           _path=os.path.join(_path, segmentID[_start:_end])

       return os.path.join(_path, segmentID)

   def ValidateSegment(self, segmentID, data):
       _sha1=hashlib.new('sha1')
       _sha1.update(data)
       return _sha1.hexdigest()==segmentID

   def Put(self, segmentID, data):
       if self.ValidateSegment(segmentID, data):
          _filename=self._segment_location(segmentID)
          self._validate_path(os.path.dirname(_filename))
          _fp=open(_filename, "wb")
          _fp.write(data)
          _fp.close()
          return None
       return "Error, Segment is invalid"

   def Get(self, segmentID):
       _filename=self._segment_location(segmentID)
       if not os.path.exists(_filename):
          return False, "Error!  Segment does not exist"

       _fp=open(_filename, "rb")
       _data=_fp.read()
       _fp.close()

       if self.ValidateSegment(segmentID, _data):
          return True, _data

       return False, "Error!, Segment is invalid"

   def _validate_path(self, path):
       if os.path.exists(path):
          return True

       _path = path.split(os.path.sep) or [path]
       _current_path=''
       while _current_path == '':
         _current_path = _path.pop(0)

       if path.startswith(os.path.sep):
          _current_path=os.path.join(os.path.sep, _current_path)

       while True:
          try:
            os.mkdir(_current_path)
          except OSError as e:
            if not e.args[0] == 17:
               raise e
            _current_path=os.path.join(_current_path, _path.pop(0))
            continue

          if len(_path) == 0:
             break

          _current_path=os.path.join(_current_path, _path.pop(0))
       
       return True
