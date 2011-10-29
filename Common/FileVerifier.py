import hashlib

class FileVerifier:
  def __init__(self, data=None):
      self._type='sha1'
      self._hash=None
      self.clear()
      if data is not None:
         self.update(data)

  def update(self, data):
      self._hash.update(data)

  def clear(self):
      self._hash=hashlib.new(self._type)

  def hexdigest(self):
      return self._hash.hexdigest()

  def __str__(self):
      return self.hexdigest()

  def __repr__(self):
      return self.hexdigest()
