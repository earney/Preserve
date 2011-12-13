import urllib.request

class CircularList:
    def __init__(self, data=[]):
        self._data= data
        self._cur= 0

    def append(self, x):
        self._data.append(x)

    def next(self):
        if self._cur==len(self._data):
           self._cur=-1

        self._cur+=1
        return self._data[self._cur]

    def size(self):
        return len(self._data)

    def __str__(self):
        return ','.join(self._data)        


class MetadataNodeHandler:
  def __init__(self, mdn_addresses):
      self._mdn_addresses=CircularList(data=mdn_addresses)
      self._current_mdn=mdn_addresses[0]

  def _try_next_mdn(self, urlpath, data=None):
      _fail=True
      while _fail:
        _url='http://%s%s' % (self._current_mdn, urlpath)
        try:
          if data is None:
             _fp = urllib.request.urlopen(_url)
          else:
             _fp = urllib.request.urlopen(_url, data=data)
          _fail=False
        except URLError:
          self._current_mdn=self._mdn_addresses.next()

      _data=_fp.read()
      _fp.close()
      return _data

  def send_message(self, urlpath, data=None):
      return self._try_next_mdn(urlpath, data=data)
