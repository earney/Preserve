import os, sys
import json

sys.path.append("../Common")
import MetadataNodeHandler

class NameMapper:
  def __init__(self, mdn_handler, encryption=False):
      self._mdn_handler=mdn_handler

  def Name2ID(self, name):
      if name.startswith('grid:/'):
         name=name[6:]
      #_result=misc.access_url("http://%s/Client/Name2ID/%s" % (self._metadataNode, name))
      _result=self._mdn_handler.send_message('/Client/Name2ID/%s' % (name))
      if _result is None: return None
      
      return json.loads(_result.decode('utf-8'))
      #lookup on metadata node the id of this name (name is path+filename)

  def ID2Name(self, id):
      #this function is probably not needed since the name of a file
      #will be returned when we list the contents of a directory.
      #I could be wrong, so lets just pass
      pass
