#!/usr/bin/env python3

## start a node, #read any configuration we have stored
## and if we know of any previous storage nodes, try
## to contact them.
## if not, just wait until they register with us.

from wsgiref.simple_server import make_server
from cgi import parse_qs, escape

import json, time
import SegmentLocator
import FileSystemMetadata
import io
import hashlib

import sys
sys.path.append("../Common")
import misc

_base_path='/tmp/MyGrid'

_segmentLocator=SegmentLocator.SegmentLocator()
_fsmetadata=FileSystemMetadata.FileSystemMetadata()
#_filemetadata=FileSystemMetadata.FileMetadata()

def MetadataNode(environ, start_response):
    _path_info=environ["PATH_INFO"]
    print(_path_info)
    if _path_info.startswith('/StorageNode/Register/'):
       #this is a storage node register
       # syntax /Register/ipaddr:port/
       #print(_segmentLocator.get_statistics())
       _ipaddr=_path_info[22:]
       #print(_ipaddr)
       _segmentLocator.refresh(_ipaddr)
       start_response('200 OK', [('Content-type', 'text/plain')])
       return [''.encode('utf-8')]
    #elif _path_info.startswith('/RegisterMetadataNode/'):
    #   #this is a storage node register
    #   # syntax /Register/ipaddr:port/
    #   _ipaddr=_path_info[22:]
    #   self._metadataNodes.refresh(_ipaddr)
    elif _path_info.startswith('/Client'):
       #this is from a client that will want certain information
       #command list (listdir, rmdir, mkdir)
       # (put file, get file)
       _cmd=_path_info[7:]

       if _cmd.startswith('/locate_segments'):
          _result=misc.get_wsgi_file_contents(environ)
          _segments=misc.receive_compressed_response(_result)
          #_segments=json.loads(_result.decode('utf-8'))
          _dict=_segmentLocator.locate_segments(_segments)

          start_response('200 OK', [('Content-type', 'text/plain')])
          return [misc.send_compressed_response(_dict)]
       elif _cmd.startswith('/listdir/'):
          _id=_cmd[9:]
          #print(_id)
          _result=_fsmetadata.listdir(_id)
          if _result is None:
             start_response('404 OK', [('Content-type', 'text/plain')])
          else:
             start_response('200 OK', [('Content-type', 'text/plain')])
          return [json.dumps(_result).encode('utf-8')]
       elif _cmd.startswith('/Name2ID/'):
          _name=_cmd[9:]
          print(_name)
          _result=_fsmetadata.lookupID(_name)
          print(_result)
          if _result is None:
             start_response('404 OK', [('Content-type', 'text/plain')])
          else:
             start_response('200 OK', [('Content-type', 'text/plain')])
          return [json.dumps(_result).encode('utf-8')]
       elif _cmd.startswith('/rmdir/'):
          _parentID, _id, _name=_cmd[7:].split('/',3)
          _result=_fsmetadata.rmdir(_parentID, _id, _name)
          start_response('200 OK', [('Content-type', 'text/plain')])
          return [json.dumps(_result).encode('utf-8')]
       elif _cmd.startswith('/mkdir/'):
          _parentid, _name_id, _name=_cmd[7:].split('/', 3)
          _result=_fsmetadata.mkdir(_parentid, _name_id, _name)
          if _result is None:
             start_response('200 OK', [('Content-type', 'text/plain')])
          else:
             start_response('404 OK', [('Content-type', 'text/plain')])
          return [json.dumps(_result).encode('utf-8')]
       elif _cmd.startswith('/putfile/'):
          _dir_id, _file_name=_cmd[9:].split('/')
          #print(_dir_id, _file_name)
          _file_metadata=misc.get_wsgi_file_contents(environ)
          #get file metadata
          if _file_metadata is not None:
             _sha1=hashlib.new('sha1')
             _sha1.update(_file_metadata)
             _shaID=_sha1.hexdigest()
             _size=0

             _result=_fsmetadata.putfile(_dir_id, _shaID, _file_name, _size)
             #print(_result)
             if _result is None:
                #_metadata=json.loads(_file_metadata.decode('utf-8'))
                _fsmetadata.add_file_metadata(_file_metadata)
                start_response('200 OK', [('Content-type', 'text/plain')])
                return [json.dumps(_result).encode('utf-8')]
          _result=''
          start_response('404 OK', [('Content-type', 'text/plain')])
          return [json.dumps(_result).encode('utf-8')]
       elif _cmd.startswith('/getfile/'):
          _shaID=_cmd[9:]
          _valid, _result=_fsmetadata.get_file_metadata(_shaID)
          if _result is None:
             start_response('404 OK', [('Content-type', 'text/plain')])
             _result=''
          else:
             start_response('200 OK', [('Content-type', 'text/plain')])

          return [misc.send_compressed_response(_result.decode('utf-8'))] 
          #return [json.dumps(_result).encode('utf-8')] 
       elif _cmd.startswith('/getStorageNodeStatistics'):
          _dict=_segmentLocator.get_statistics()
          if _dict=={}:  #try refreshing to get a list of nodes
             _segmentLocator.refresh()
             _dict=_segmentLocator.get_statistics()

          #send string to client
          start_response('200 OK', [('Content-type', 'text/plain')])
          return [json.dumps(_dict).encode('utf-8')]
       elif _cmd.startswith('/rmfile/'):
          _dir_id, _shaID=_cmd[8:].split('/')

          _result=_fsmetadata.rmfile(_dir_id, _shaID)
          if _fsmetadata.get_file_locations(_shaID)==[]:
             #this file isn't in the metadata anymore so lets 
             #remove the segments
             _fsmetadata.rmfile(_shaID)

          if _result is None:
             start_response('200 OK', [('Content-type', 'text/plain')])
             return [json.dumps('').encode('utf-8')]
          else:
             start_response('404 OK', [('Content-type', 'text/plain')])
             return [json.dumps(_dict).encode('utf-8')]

       start_response('404 OK', [('Content-type', 'text/plain')])
       return [json.dumps('').encode('utf-8')]


def contact_ServerNodes(interval):
    while 1:
       _segmentLocator.refresh()
       time.sleep(interval)


import threading
_t=threading.Timer(10, contact_ServerNodes, [10])
_t.start()

try:
  httpd = make_server('', 9696, MetadataNode)
  print("Serving on port 9696...")
  httpd.serve_forever()
except KeyboardInterrupt:
  pass

del _segmentLocator
del _fsmetadata
