#!/usr/bin/env python3

## start a node, #read any configuration we have stored
## and if we know of any previous storage nodes, try
## to contact them.
## if not, just wait until they register with us.

#from wsgiref.simple_server import make_server
from cgi import parse_qs, escape

import json, time, datetime
import io
import hashlib
import io

import sys
sys.path.append("libs")
import XYAPTU
import SegmentLocator
import FileSystemMetadata

sys.path.append("../Common")
import misc
import Config
#from wsgissl import SecureWSGIServer, SecureWSGIRequestHandler
from cherrypy import wsgiserver

_config=Config.Config()
_sldb=_config.get_MDN_SegmentLocatorDB()
_fsdir=_config.get_MDN_FileSystemDirectory()
_fsdb=_config.get_MDN_FileSystemDB()
_cert=_config.get_MDN_CertFile()

_segmentLocator=SegmentLocator.SegmentLocator(_sldb)
_fsmetadata=FileSystemMetadata.FileSystemMetadata(metadataDir=_fsdir,
                                                  segmentLocator=_segmentLocator,
                                                  DBFile=_fsdb)

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
    elif _path_info.startswith('/Monitor'):
       #return a nice looking page with basic info such as
       #the number of storage nodes, the number of segments
       #each is storing, the last time they checked in, etc

       _fp=open("templates/default.tmpl", "r")
       _nodes_dict=_segmentLocator.get_statistics()
       _storagenodes=[]
       _mdn=[]
       _missing_segments=[]
       for _node in _nodes_dict.keys():
           _data=_nodes_dict[_node]
           _data['timedelta']=misc.convert_delta_time(time.time() - _data['timestamp'])
           _data['timestamp']=misc.convert_seconds(_data['timestamp'])
           _data['free']=misc.convert_bytes(_data['free'])
           _data['used']=misc.convert_bytes(_data['used'])
           
           _storagenodes.append(_nodes_dict[_node])

       _data={'last_updated': datetime.datetime.now(),
              'storagenodes': _storagenodes,
              'metadatanodes': _mdn,
              'missingsegments': _fsmetadata.get_statistics()}
       _out=io.StringIO()
       _tmpl=XYAPTU.xcopier(_data, ouf=_out)
       _tmpl.xcopy(_fp)
       start_response('200 OK', [('Content-type', 'text/html')])
       return [_out.getvalue().encode('utf-8')]
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
          #print(_name)
          _result=_fsmetadata.lookupID(_name)
          print(_result)
          if _result is None:
             start_response('404 OK', [('Content-type', 'text/plain')])
          else:
             start_response('200 OK', [('Content-type', 'text/plain')])
          return [json.dumps(_result).encode('utf-8')]
       elif _cmd.startswith('/rmdir/'):
          _parentID, _id, _name=_cmd[7:].split('/',2)
          _result=_fsmetadata.rmdir(_parentID, _id, _name)
          start_response('200 OK', [('Content-type', 'text/plain')])
          return [json.dumps(_result).encode('utf-8')]
       elif _cmd.startswith('/cp/'):
            #copy metadata within the grid (ie, from grid location to grid location)
          _parentID, _shaID, _target_dir_id, _name=_cmd[4:].split('/',3)
          _result=_fsmetadata.cp(_parentID, _shaID, _target_dir_id, _name)
          start_response('200 OK', [('Content-type', 'text/plain')])
          return [json.dumps(_result).encode('utf-8')]
       elif _cmd.startswith('/mv/'):
            #copy metadata within grid
          _parentID, _shaID, _target_dir_id, _name=_cmd[4:].split('/',3)
          _result=_fsmetadata.cp(_parentID, _shaID, _target_dir_id, _name)
          start_response('200 OK', [('Content-type', 'text/plain')])
          return [json.dumps(_result).encode('utf-8')]
       elif _cmd.startswith('/mkdir/'):
          _parentid, _name_id, _name=_cmd[7:].split('/', 2)
          print(_parentid, _name_id, _name)
          _result=_fsmetadata.mkdir(_parentid, _name_id, _name)
          if _result is None:
             start_response('200 OK', [('Content-type', 'text/plain')])
             _result=''
          else:
             print(_result)
             start_response('404 OK', [('Content-type', 'text/plain')])
          return [json.dumps(_result).encode('utf-8')]
       elif _cmd.startswith('/putfile/'):
          _parentID, _file_name=_cmd[9:].split('/',1)
          _file_metadata=misc.get_wsgi_file_contents(environ)
          #get file metadata
          if _file_metadata is not None:
             _shaID=misc.get_shaID(_file_metadata)
             _size=0

             _result=_fsmetadata.putfile(_parentID, _shaID, _file_name, _size)
             #print(_result)
             if _result is None:
                #_metadata=json.loads(_file_metadata.decode('utf-8'))
                _fsmetadata.add_file_metadata(_file_metadata)
                start_response('200 OK', [('Content-type', 'text/plain')])
                return [json.dumps(_result).encode('utf-8')]
          print(_result)
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
          #if _fsmetadata.get_file_locations(_shaID)==[]:
             #this file isn't in the metadata anymore so lets 
             #remove the segments
             #_fsmetadata.rmfile(_shaID)

          if _result is None:
             start_response('200 OK', [('Content-type', 'text/plain')])
             return [json.dumps('').encode('utf-8')]
          else:
             start_response('404 OK', [('Content-type', 'text/plain')])
             return [json.dumps(_dict).encode('utf-8')]

       start_response('404 OK', [('Content-type', 'text/plain')])
       return [json.dumps('').encode('utf-8')]


def refresh_grid(interval):
    while 1:
       _fsmetadata.refresh()
       _segmentLocator.refresh()
       time.sleep(interval)


_mdn, _port=_config.get_MDN_Address()

import threading
_t=threading.Timer(10, refresh_grid, [10])
_t.start()

print(_mdn, _port)
_server=wsgiserver.CherryPyWSGIServer((_mdn, int(_port)), MetadataNode)

_server.ssl_adapter=wsgiserver.ssl_builtin.BuiltinSSLAdapter(_cert, _cert)

try:
  _server.start()
except KeyboardInterrupt:
  _server.stop()


#try:
#  _httpd = make_server(_mdn, int(_port), MetadataNode,
#                       server_class=SecureWSGIServer, 
#                       handler_class=SecureWSGIRequestHandler)

#  print("Serving on port %s..." % _port)
#  _httpd.set_credentials(keypath=_cert, certpath=_cert)
#  _httpd.serve_forever()
#except KeyboardInterrupt:
#  pass

del _segmentLocator
del _fsmetadata
