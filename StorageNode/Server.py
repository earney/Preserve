#!/usr/bin/env python3

## start a node, and read the storage directory to see which segments we have
## if the directory doesn't, create those directories

#from wsgiref.simple_server import make_server


import json
import time
import StorageInfo
import io
#import gzip
import urllib.request

import sys
sys.path.append("../Common")
import misc
import SegmentStorage
import Config
import MetadataNodeHandler
#from wsgissl import SecureWSGIServer, SecureWSGIRequestHandler

from cherrypy import wsgiserver

_config=Config.Config()
_mdn=_config.get_MetadataNodes()

_storage_address, _storage_port=_config.get_SN_Address()
_storage_path=_config.get_SN_Path()
_quota=_config.get_SN_Quota()

_cert=_config.get_SN_CertFile()

_ss=SegmentStorage.SegmentStorage(_storage_path, _quota)
_si=StorageInfo.StorageInfo(_storage_path, _quota)

_myaddr='%s:%s' % (_storage_address, _storage_port)

last_contact_with_metadatanode=0
last_contact_interval=60  #60 seconds

def register_with_metadataNode(mdn_handler):
    while 1:
      try:
        if last_contact_with_metadatanode + 2*last_contact_interval < time.time():
           # we haven't heard from Metadata Node in a while so lets
           # re register..
           print("start:trying to access Metadata node")
           mdn_handler.send_message("/StorageNode/Register/%s" % (_myaddr))
           print("stop:trying to access Metadata node")
      except:
        pass
      time.sleep(last_contact_interval)

def StorageNode(environ, start_response):
    global last_contact_with_metadatanode
    global last_contact_interval

    _path_info=environ['PATH_INFO']

    if _path_info.startswith('/GetSegment'):
       _id=misc.inspect_id(_path_info[12:])

       #print(_id)
       if _id is None:
          start_response('404 OK', [('Content-type', 'text/plain')])
          return ['Error, Segment ID is invalid'.encode('utf-8')]

       _valid, _data=_ss.Get(_id)

       if not _valid:
          headers = [('Content-type', 'text/plain')]
          start_response('404 OK', headers)
          return [_data.encode('utf-8')]

       start_response('200 OK', [('Content-type', 'application/data')])
       return [_data]

    elif _path_info=='/Info':
       #fixme, need to check that the recipient of this is a metadata server
       _ip=environ['REMOTE_ADDR']

       last_contact_with_metadatanode=time.time()
       #maybe retrieve interval and list of metadatanodes from server?
       #print("in info" + str(last_contact_with_metadatanode))

       _vars=_si.get_info()
       print(_vars)
       _headers=[('Content-type', 'application/json')]
       start_response('200 OK', _headers)
       return [misc.send_compressed_response(_vars)]

    elif _path_info.startswith('/PutSegment'):
       _id=misc.inspect_id(_path_info[12:])

       if _id is None:
          start_response('404 OK', [('Content-type', 'text/plain')])
          return ['Error, Segment ID is invalid'.encode('utf-8')]

       _segment_data=misc.get_wsgi_file_contents(environ)
       if _segment_data is None:
          start_response('404 OK', [('Content-type', 'text/plain')])
          return ['Error, No segment data'.encode('utf-8')]

       _result=_ss.Put(_id, _segment_data)
       if _result is None:
          start_response('200 OK', [('Content-type', 'text/plain')])
          return [''.encode('utf-8')]
       else:
          start_response('404 OK', [('Content-type', 'text/plain')])
          return [_result.encode('utf-8')]


    start_response('404 OK', [('Content-type', 'text/plain')])
    return ['Error, must provide an action'.encode('utf-8')]

mdn_handler=MetadataNodeHandler.MetadataNodeHandler(_mdn)

import threading
_t=threading.Timer(1, register_with_metadataNode, [mdn_handler])
_t.start()

print(_storage_address, _storage_port)
print(_cert)
_server = wsgiserver.CherryPyWSGIServer((_storage_address, _storage_port), 
                                        StorageNode)

#The CherryPy WSGI server can serve as many WSGI applications
#as you want in one instance by using a WSGIPathInfoDispatcher:

# interesting idea..  implement this later :)
#    d = WSGIPathInfoDispatcher({'/': my_crazy_app, '/blog': my_blog_app})
#    server = wsgiserver.CherryPyWSGIServer(('0.0.0.0', 80), d)

#Want SSL support? Just set these attributes:

_server.ssl_adapter=wsgiserver.ssl_builtin.BuiltinSSLAdapter(_cert, _cert)

try:
  _server.start()
except KeyboardInterrupt:
  _server.stop()

#try:
#  _httpd=make_server(_storage_address, _storage_port, StorageNode,
#                     server_class=SecureWSGIServer, 
#                     handler_class=SecureWSGIRequestHandler)
#  print("Serving on port %s..." % _storage_port)
#  _httpd.set_credentials(keypath=_cert, certpath=_cert)
#  _httpd.serve_forever()
#except KeyboardInterrupt:
#  pass

del _ss
del _si
