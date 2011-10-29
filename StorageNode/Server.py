#!/usr/bin/env python3

## start a node, and read the storage directory to see which segments we have
## if the directory doesn't, create those directories

from wsgiref.simple_server import make_server

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

_base_path='/tmp/MyGrid'
_quota=1024*1024*1024*10 # 1GB

_ss=SegmentStorage.SegmentStorage(_base_path, _quota)
_si=StorageInfo.StorageInfo(_base_path, _quota)

MetadataNode_ipaddr="127.0.0.1:9696"
_myaddr="127.0.0.1:9697"

last_contact_with_metadatanode=0

def register_with_metadataNode(ipaddr, interval):
    while 1:
      try:
        if last_contact_with_metadatanode + 2*interval < time.time():
           # we haven't heard from Metadata Node in a while so lets
           # re register..
           _fp=urllib.request.urlopen("http://%s/StorageNode/Register/%s" % (ipaddr, _myaddr))
           _data=_fp.read()
           _fp.close()
      except:
        pass
      time.sleep(interval)

def StorageNode(environ, start_response):
    global last_contact_with_metadatanode

    _path_info=environ['PATH_INFO']

    if _path_info.startswith('/GetSegment'):
       _id=misc.inspect_id(_path_info[12:])

       #print(_id)
       if _id is None:
          start_response('404 OK', [('Content-type', 'text/plain')])
          return ['Error, Segment ID is invalid'.encode('utf-8')]

       _valid, _data=_ss.Get(_id)

       #print(_valid)
       #print(_data)
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
       #print("in info" + str(last_contact_with_metadatanode))

       _vars=_si.get_info()
       _headers=[('Content-type', 'application/json')]
       start_response('200 OK', _headers)
       return [misc.send_compressed_response(_vars)]
       #_json=json.dumps(_vars)

       #import gzip
       #_gzip_json=gzip.compress(_json.encode('utf-8'))  
       #return [_json.encode('utf-8')]
       #return [_gzip_json]

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

import threading
_t=threading.Timer(10, register_with_metadataNode, [MetadataNode_ipaddr, 10])
_t.start()

try:
  httpd = make_server('', 9697, StorageNode)
  print("Serving on port 9697...")
  httpd.serve_forever()
except KeyboardInterrupt:
  pass

del _ss
del _si
