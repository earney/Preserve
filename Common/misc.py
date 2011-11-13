import urllib.error 
import urllib.request

import gzip
import json
import hashlib

def get_wsgi_file_contents(environ):
    _body=None
    try:
      _length= int(environ.get('CONTENT_LENGTH', '0'))
    except ValueError:
      _length= 0

    if _length!=0:
         _body= environ['wsgi.input'].read(_length)

    return _body

def inspect_id(id):
    if id.endswith('/'):
       id=id[:-1]

    if len(id)==40:
       return id

    return None


def access_url(url, data=None):
    try:
      if data is None:
         _fp = urllib.request.urlopen(url)
      else:
         _fp = urllib.request.urlopen(url, data=data)

      _data=_fp.read()
      _fp.close()
      return _data
    except urllib.error.HTTPError as e:
      print("HTTP Error:",e.code , url)
      return None
    except urllib.error.URLError as e:
      print("URL Error:",e.reason , url)

    return None


def send_compressed_response(results):
    _json=json.dumps(results).encode('utf-8')
    return gzip.compress(_json)

def receive_compressed_response(results):
    _json=gzip.decompress(results)
    return json.loads(_json.decode('utf-8'))

def get_shaID(str):
    _sha=hashlib.new('sha1')
    _sha.update(str)
    return _sha.hexdigest()
