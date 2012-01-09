import urllib.error 
import urllib.request
import datetime
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
    _json=json.dumps(results).replace('\n', '').encode('utf-8')
    return gzip.compress(_json)

def receive_compressed_response(results):
    _json=gzip.decompress(results)
    return json.loads(_json.decode('utf-8'))

def get_shaID(str):
    _sha=hashlib.new('sha1')
    _sha.update(str)
    return _sha.hexdigest()


def convert_bytes(bytes):
    """convert bytes into human readable format"""

    bytes = float(bytes)
    if bytes >= 1099511627776:
        terabytes = bytes / 1099511627776
        size = '%.2fT' % terabytes
    elif bytes >= 1073741824:
        gigabytes = bytes / 1073741824
        size = '%.2fG' % gigabytes
    elif bytes >= 1048576:
        megabytes = bytes / 1048576
        size = '%.2fM' % megabytes
    elif bytes >= 1024:
        kilobytes = bytes / 1024
        size = '%.2fK' % kilobytes
    else:
        size = '%.2fb' % bytes
    return size

def convert_delta_time(sec):
    """convert number of seconds to human readable format"""
    _sec=float(sec)
    if _sec >= 24*3600:
        return '%.2f days' % (_sec/24/3600)
    elif _sec >= 3600:
        return '%.2f hours' % (_sec/3600)
    elif _sec >= 60:
        return '%.2f minutes' % (_sec/60)

    return "%.2f seconds" % _sec

def convert_seconds(sec):
    """convert seconds to YYYY-MM-DD HH:MI:SS"""
    try:
      _date=datetime.datetime.fromtimestamp(sec)
    except:
      return ''

    return _date.isoformat(' ')
