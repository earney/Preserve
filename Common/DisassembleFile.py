import collections
import hashlib
import time
import urllib.request
import json
import os
from zfec import easyfec

import sys
sys.path.append("../Common/")
import misc

class UploadFile:
   def __init__(self, mdn_handler, refresh_stats_time=120):
       self._mdn_handler=mdn_handler
       self._last_contact=0
       self._refresh_stats_time=refresh_stats_time
       self._storage_stats={}
       self._used_storage_nodes_count=collections.defaultdict(int)

       self._best_nodes=[]

   def _getStorageNodeStatistics(self, segment_size):
       if ((self._last_contact + self._refresh_stats_time) < time.time()) or \
          self._best_nodes == []:
          self._last_contact=time.time()
          #need to update our stats of the Storage nodes so we know
          #where we should put more segments.
          _result=self._mdn_handler.send_message('/Client/getStorageNodeStatistics')
          #_url="https://%s/Client/getStorageNodeStatistics" % self._metadatanode
          #_fp = urllib.request.urlopen(_url)
          #_data=_fp.read()
          #print(_data)
          #_fp.close()

          self._storage_stats=json.loads(_result.decode('utf-8'))
          #self._storage_stats=json.loads(_data.decode('utf-8'))
          if self._storage_stats=={}:
             print("Error! no storage nodes returned")

       self._best_nodes=self._get_min_nodes(segment_size)

   def _get_min_nodes(self, segment_size):
       #retrieve a list of nodes which contain the minimum number
       #of segments for this file upload.

       _min=None
       for _node in list(self._storage_stats.keys()):
           if self._storage_stats[_node]['free'] < segment_size:
              # ignore nodes that do not have enough space to store
              # another segment
              continue

           if _min is None:
              _min=[_node]
           elif _min > self._storage_stats[_node]:
              _min=[_node]
           elif _min == self._storage_stats[_node]:
              _min.append(_node)

       if _min is None:
          return []

       return _min

   def upload_segment(self, segmentID, data):
       self._getStorageNodeStatistics(len(data))
       #we can pick a storage node based on several different factors,
       #such as amount of free space, # of segments, etc
      
       if self._best_nodes==[]:  
          #we have a problem, no nodes can store data.
          print("Error, we can't store any more data in the Grid")
          return "Error, we can't store any more data in the Grid"

       _node=self._best_nodes[0]
       if len(self._best_nodes) > 1:
          _node=self._best_nodes[1:]
       else:
          self._best_nodes=[]

       #put data on given node.
       _url="https://%s/PutSegment/%s" % (_node, segmentID)
       _result=misc.access_url(_url, data=data)
       return _result

class ZfecParameterCalculator:
   def __init__(self, group_num, file_type=None):
       #group_num will allow us to change the values of k, m
       #based on which group_num (ie, extent) we are in the file.
       #we can also use other file attributes (such a file type
       #or file size) to figure out appropriate values for
       #k and m.

       #for now, just go with some basic defaults
       self._k=10
       self._m=15

   def get_parameters(self):
       return 10, 15
       #return {'k': self._k, 'm': self._m}


class DisassembleFile:
   def __init__(self, filename, parentID, grid_filename, 
                mdn_handler, group_size=10*1024*1024):
       self._filename=filename
       self._group_size=group_size

       self._mdn_handler=mdn_handler
       #self._metadataNode=metadataNode
       self._parentID=parentID
       self._grid_filename=grid_filename

       #self._upload_file=UploadFile(metadataNode)
       self._upload_file=UploadFile(mdn_handler)

   def process(self):
       #fix this, each group should be in its own list for easier processing
       #when we reassemble the file
       _fp=open(self._filename, "rb")
       _group_count=0

       _metadata=[]
       while True:
         _data=_fp.read(self._group_size)
         if not _data:
            break
 
         _parms=ZfecParameterCalculator(_group_count)
         _k, _m=_parms.get_parameters()
         _encoder=easyfec.Encoder(_k, _m)
         _segments=_encoder.encode(_data)

         _segment_length=len(_segments[0])
         #padding should be 0 if _k evenly divides into group size
         _padding=_segment_length*_k-len(_data)

         _recovery_data={'type': 'zfec', 'group': _group_count, 
                         'k': _k, 'm': _m,
                         'padding': _padding}
          
         _seq=0
         _blocks=[]
         for _segment in _segments:
             _segmentID=misc.get_shaID(_segment)
             #_sha=hashlib.new('sha1')
             #_sha.update(_segment)
             #_segmentID=_sha.hexdigest()
             _restoreBlock=False
             if _seq >= _k:
                _restoreBlock=True
             _blocks.append({'group': _group_count, 'seq': _seq,
                             'segmentID': _segmentID, 'length': len(_segment),
                             'restoreBlock': _restoreBlock})

             self._upload_file.upload_segment(_segmentID, _segment)

             _seq+=1
             print(_group_count, _seq, _segmentID)

         _metadata.append({'segments' : _blocks, 'recovery': _recovery_data})
         _group_count+=1

         #write out segment to temp storage so we can upload the data
         #and metadata. (or just upload segment to one or more 
         #storage nodes)

       _fp.close()

       _json=json.dumps(_metadata).encode('utf-8')

       #_shaID=misc.get_shaID(_json)

       _result=self._mdn_handler.send_message('/Client/putfile/%s/%s' % (self._parentID, self._grid_filename), data=_json)
       #_url="https://%s/Client/putfile/%s/%s" % (self._metadataNode, 
       #          self._parentID, self._grid_filename)
       #_result=misc.access_url(_url, data=_json)
       return _result
