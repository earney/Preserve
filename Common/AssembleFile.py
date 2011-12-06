#!/usr/bin/env python3

import json
from zfec import easyfec

import sys
sys.path.append("../Common")
import misc
import FileVerifier

class AssembleFile:
   def __init__(self, metadataNode):
       self._metadataNode=metadataNode

       self._decoder_k=0
       self._decoder_m=0
       self._decoder_padding=0
       self._data=[]

   def _get_metadata(self, shaID):
       _url="http://%s/Client/getfile/%s" % (self._metadataNode, shaID)
       _result=misc.access_url(_url)
       #print(isinstance(_result, bytes))
       _metadata=misc.receive_compressed_response(_result)
       #return _metadata.decode('utf-8')
       import json
       return json.loads(_metadata)

   def _parse_metadata_for_segments(self, metadata):
       _segments=[]
       _group=[]
       for _group in metadata:
           _restore_segments=[]
           for _mydict in _group_dict['segments']:
               if _mydict['restoreBlock']:
                  _restore_segments.append(_mydict['segmentID'])
               else:
                  _group.append(_mydict['segmentID'])

           _segments.append({'segments': _group, 'restoreBlocks': _restore_segments})
       
       return _segments

   def _get_recovery_parameters(self, recovery):
       self._decoder_type=recovery['type']
       self._decoder_k=recovery['k']
       self._decoder_m=recovery['m']
       self._decoder_padding=recovery['padding']

   def _retrieve_segment_locations(self, segments):
       """retrieve locations of all segments in this file """
       _all_segments=[]
       for _dict in segments:
           _all_segments.append(_dict['segmentID'])

       _json=misc.send_compressed_response(_all_segments)

       _result=misc.access_url("http://%s/Client/locate_segments" % self._metadataNode, data=_json)
       return misc.receive_compressed_response(_result)

   def process(self, shaID, output_filename):
       _metadata=self._get_metadata(shaID)

       _fp=open(output_filename, "wb")
       for _group in _metadata:
           _locations=self._retrieve_segment_locations(_group['segments'])

           self._get_recovery_parameters(_group['recovery'])
           _success, _group_data=self._process_group(_group, _locations)
           if not _success:  # we have a problem, can't recover
              print("cannot recover group")
           else:
              _fp.write(_group_data)

       _fp.close()

   def _process_group(self, group_dict, locations):
       _main_segments=[]
       _restore_segments=[]
       for _segment_dict in group_dict['segments']:
           if _segment_dict['restoreBlock']:
              _restore_segments.append(_segment_dict['segmentID'])
           else:
              _main_segments.append(_segment_dict['segmentID'])

       _missing_segments=[]

       #now download each segment
       _count=0
       _retrieved_segments=[]
       _segments=[]
       for _segment in _main_segments:
           if locations[_segment]==[]:
              #this segment isn't located anywhere..
              _missing_segments.append(_segment)

           for _nodeID in locations[_segment]:
               _data=misc.access_url("http://%s/GetSegment/%s" % (_nodeID, _segment))

               if _data is not None:
                  if self._is_verified(_segment, _data):
                     # verified that _data == _segmentID
                     _retrieved_segments.append(_count)
                     _segments.append(_data)
                     #can ignore rest of the locations and download next segment
                     break
           _count+=1

       _num_missing_segments=len(_missing_segments)

       if _num_missing_segments==0:
          if self._decoder_padding > 0:
             #remove padding from the end of the group
             _segments[-1]=_segments[-1][:-self._decoder_padding]

          #return data since recovery isn't needed
          return True, b''.join(_segments)

       #retrieve blocks to do recovery
       _recovery_segments=[]
       while _restore_segments != []:
             _segment=_restore_segments.pop(0)

             for _nodeID in locations[_segment]:
                 _data=misc.access_url("http://%s/GetSegment/%s" % (_nodeID, _segment))

                 if _data is not None:
                    #need to verify that _data == _segmentID
                    #if so
                    _retrieved_segments.append(_count)
                    _recovery_segments.append(_data)

                 #if we have enough recovery blocks lets recover
                 if len(_retrieved_segments) >= _num_missing_segments:
                    _decoder=easyfec.Decoder(self._decoder_k, self._decoder_m)
                    return True, b''.join(_decoder.decode(_segments + _recovery_segments, 
                                           _retrieved_segments,
                                           self._decoder_padding))

       #give up, we cannot recover this segment group
       return False, 'Error! Cannot recover this group segment'

   def _is_verified(self, id, data):
       return id==FileVerifier.FileVerifier(data).hexdigest()
