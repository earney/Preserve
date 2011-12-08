import urllib.request
import json, time
import collections
import os, sys
#sys.path.append("../Common")

import sqlite3

class SegmentLocator:
   def __init__(self, DBFile="MDN.db"):
       self._db_name=os.path.expanduser(DBFile)
       self._ip_addr2nodeID={}
       self._nodeID2ip_addr={}
       self._last_contact=collections.defaultdict(int)
       self._max_nodeID=-1

       if os.path.exists(self._db_name):
          os.remove(self._db_name)

       if not os.path.exists(os.path.basename(self._db_name)):
          os.mkdir(os.path.basename(self._db_name))

       _db=sqlite3.connect(self._db_name)
       _conn=_db.cursor()

       _conn.execute("""create table storage_node_segments (nodeID int,
                                                            segmentID text)""")

       _conn.execute("""create index storage_node_segments_ndx on
                                     storage_node_segments(nodeID)""")

       _conn.execute("""create table storage_nodeID (nodeID int,
                                                     ip_addr text,
                                                     contact_date int,
                                                     free_space bigint,
                                                     used_space bigint)""")

       _conn.execute("""create index storage_node_info_ndx on
                                     storage_nodeID(nodeID)""")

       _db.commit()
       _db.close()

   def _get_storage_nodeID(self, ip_addr):
       if ip_addr in self._ip_addr2nodeID:
          return self._ip_addr2nodeID[ip_addr]

       _db=sqlite3.connect(self._db_name)
       _conn=_db.cursor()
       _conn.execute("""select nodeID 
                          from storage_nodeID 
                         where ip_addr=?""", (ip_addr,))
       _nodeID=_conn.fetchone()
       if _nodeID is not None:
          self._ip_addr2nodeID[ip_addr]=_nodeID
          self._nodeID2ip_addr[_nodeID]=_ip_addr
          return self._ip_addr2nodeID[ip_addr]

       #we don't know this node, so lets create a new ID for it.
       self._max_nodeID+=1

       _conn.execute("insert into storage_nodeID values(?,?,NULL,NULL,NULL)", (self._max_nodeID, ip_addr,))
       _db.commit()
       _db.close()
       self._ip_addr2nodeID[ip_addr]=self._max_nodeID
       self._nodeID2ip_addr[self._max_nodeID]=ip_addr
       self._last_contact[self._max_nodeID]=None

       return self._max_nodeID

   def _update_storage_node_info(self, ipaddr, data):
       _vars=json.loads(str(data))
       #print(_vars)
       _segments=_vars['segments']
       _free_space=_vars['free_space']
       _used_space=_vars['used_space']

       _nodeID=self._get_storage_nodeID(ipaddr)

       _db=sqlite3.connect(self._db_name)
       _conn=_db.cursor()

       _conn.execute("""update storage_nodeID 
                           set contact_date=?,
                               free_space=?,
                               used_space=?
                         where nodeID=?""", (time.time(), _free_space, _used_space, _nodeID, ))

       self._last_contact[_nodeID]=time.time()
 
       _conn.execute("""delete 
                          from storage_node_segments 
                         where nodeid=?""", (_nodeID,))
       
       for _segment in _segments:
           _conn.execute("""insert into storage_node_segments 
                                values (?,?)""", (_nodeID, _segment,))
       _db.commit()

   def locate_segments(self, segmentIDs):
       _db=sqlite3.connect(self._db_name)
       _conn=_db.cursor()

       _dict={}
       for _segmentID in segmentIDs:
           _dict[_segmentID]=[]
           _conn.execute("""select nodeID 
                              from storage_node_segments 
                             where segmentID=?""", (_segmentID,))

           for _nodeID, in _conn:
               _ip_addr=self._nodeID2ip_addr[_nodeID]
               _dict[_segmentID].append(_ip_addr)

       _conn.close()
       return _dict
      

   def get_statistics(self):
       _db=sqlite3.connect(self._db_name)
       _conn=_db.cursor()
       _conn.execute("""select nodeID, free_space, used_space, contact_date
                          from storage_nodeID""")
       _list={}
       for _nodeID, _free, _used, _timestamp, in _conn:
           print(_nodeID, _free, _used, _timestamp)
           try:
             _ipaddr=self._nodeID2ip_addr[_nodeID]
             _list[_ipaddr]={'free': _free, 'used': _used,
                             'number_segments': '0',
                             'timestamp': _timestamp, 'name': _ipaddr}
           except:
             print("dont know ip address of node %s" % _nodeID)
             pass

       _conn.execute("""select nodeID, count(*)
                          from storage_node_segments""")
       for _nodeID, _count, in _conn:
           try:
             _ipaddr=self._nodeID2ip_addr[_nodeID]
             _list[_ipaddr].update({'number_segments': _count})
           except:
             pass

       _conn.close()
       return _list

   def node_contents(self, ip_addr):
       _nodeID=self._ip_addr2nodeID[ip_addr]

       _db=sqlite3.connect(self._db_name)
       _conn=_db.cursor()

       _conn.execute("""select segmentID
                          from storage_node_segments 
                         where nodeID=?""", (_nodeID,))

       _list=[]
       for _segmentID, in _conn:
           _list.append(_segmentID)

       _conn.close()

       return _list

   def _segment_list_compare(self, segment_list1, segment_list2):
       _list=[]
       for _id in segment_list1:
           if _id not in segmenta_list2:
              _list.append(_id)

       return _list

   def _missing_segments(self, metadata_segmentIDs):
       """finds segment IDs in metadata, that are not stored in
          any of our storage nodes, Houston, we have a problem!"""

       _db=sqlite3.connect(self._db_name)
       _conn=_db.cursor()

       _conn.execute("""select distinct segmentID
                          from storage_node_segments""")

       _segmentIDs=[]
       for _segmentID, in _conn:
           _segmentIDs.append(_segmentID)

       _conn.close()

       self._segment_list_compare(self, metadata_segmentIDs, _segmentIDs)

   def find_unclaimed_segments(self, metadata_segmentIDs):
       """find segments located on storage nodes that are not in
          any metadata files"""

       _db=sqlite3.connect(self._db_name)
       _conn=_db.cursor()

       _conn.execute("""select distinct segmentID
                          from storage_node_segments""")

       _segmentIDs=[]
       for _segmentID, in _conn:
           _segmentIDs.append(_segmentID)

       _conn.close()

       return self._segment_list_compare(self, _segmentIDs, metadata_segmentIDs)

   def refresh(self, ipaddr=None):
       if ipaddr is not None:
          _ipaddrs=[ipaddr]
       else:
          #try to contact each node we know about.
          _ipaddrs=list(self._ip_addr2nodeID.keys())

       for _ipaddr in _ipaddrs:
           self._contact_storage_node(_ipaddr)

   def _contact_storage_node(self, ipaddr):
       #contact node and get info from them..
       _data=None
       try:
         _url="http://%s/Info" % ipaddr
         _fp=urllib.request.urlopen(_url)  
         _data=_fp.read()
         _fp.close()
       except urllib.request.HTTPError as e:
         print("HTTP Error:", e.code, _url)
       except urllib.request.URLError as e:
         print("URL Error:", e.reason, _url)

       if _data is not None:
          import gzip
          _json=gzip.decompress(_data)
          #self._update_storage_node_info(ipaddr, _data.decode('utf-8'))
          self._update_storage_node_info(ipaddr, _json.decode('utf-8'))
          return None

       return _data
