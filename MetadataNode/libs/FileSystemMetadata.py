#!/usr/bin/env python3

import os, json
import time
import hashlib

import sqlite3

import sys
sys.path.append("../Common")
import DB
from SegmentStorage import SegmentStorage as FileMetadataStorage

class FileSystemMetadata:
   def __init__(self, rootID=0, encrypted=False, 
                seed="seed", metadataDir="./MetaData",
                segmentLocator=None,
                DBFile="/tmp/FS.db"):
       self._seed=seed
       self._encrypted=encrypted
       self._rootID=rootID
       self._metadataDir=os.path.expanduser(metadataDir)
       self._DBFile=os.path.expanduser(DBFile)

       self._segmentLocator=segmentLocator

       self._file_metadata=FileMetadataStorage(self._metadataDir, 10*1024*1024)

       if os.path.exists(self._DBFile):
          _db=sqlite3.connect(self._DBFile)
          _conn=_db.cursor()

          _conn.execute("select max(id) from attributes")
          self._maxID,=_conn.fetchone()
          if self._maxID is None:
             self._maxID=0
          _conn.execute("select shaID from attributes where name='/'")
          self._root,=_conn.fetchone()

          _conn.execute("delete from missing_segments")
          _conn.execute("delete from metadata_segments")

          _db.commit()
          _db.close()
       else:
          self._maxID=0
          _db=sqlite3.connect(self._DBFile)
          _conn=_db.cursor()

          print("creating %s" % self._DBFile)
          _conn.execute("""create table attributes (
                                     id int,
                                     shaID text,
                                     type text,
                                     name text,
                                     size bigint,
                                     modified int)""")

          _conn.execute("""create index attributes_ndx on
                                      attributes(id, shaID)""")

          _conn.execute("""create index attributes_ndx1 on
                                      attributes(shaID, id)""")

          _str="%s:%s" % (self._seed, '/')
          self._root=self._calc_shaID(_str.encode('utf-8'))
          _conn.execute("""insert into attributes
                                values (0,?,'DIR','/',0,0)""", (self._root,))

          _conn.execute("""create table directory_contents (
                                     parentID bigint,
                                     childID  bigint)""")

          _conn.execute("""create table metadata_segments (
                                     fileID    int,
                                     segmentID text)""")

          _conn.execute("""create table missing_segments (
                                     shaID     text,
                                     segmentID text)""")

          _conn.execute("""create index missing_segments_ndx on
                                      missing_segments(shaID)""")

          _conn.execute("""create index metadata_segments_ndx on
                                      metadata_segments(fileID)""")

          _conn.execute("""create index contents_ndx on
                                      directory_contents(parentID)""")

          _conn.execute("""create index contents_ndx1 on
                                  directory_contents(childID)""")

          _db.commit()
          _db.close()

   def _shaID2fileID(self, conn, shaID):
       conn.execute("select id from attributes where shaID=?", (shaID,))
       try:
         _id,=conn.fetchone()
       except:
         raise ValueError
         return None

       return _id

   def _fileID2shaID(self, conn, id):
       conn.execute("select shaID from attributes where id=?", (id,))
       try:
         _shaID,=conn.fetchone()
       except:
         raise ValueError
         return None

       return _shaID

   def _calc_shaID(self, value):
       _sha1=hashlib.new('sha1')
       _sha1.update(value)
       return _sha1.hexdigest()

   def _segments_metadata_location(self, conn, segmentIDs):
       conn.execute("""delete from missing_segments""")

       if segmentIDs != []:
          conn.execute("""insert into missing_segments
                          select distinct segmentID, a.shaID 
                            from metadata_segments ms,
                                 attributes a
                           where ms.fileID=a.id
                             and ms.segmentID in ('%s')""" % ("','".join(segmentIDs)))

   def get_statistics(self):
       _db=sqlite3.connect(self._DBFile)
       _conn=_db.cursor()

       #get missing segments info
       _conn.execute("""select distinct shaID, segmentID
                               from missing_segments
                              order by shaID""")

       _missing=[]
       for _shaID, _segmentID, in _conn:
           _missing.append({'shaID': _shaID, 'segmentID': _segmentID})

       _db.close()
       print("missing", _missing)
       return _missing 

   def _find_files_with_missing_segments(self):
       _db=sqlite3.connect(self._DBFile)
       _conn=_db.cursor()

       _conn.execute("select distinct segmentID from metadata_segments")
       _all_segments=[]
       for _segmentID, in _conn:
           _all_segments.append(_segmentID)

       print("number of segments", len(_all_segments))
       _node_segments=self._segmentLocator.get_segment_list()

       #find segments that are missing
       _missing_segments=self._segment_list_compare(_all_segments, _node_segments)
       print("missing segments", _missing_segments)
       #now find out which files have missing segments and return
       #the shaIDs of those files
       self._segments_metadata_location(_conn, _missing_segments)
       _db.commit()
       _db.close()
       
   def _segment_list_compare(self, segment_list1, segment_list2):
       _list=[]
       for _id in segment_list1:
           if _id not in segment_list2:
              _list.append(_id)

       return _list

   def _add_segments(self, shaID, file_metadata):
       _db=sqlite3.connect(self._DBFile)
       _conn=_db.cursor()
       _fileID=self._shaID2fileID(_conn, shaID)

       for _groups in file_metadata:
           #print(_group)
           for _segment in _groups['segments']:
               #insert segmentID into database so we know all segments in
               #our metadata along with _shaID
               print(_segment['segmentID'])
               _conn.execute("insert into metadata_segments values (?,?)", (_fileID, _segment['segmentID'],))
       _db.commit()
       _db.close()

   def _del_segments(self, shaID):
       _db=sqlite3.connect(self._DBFile)
       _conn=_db.cursor()

       #write query to remove shaID from database
       _fileID=self._shaID2fileID(_conn, shaID)
       _conn.execute("delete from metadata_segments where fileID=?", (_fileID,))
       _db.commit()
       _db.close()

   def add_file_metadata(self, file_metadata):
       #print(file_metadata)
       _shaID=self._calc_shaID(file_metadata)
       #read file_metadata and retrieve the segmentID's so that we
       #know what segments should be in our grid
       _file_metadata=json.loads(file_metadata.decode('utf-8'))
       self._add_segments(_shaID, _file_metadata)

       self._file_metadata.Put(_shaID, file_metadata)

   def remove_file_metadata(self, shaID):
       #remove all segments in our database who are owned by this fileID
       self._del_segments(shaID)
       self._file_metadata.Remove(shaID)
       #remove metadata from OS filesystem
       return None

   def refresh(self):
       self._find_files_with_missing_segments()

   def get_file_metadata(self, shaID):
       #find metadata location and send file contents
       return self._file_metadata.Get(shaID)

   def listdir(self, id):
       _db=sqlite3.connect(self._DBFile)
       _conn=_db.cursor()

       _parentid=self._shaID2fileID(_conn, id)

       _conn.execute("""select id, shaID, type, name, modified, size
                                  from directory_contents a,
                                       attributes b
                                 where a.childID=b.id
                                   and a.parentID=?""", (_parentid,))

       _list=[]
       for _id, _shaID, _type, _name, _modified, _size, in _conn:
           _list.append({'id': _id, 'type': _type, 'name': _name,
                         'modified': _modified, 'size': _size})
       _db.close()

       return _list

   def rmdir(self, parentID, id, name):
       _db=sqlite3.connect(self._DBFile)
       _conn=_db.cursor()

       _parentID=self._shaID2fileID(_conn, parentID)
       _id=self._shaID2fileID(_conn, id)
       
       _conn.execute("""select count(*)
                          from directory_contents
                         where parentID=?""", (_id,))

       _count,=_conn.fetchone()
       if _count == 0:
          _conn.execute("""delete
                             from directory_contents
                            where parentID=?
                              and childID=?""", (_parentID, _id,))
          _conn.execute("""delete
                             from attributes
                            where shaID=?""", (id,))
          _db.commit()
          _db.close()
          return None

       _db.close()
       return "Directory is not empty"

   def _get_new_id(self):
       self._maxID+=1
       return self._maxID

   def mkdir(self, parentID, shaID, name):
       _db=sqlite3.connect(self._DBFile)
       _conn=_db.cursor()

       _parentID=self._shaID2fileID(_conn, parentID)

       _conn.execute("""select count(*)
                          from attributes a,
                               directory_contents b
                         where a.id=b.childID
                           and b.parentID=?
                           and a.shaID=?
                           and a.name=?""", (_parentID, shaID, name,))

       _count,=_conn.fetchone()
       if _count==0:  # we don't have a child with this name, so we can create it
          _id=self._get_new_id()
          print("%s,%s,%s" % (_id, shaID, name))
          _conn.execute("""insert into attributes 
                              values (?,?, 'DIR',?,0,?)""", (_id, shaID, name, int(time.time()),))
 
          _conn.execute("""insert into directory_contents 
                                   values (?,?)""", (_parentID, _id,))
          _db.commit()
          _db.close()
          return None

       _db.close()
       return "Error! An object already exists with that id/name"

   def putfile(self, parentID, shaID, name, size):
       _db=sqlite3.connect(self._DBFile)
       _conn=_db.cursor()

       _parentID=self._shaID2fileID(_conn, parentID)

       _conn.execute("""select count(a.id)
                                  from attributes a,
                                       directory_contents b
                                 where a.id=b.childID
                                   and b.parentID=?
                                   and a.shaID=?
                                   and a.name=?""", (_parentID, shaID, name,))

       _count,=_conn.fetchone()
       if _count==0:  # we don't have a child with this name, so we can create it
          _id=self._get_new_id()
          _conn.execute("""insert into attributes 
                              values (?, ?,'FILE',?,?,?)""", (_id, shaID, name, size, int(time.time())))
 
          _conn.execute("""insert into directory_contents 
                                  values (?,?)""", (_parentID, _id,))
          _db.commit()
          _db.close()
          return None

       _db.close()
       return "Error, file already exists"

   def rmfile(self, dir_id, shaID):
       #print(dir_id, shaID)
       _db=sqlite3.connect(self._DBFile)
       _conn=_db.cursor()

       _parentID=self._shaID2fileID(_conn, dir_id)
       _id=self._shaID2fileID(_conn, shaID)
       #_fileID=self.get_fileID(dir_id, shaID)
       #print(_fileID)

       #if self._get_file_locations(_conn, shaID)==1:
       _conn.execute("delete from attributes where id=?", (_id,))
       
       _conn.execute("""delete from directory_contents 
                       where parentID=? and childID=?""", (_parentID, _id,))
       print("parentID, childID", _parentID, _id)
       _db.commit()
       _db.close()

       return None

   #look at caching some of these values so that lookups are faster.
   def _traverse_path(self, conn, dirlist, parentID=0):
       conn.execute("""select a.childID
                             from directory_contents a,
                                  attributes b
                            where b.id=a.childID
                              and b.name=?
                              and a.parentID=?""", (dirlist[0], parentID,))

       try:
          _id,=conn.fetchone()
       except:
          return None

       if _id is None:
          return None

       if len(dirlist)==1: return self._fileID2shaID(conn, _id)

       return self._traverse_path(conn, dirlist[1:], _id)

   #name is pathname + filename
   def lookupID(self, name):
       if name==os.path.sep: return self._root
       _dirs=name.split(os.path.sep)

       while _dirs[0] == '' and len(_dirs) > 1:
          _dirs=_dirs[1:]

       _db=sqlite3.connect(self._DBFile)
       _conn=_db.cursor()
 
       _result=self._traverse_path(_conn, _dirs)

       _db.close()

       return _result

   def get_fileID(self, conn, dir_id, shaID):
       _conn.execute("""select b.id
                             from directory_contents a,
                                  attributes b
                            where b.id=a.childID
                              and b.shaID=?
                              and a.parentID=?""", (shaID,dir_id,))

       try:
         _fileID,=_conn.fetchone()
       except:
         _fileID=None

       return _fileID

   def get_file_locations(self, shaID):
       _db=sqlite3.connect(self._DBFile)
       _conn=_db.cursor()
 
       _dir_ids=self._get_file_locations(_conn, shaID)       
       _db.close()

       return _dir_ids

   def _get_file_locations(self, conn, shaID):
       conn.execute("""select a.parentID
                             from directory_contents a,
                                  attributes b
                            where b.id=a.childID
                              and b.shaID=?""", (shaID,))
       _dir_ids=[]
       for _id in conn:
           _dir_ids.append(_id)

       return _dir_ids 

if __name__== '__main__':
   _fs=FileSystemMetaData()
   print(_fs.listdir(0))
   #print(_fs.mkdir(2, 'mytest'))
   print(_fs.listdir(1))
   print(_fs.listdir(2))
   print(_fs.rmdir(0))
