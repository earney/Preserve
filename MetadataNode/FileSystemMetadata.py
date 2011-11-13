#!/usr/bin/env python3

import os, json
import time
import hashlib

import sys
sys.path.append("../Common")
import DB
from SegmentStorage import SegmentStorage as FileMetadataStorage

class FileSystemMetadata:
   def __init__(self, rootID=0, encrypted=False, 
                seed="seed", metadataDir="./MetaData",
                DBFile="/tmp/FS.db"):
       self._seed=seed
       self._encrypted=encrypted
       self._rootID=rootID
       self._metadataDir=os.path.expanduser(metadataDir)
       self._DBFile=os.path.expanduser(DBFile)

       self._file_metadata=FileMetadataStorage(self._metadataDir, 10*1024*1024)

       if os.path.exists(self._DBFile):
          self._db=DB.DB(self._DBFile)

          self._db.execute("select max(id) from attributes")
          self._maxID,=self._db.fetchone()
          if self._maxID is None:
             self._maxID=0
          self._db.execute("select shaID from attributes where name='/'")
          self._root,=self._db.fetchone()
       else:
          self._maxID=0
          print("creating %s" % self._DBFile)
          self._db=DB.DB(self._DBFile)
          self._db.ExecuteQuery("""create table attributes (
                                     id bigint,
                                     shaID text,
                                     type text,
                                     name text,
                                     size bigint,
                                     modified int)""")

          self._db.ExecuteQuery("""create index attributes_ndx on
                                      attributes(id, shaID)""")

          self._db.ExecuteQuery("""create index attributes_ndx1 on
                                      attributes(shaID, id)""")

          _str="%s:%s" % (self._seed, '/')
          _shaID=self._calc_shaID(_str.encode('utf-8'))
          self._root=_shaID
          self._db.ExecuteQuery("""insert into attributes
                                   values (0,?,'DIR','/',0,0)""", (_shaID,))

          self._db.ExecuteQuery("""create table directory_contents (
                                     parentID bigint,
                                     childID  bigint)""")

          self._db.ExecuteQuery("""create index contents_ndx on
                                      directory_contents(parentID)""")

          self._db.ExecuteQuery("""create index contents_ndx1 on
                                      directory_contents(childID)""")

          self._db.commit()

   def __del__(self):
       del self._db

   def _shaID2fileID(self, shaID):
       self._db.execute("select id from attributes where shaID=?", (shaID,))
       try:
         _id,=self._db.fetchone()
       except:
         return None

       return _id

   def _fileID2shaID(self, id):
       self._db.execute("select shaID from attributes where id=?", (id,))
       _shaID,=self._db.fetchone()
       return _shaID

   def _calc_shaID(self, value):
       _sha1=hashlib.new('sha1')
       _sha1.update(value)
       return _sha1.hexdigest()
       
   def add_file_metadata(self, file_metadata):
       _shaID=self._calc_shaID(file_metadata)
       self._file_metadata.Put(_shaID, file_metadata)

   def remove_file_metadata(self, shaID):
       #_fileID=self._sha2fileID(shaID)
       #remove metadata from OS filesystem

       return None

   def get_file_metadata(self, shaID):
       #find metadata location and send file contents
       return self._file_metadata.Get(shaID)

   def listdir(self, id):
       _parentid=self._shaID2fileID(id)
       self._db.ExecuteQuery("""select id, shaID, type, name, modified, size
                                  from directory_contents a,
                                       attributes b
                                 where a.childID=b.id
                                   and a.parentID=?""", (_parentid,))

       _list=[]
       for _id, _shaID, _type, _name, _modified, _size, in self._db:
           #if _type=='FILE':
           #   _id=_shaID
           _list.append({'id': _id, 'type': _type, 'name': _name,
                         'modified': _modified, 'size': _size})

       return _list

   def rmdir(self, parentID, id, name):
       _parentID=self._shaID2fileID(parentID)
       _id=self._shaID2fileID(id)
       
       #fix me  id is a shaID, but the tables below expect an int (fileID)
       self._db.ExecuteQuery("""select count(*)
                                  from directory_contents
                                 where parentID=?""", (_id,))

       _count,=self._db.fetchone()
       if _count == 0:
          self._db.ExecuteQuery("""delete
                                     from directory_contents
                                    where parentID=?
                                      and childID=?""", (_parentID, _id,))
          self._db.ExecuteQuery("""delete
                                     from attributes
                                    where shaID=?""", (id,))
          self._db.commit()
          return None

       return "Directory is not empty"

   def _get_new_id(self):
       self._maxID+=1
       return self._maxID

   def mkdir(self, parentID, shaID, name):
       _parentID=self._shaID2fileID(parentID)

       self._db.ExecuteQuery("""select count(*)
                                  from attributes a,
                                       directory_contents b
                                 where a.id=b.childID
                                   and b.parentID=?
                                   and a.shaID=?
                                   and a.name=?""", (_parentID, shaID, name,))

       _count,=self._db.fetchone()
       if _count==0:  # we don't have a child with this name, so we can create it
          _id=self._get_new_id()
          print("%s,%s,%s" % (_id, shaID, name))
          self._db.execute("""insert into attributes 
                              values (?,?, 'DIR',?,0,?)""", (_id, shaID, name, int(time.time()),))
 
          self._db.execute("""insert into directory_contents 
                                   values (?,?)""", (_parentID, _id,))
          self._db.commit()
          return None

       return "Error! An object already exists with that id/name"

   def putfile(self, parentID, shaID, name, size):
       _parentID=self._shaID2fileID(parentID)

       self._db.ExecuteQuery("""select count(a.id)
                                  from attributes a,
                                       directory_contents b
                                 where a.id=b.childID
                                   and b.parentID=?
                                   and a.shaID=?
                                   and a.name=?""", (_parentID, shaID, name,))

       _count,=self._db.fetchone()
       if _count==0:  # we don't have a child with this name, so we can create it
          _id=self._get_new_id()
          self._db.execute("""insert into attributes 
                              values (?, ?,'FILE',?,?,?)""", (_id, shaID, name, size, int(time.time())))
 
          self._db.execute("""insert into directory_contents 
                                   values (?,?)""", (_parentID, _id,))
          self._db.commit()
          return None

       return "Error, file already exists"

   def rmfile(self, dir_id, shaID):
       #print(dir_id, shaID)
       _parentID=self._shaID2fileID(dir_id)
       _id=self._shaID2fileID(shaID)
       #_fileID=self.get_fileID(dir_id, shaID)
       #print(_fileID)

       if self.get_file_locations(shaID)==1:
          self._db.execute("delete from attributes where shaID=?", (shaID,))
       
       self._db.execute("""delete from directory_contents 
                            where parentID=? and childID=?""", (_parentID, _id,))
       self._db.commit()

       return None

   #look at caching some of these values so that lookups are faster.
   def _traverse_path(self, dirlist, parentID=0):
       self._db.execute("""select a.childID
                             from directory_contents a,
                                  attributes b
                            where b.id=a.childID
                              and b.name=?
                              and a.parentID=?""", (dirlist[0], parentID,))

       _id,=self._db.fetchone()
       if _id is None:
          return None

       if len(dirlist)==1: return self._fileID2shaID(_id)

       return self._traverse_path(dirlist[1:], _id)

   #name is pathname + filename
   def lookupID(self, name):
       if name==os.path.sep: return self._root
       _dirs=name.split(os.path.sep)

       while _dirs[0] == '' and len(_dirs) > 1:
          _dirs=_dirs[1:]

       return self._traverse_path(_dirs)

   def get_fileID(self, dir_id, shaID):
       self._db.execute("""select b.id
                             from directory_contents a,
                                  attributes b
                            where b.id=a.childID
                              and b.shaID=?
                              and a.parentID=?""", (shaID,dir_id,))

       try:
         _fileID,=self._db.fetchone()
       except:
         _fileID=None

       return _fileID

   def get_file_locations(self, shaID):
       self._db.execute("""select a.parentID
                             from directory_contents a,
                                  attributes b
                            where b.id=a.childID
                              and b.shaID=?""", (shaID,))
       _dir_ids=[]
       for _id in self._db:
           _dir_ids.append(_id)

       return _dir_ids 

if __name__== '__main__':
   _fs=FileSystemMetaData()
   print(_fs.listdir(0))
   #print(_fs.mkdir(2, 'mytest'))
   print(_fs.listdir(1))
   print(_fs.listdir(2))
   print(_fs.rmdir(0))
