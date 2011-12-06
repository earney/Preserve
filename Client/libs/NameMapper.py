import os, sys
import json

sys.path.append("../Common")
import DB, misc

class NameMapper:
  def __init__(self, encryption=False, metadataNode=[]):
      self._encryption=encryption

      if encryption:
         self._module=LocalNameMapper()
      else:
         self._module=DefaultNameMapper(metadataNode)

      self.Name2ID=self._module.Name2ID
      self.ID2Name=self._module.ID2Name  #is this needed? ever?
 
      if encryption:
         self.ADD=self._module.Add
         self.UpdateID=self._module.UpdateID
         self.UpdateName=self._module.UpdateName
         self.Delete=self._module.Delete

  def __del__(self):
      del self._module

class LocalNameMapper:
  def __init__(self, filename="names.db"):
      if os.path.exists(filename):
         self._db=DB.DB(filename)
      else:
         self._db=DB.DB(filename)
         #name contains the path and filename
         self._db.execute("""create table mapper (id   text,
                                                  name text)""")

         self._db.commit()

  def Name2ID(self, name):
      self._db.execute("select id from mapper where name=?", (name,))

      try:
         _id,=self._db.fetchone()
      except:
         return None

      return _id

  def ID2Name(self, id):
      self._db.execute("select name from mapper where id=?", (id,))

      try:
         _name,=self._db.fetchone()
      except:
         return None

      return _name

  def Add(self, id, name):
      self._db.execute("insert into mapper values(?,?)", (id, name,))
      self._db.commit()

  def UpdateID(self, id, name):
      self._db.execute("update mapper set id=? where name=?", (id, name,))
      self._db.commit()

  def UpdateName(self, id, name):
      self._db.execute("update mapper set name=? where id=?", (name, id,))
      self._db.commit()

  def Delete(self, name):
      self._db.execute("delete from mapper where name=?", (name,))
      self._db.commit()
      return None

 
class DefaultNameMapper:
  def __init__(self, metadataNode):
      self._metadataNode=metadataNode

  def Name2ID(self, name):
      if name.startswith('grid:/'):
         name=name[6:]
      _result=misc.access_url("http://%s/Client/Name2ID/%s" % (self._metadataNode, name))
      if _result is None: return None
      
      return json.loads(_result.decode('utf-8'))
      #lookup on metadata node the id of this name (name is path+filename)

  def ID2Name(self, id):
      #this function is probably not needed since the name of a file
      #will be returned when we list the contents of a directory.
      #I could be wrong, so lets just pass
      pass      
