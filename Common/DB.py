import os

try:
  import sqlite3
  _dbtype='sqlite3'
except:
  import gadfly
  _dbtype='gadfly'

class DB:
   def __init__(self, name, auto_commit=True):
       self._auto_commit=auto_commit
       self.ExecuteQuery=self.execute

       self._conn=sqlite3.connect(name)
       self._db=self._conn.cursor()

   def __del__(self):
       if self._auto_commit:
          self.commit()

       self._db.close()

   def commit(self):
       self._conn.commit()

   def execute(self, query, values=None):
       if values is not None:
          self._db.execute(query, values)
       else:
          self._db.execute(query)

       if self._auto_commit:
          self.commit()


   def __iter__(self):
       return self._db

   def __iter__gadfly(self):
       return self._db.fetchall()

   def __next__(self):
       return self._db.next()

   def fetchone(self):
       return self._db.fetchone()


class DB_ThreadSafe:
   def __init__(self, name, auto_commit=True):
       self._auto_commit=auto_commit
       self.ExecuteQuery=self.execute
       self._name=name

   def start_transaction(self):
       self._conn=sqlite3.connect(self._name)
       self._db=self._conn.cursor()

   def end_transaction(self):
       self._db.close()
       del self._conn

   def __del__(self):
       if self._auto_commit:
          self.commit()

       self._db.close()

   def commit(self):
       self._conn.commit()

   def execute(self, query, values=None):
       self._start_()
       if values is not None:
          self._db.execute(query, values)
       else:
          self._db.execute(query)

       if self._auto_commit:
          self.commit()


   def __iter__(self):
       return self._db

   def __next__(self):
       return self._db.next()

   def fetchone(self):
       return self._db.fetchone()
