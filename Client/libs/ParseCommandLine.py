import json
import sys, os
import hashlib

import NameMapper
#sys.path.append("../../Common")
sys.path.append("../Common")
import misc

class ParseCommandLine:
   def __init__(self, metadataNode, encryption=False, seed="myDIR"):
       self._metadataNode=metadataNode
       self._encryption=encryption
       self._seed=seed
       self._sys_args=[]

       self._name_mapper=NameMapper.NameMapper(encryption=encryption, metadataNode=self._metadataNode)

   def __del__(self):
       del self._name_mapper

   def process_command(self, args):
       self._sys_args=args

       if len(args) < 1:
          return "Error!, must provide command"

       #Client <command> <command line options>
       _command=self._sys_args[1]
       if self._valid_command(_command):
          return self._parse(_command)

       return "Error!, Command is not valid"

   def _valid_command(self, s):
       if s in ('cd', 'ls', 'mkdir', 'rmdir'):
          return True
       return False 

   def _parse(self, command):
       if command=='ls':
          _result=self._ls_input()
          try:
            _result=int(_result)
          except:
            pass
          if isinstance(_result, int): 
             return self._ls_output(_result)
          return _result
       elif command=='mkdir':
          _parentID, _name_id, _name=self._dir_input()
          if _name is None:  #this is an Error!
             return "Error, you must provide a directory name"
          return self._dir_output("mkdir", _parentID, _name_id, _name)
       elif command=='rmdir':
          _parentID, _name_id, _name=self._dir_input()
          if _name is None:  #this is an Error!
             return "Error, you must provide a directory name"
          return self._dir_output("rmdir", _parentID, _name_id, _name)
       else:
          return "Error! command %s is not valid" % command

   def _calc_shaID(self, value):
       _sha1=hashlib.new('sha1')
       _sha1.update(value)
       return _sha1.hexdigest()


   def _dir_output(self, command, parentID, name_id, name):
       if self._encryption:
          name=None
       _url="http://%s/Client/%s/%s/%s/%s" % (self._metadataNode, command, parentID, name_id, name)
       _result=misc.access_url(_url)
       print(_result)
       return json.loads(_result.decode('utf-8'))

   def _dir_input(self):
       try:
         _name=self._sys_args[2]
       except:
         return None, None, None

       #print(_name)
       if _name=='grid:/': return None, None, None  #user didn't provide a directory name
       if _name.startswith('grid:/'):
          _name=_name[5:]
          _path, _dir_name=os.path.split(_name)
          #print (_path)
          _dir_id=self._lookup_id(_path)
          print(_dir_id)
          _str="%s:%s" % (self._seed, _dir_name)
          _name_id=self._calc_shaID(_str.encode('utf-8'))

          return _dir_id, _name_id, _dir_name

       return None, None, None


   def _ls_input(self):
       try:
         _dir=self._sys_args[2]
       except:
         _dir='grid:/'

       if _dir.startswith('grid:/'):
          #we want to query our grid
          _dir_id=self._lookup_id(_dir[5:])          
          if _dir_id is None:
             return "Error, directory %s doesn't exist" % _dir
          return _dir_id
       else:
          #looking at regular OS (doesn't make sense for LS) so return error
          return "Error, ls not supported on non grid directories"
        
   def _ls_output(self, dir_id):
       _url="http://%s/Client/listdir/%s" % (self._metadataNode, dir_id)
       _result=misc.access_url(_url)
       _result=json.loads(_result.decode('utf-8'))

       _items={}
       _items['FILE']={}
       _items['DIR']={}
       for _dict in _result:
           _items[_dict['type']][_dict['name']]=_dict

       _str='\n'
       for _type in ('DIR', 'FILE'):
           _dirs=list(_items[_type].keys())
           _dirs.sort()
           #print out directory contents
           for _dir in _dirs:
               _dict=_items[_type][_dir]
               #size  modified time, name"
               #print(_dir)
               _date_time='%s' % _items[_type][_dir]['modified']
               _padding=' '*(15-len(_date_time))
               _str+="%s %s %10d %s %s %s\n" % (_type[0], _dict['id'], _dict['size'], _date_time, _padding, _dict['name'])

       return _str
 
   #todo
   def _lookup_id(self, path):
       #send path to metadatanode and expect a dir_id (or null if 
       #it doesn't exist
       return self._name_mapper.Name2ID(path)

   def _rm_file(self, filename):
       _dirID, _shaID=self._lookup_fileID(filename)
       if _shaID is not None:
          _url="http://%s/Client/rmfile/%s/%s" % (self._metadataNode, _dirID, _fileID)
          _result=misc.access_url(_url)
          _result=json.loads(_result.decode('utf-8'))
          return _result
       
       return "File does not exist."

   #todo
   def _lookup_fileID(pathfile):
       #returns dirID and shaID of the file
       #look up fileID of the given pathfile(name)
       pass

   def _cp_object(self, object1, object2):
       #get dirID (and possibliy shaID, if file) of object 1
       pass

if __name__=='__main__':
   _pcl=ParseCommandLine("127.0.0.1:9696")
   _cmd="prog ls grid:/"
   _args=_cmd.split()
   print(_pcl.process_command(_args))
