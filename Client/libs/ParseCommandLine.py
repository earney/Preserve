import json, time
import sys, os
import hashlib

import NameMapper
#sys.path.append("../../Common")
sys.path.append("../Common")
import DisassembleFile
import misc

class ParseCommandLine:
   def __init__(self, mdn_handler, encryption=False, seed="myDIR"):
       #for now default to first metadatanode in list but eventually create
       #function to find working master metadata node, etc
       self._mdn_handler=mdn_handler
       self._encryption=encryption
       self._seed=seed
       self._sys_args=[]

       #TODO: don't use a name mapper, encrypt name instead, that way
       #only private key is necessary to have.
       self._name_mapper=NameMapper.NameMapper(self._mdn_handler)

   def __del__(self):
       del self._name_mapper

   def process_command(self, args):
       self._sys_args=args

       if len(args) < 2:
          return "Error!, must provide command"

       #Client <command> <command line options>
       _command=self._sys_args[1]
       if self._valid_command(_command):
          return self._parse(_command)

       return "Error!, Command is not valid"

   def _valid_command(self, s):
       #todo add rm, mv
       if s in ('cd', 'ls', 'mkdir', 'rmdir', 'cp', 'rm', 'mv'):
          return True
       return False 

   def _parse(self, command):
       if command=='ls':
          _result=self._ls_input()
          if isinstance(_result, str) and len(_result)==40: 
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
       elif command=='cp':
          return self._cp_input()
       elif command=='rm':
          return self._rm_input()
       elif command=='mv':
          return self._mv_input()
       else:
          return "Error! command %s is not valid" % command

   # this is has been replaced with misc.get_shaID
   def _calc_shaID(self, value):
       _sha1=hashlib.new('sha1')
       _sha1.update(value)
       return _sha1.hexdigest()

   def _dir_output(self, command, parentID, name_id, name):
       if self._encryption:
          name=None
       #_url="http://%s/Client/%s/%s/%s/%s" % (self._metadataNode, command, parentID, name_id, name)
       _result=self._mdn_handler.send_message('/Client/%s/%s/%s/%s' % (command, parentID, name_id, name))

       #_result=misc.access_url(_url)
       if command in ('mkdir') and _result is None:
          return None
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

   def _xx_grid2grid(self, command, source, target):
       _dirID1, _shaID1=self._lookup_fileID(source)
       print("%s, %s, %s" % (source[5:], _dirID1, _shaID1))
       _head, _tail=os.path.split(target[5:])
       print("%s,%s,%s" % (target, _head, _tail))
       _dirID2=self._lookup_id(_head)

       if _tail=='' and command=='cp':
          #copy the filename from the source
          _, _tail=os.path.split(_source[5:])

       return self._mdn_handler.send_message('/Client/%s/%s/%s/%s/%s' % (command, _dirID1, _shaID1, _dirID2, _tail))


   def _mv_input(self):
       if len(self._sys_args) != 4:
          return "Error.. invalid syntax!\preserve.py cp <source> <target>"

       _source=self._sys_args[2]
       _target=self._sys_args[3]

       if _source.startswith("grid:/") and _target.startswith("grid:/"):
          #move file within grid
          _result=self._xx_grid2grid("mv", _source, _target)
          if _result is None:  #the move worked!
             _result=self._rm_file()
       elif _source.startswith("grid:/"):
          #move file/dir out of grid
          _id=self._lookup_id(source)
          import AssembleFile
          _as=AssembleFile.AssembleFile(self._mdn_handler)
          _result=_as.process(_id, target)
          if _result is None:
             #remove dir/file in grid
             return self._rm_file(_filename)
          return "Error! copying dir/file out of grid failed"
       elif _target.startswith("grid:/"):
          #move file/dir into grid
          _target_dir, _target_name=os.path.split(_target[5:])
          _parentID=self._lookup_id(_target_dir)
          _df=DisassembleFile.DisassembleFile(_source, _parentID, _target_name, 
                                              self._mdn_handler)
          _result=_df.process()
          if _result is None:
             import shutil
             shutil.remove(_source)
             return None
          return "Error! Trouble copying file into grid"
       return "Error! source and/or target must start with grid:/"


   def _cp_input(self):
       if len(self._sys_args) != 4:
          return "Error.. invalid syntax!\preserve.py cp <source> <target>"

       _source=self._sys_args[2]
       _target=self._sys_args[3]

       if _source.startswith("grid:/") and _target.startswith("grid:/"):
          #grid to grid
          #just have to copy metadata
          return self._xx_grid2grid('cp', _source, _target)
       elif _source.startswith("grid:/"):
          #copy file out of grid
          #lookup id of source file
          _id=self._lookup_id(_source)
          if _id is None: #file doesn't exist in grid!
             return "Error! file %s does not exist" % _source
          import AssembleFile
          _as=AssembleFile.AssembleFile(self._mdn_handler)
          #TODO
          #need to check if target exists, if so, maybe prompt user
          #to over write?
          return _as.process(_id, _target)
       elif _target.startswith("grid:/"):
          #copy file into the grid
          _target_dir, _target_name=os.path.split(os.path.expanduser(_target[5:]))
          _parentID=self._lookup_id(_target_dir)
          _df=DisassembleFile.DisassembleFile(_source, _parentID, _target_name, 
                                              self._mdn_handler)
          return _df.process()

       return "Error! source and/or target must start with grid:/"

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
       #_url="http://%s/Client/listdir/%s" % (self._metadataNode, dir_id)
       #_result=misc.access_url(_url)
       _result=self._mdn_handler.send_message('/Client/listdir/%s' % dir_id)
       _result=json.loads(_result.decode('utf-8'))

       _str='\n'
       for _dict in _result:
           try:
             _time_sec=int(_dict['modified'])
             _date_time=time.ctime(_time_sec)
           except:
             _date_time=''

           _padding=' '*(25-len(_date_time))
           _padding1=' '*(5-len(_dict['type']))
           _str+="%s%s%s %10d %s %s %s\n" % (_dict['type'], _padding1, _dict['id'], _dict['size'], _date_time, _padding, _dict['name'])

       return _str
 
   def _lookup_id(self, path):
       #send path to metadatanode and expect a dir_id (or null if 
       #it doesn't exist
       return self._name_mapper.Name2ID(path)

   def _rm_input(self):
       _filename=self._sys_args[2]
       if not _filename.startswith('grid:/'):
          return "Error! can only delete files in the grid"

       return self._rm_file(_filename)

   def _rm_file(self, filename):
       _dirID, _shaID=self._lookup_fileID(filename)
       if _shaID is not None:
          #_url="http://%s/Client/rmfile/%s/%s" % (self._metadataNode, _dirID, _shaID)
          #_result=misc.access_url(_url)
          _result=self._mdn_handler.send_message('/Client/rmfile/%s/%s' % (_dirID, _shaID))
          if _result is None:
             _result=''
          #_result=json.loads(_result.encode('utf-8'))
          return _result
       
       return "File does not exist."

   #todo
   def _lookup_fileID(self, pathfile):
       #returns dirID and shaID of the file
       #look up fileID of the given pathfile(name)
       _target_dir, _target_name=os.path.split(pathfile[5:])
       _parentID=self._lookup_id(_target_dir)
       _shaID=self._lookup_id(pathfile[5:])

       return _parentID, _shaID
