import configparser, os

# config file "~/.preserve/.config
#[Grid]
#MetaDataNodeAddresses=127.0.0.1:9696

#[StorageNode]
#Address=127.0.0.1
#Port=9695

#[MetaDataNode]
#Address=127.0.0.1
#Port=9696

class Config:
  def __init__(self, filename="~/.preserve/.config"):
      filename=os.path.expanduser(filename)
      self._parser=configparser.SafeConfigParser()
      self._parser.read(filename)

      if not self._parser.has_option('Grid', 'MetaDataNodeAddresses'):
         import sys
         sys.stderr.write("Error, must provide [Grid] and MetaDataNodeAddresses in %s" % filename)
         sys.exit()

      _address=self._parser.get('Grid', 'MetaDataNodeAddresses')
      self._MetadataNodes=_address.split(',')

      if self._parser.has_option('StorageNode', 'Address'):
         self._StorageNodeAddress=self._parser.get('StorageNode', 'Address')
      else:
         self._StorageNodeAddress='0.0.0.0'

      if self._parser.has_option('StorageNode', 'Port'):
         self._StorageNodePort=self._parser.get('StorageNode', 'Port')
      else:
         self._StorageNodePort='9695'

      if self._parser.has_option('MetadataNode', 'Address'):
         self._MetadataNodeAddress=self._parser.get('MetadataNode', 'Address')
      else:
         self._MetadataNodeAddress='0.0.0.0'

      if self._parser.has_option('MetadataNode', 'Port'):
         self._MetadataNodePort=self._parser.get('MetadataNode', 'Port')
      else:
         self._MetadataNodePort='9696'

      if self._parser.has_option('MetadataNode', 'SegmentLocatorDB'):
         self._SegmentLocatorDB=self._parser.get('MetadataNode', 'SegmentLocatorDB')
      else:
         self._SegmentLocatorDB="~/.preserve/SegmentLocator.db"

      if self._parser.has_option('MetadataNode', 'FileSystemDirectory'):
         self._FileSystemDirectory=self._parser.get('MetadataNode', 'FileSystemDirectory')
      else:
         self._FileSystemDirectory="~/.preserve/MDN/"

      if self._parser.has_option('MetadataNode', 'FileSystemDB'):
         self._FileSystemDB=self._parser.get('MetadataNode', 'FileSystemDB')
      else:
         self._FileSystemDB="~/.preserve/FS.db"

  def get_MetadataNodes(self):
      return self._MetadataNodes

  def get_SN_Address(self):
      return self._StorageNodeAddress, self._StorageNodePort

  def get_MDN_Address(self):
      return self._MetadataNodeAddress, self._MetadataNodePort

  def get_MDN_SegmentLocatorDB(self):
      return self._SegmentLocatorDB

  def get_MDN_FileSystemDirectory(self):
      return self._FileSystemDirectory

  def get_MDN_FileSystemDB(self):
      return self._FileSystemDB
