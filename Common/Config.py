import configparser, os

# config file "~/.preserve/.config
#[Grid]
#MetaDataNodeAddresses=127.0.0.1:9696

#[StorageNode]
#Address=127.0.0.1
#Port=9695
#Path="~/.preserve/Storage"
#Quota=107374182400   #100 GB

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
         try:
           self._StorageNodePort=int(self._parser.get('StorageNode', 'Port'))
         except:
           self._StorageNodePort=9695
      else:
         self._StorageNodePort=9695

      if self._parser.has_option('StorageNode', 'StoragePath'):
         self._StorageNodePath=os.path.expanduser(self._parser.get('StorageNode', 'StoragePath'))
      else:
         self._StorageNodePath=os.path.expanduser("~/.preserve/Storage")

      if self._parser.has_option('StorageNode', 'Quota'):
         self._StorageNodeQuota=self._parser.get('StorageNode', 'Quota')
      else:
         self._StorageNodeQuota=107374182400  #100 GB

      if self._parser.has_option('StorageNode', 'CertFile'):
         self._SN_CertFile=os.path.expanduser(self._parser.get('StorageNode', 'CertFile'))
      else:
         self._SN_CertFile=None

      if self._parser.has_option('MetadataNode', 'Address'):
         self._MetadataNodeAddress=self._parser.get('MetadataNode', 'Address')
      else:
         self._MetadataNodeAddress='0.0.0.0'

      if self._parser.has_option('MetadataNode', 'Port'):
         self._MetadataNodePort=self._parser.get('MetadataNode', 'Port')
      else:
         self._MetadataNodePort='9696'

      if self._parser.has_option('MetadataNode', 'CertFile'):
         self._MDN_CertFile=os.path.expanduser(self._parser.get('MetadataNode', 'CertFile'))
      else:
         self._MDN_CertFile=None

      if self._parser.has_option('MetadataNode', 'SegmentLocatorDB'):
         self._SegmentLocatorDB=os.path.expanduser(self._parser.get('MetadataNode', 'SegmentLocatorDB'))
      else:
         self._SegmentLocatorDB=os.path.expanduser("~/.preserve/SegmentLocator.db")

      if self._parser.has_option('MetadataNode', 'FileSystemDirectory'):
         self._FileSystemDirectory=os.path.expanduser(self._parser.get('MetadataNode', 'FileSystemDirectory'))
      else:
         self._FileSystemDirectory=os.path.expanduser("~/.preserve/MDN/")

      if self._parser.has_option('MetadataNode', 'FileSystemDB'):
         self._FileSystemDB=os.path.expanduser(self._parser.get('MetadataNode', 'FileSystemDB'))
      else:
         self._FileSystemDB=os.path.expanduser("~/.preserve/FS.db")

  def get_MetadataNodes(self):
      return self._MetadataNodes

  def get_SN_Address(self):
      return self._StorageNodeAddress, self._StorageNodePort

  def get_SN_CertFile(self):
      return self._SN_CertFile

  def get_SN_Path(self):
      return self._StorageNodePath

  def get_SN_Quota(self):
      return self._StorageNodeQuota

  def get_MDN_Address(self):
      return self._MetadataNodeAddress, self._MetadataNodePort

  def get_MDN_SegmentLocatorDB(self):
      return self._SegmentLocatorDB

  def get_MDN_FileSystemDirectory(self):
      return self._FileSystemDirectory

  def get_MDN_FileSystemDB(self):
      return self._FileSystemDB

  def get_MDN_CertFile(self):
      return self._MDN_CertFile
