import configparser

# config file "~/.preserve/.config
# [MetaDataNodes]
# Address=127.0.0.1:123,MyMDN:456,YourMDN:987
#
# [Local]
# StorageNodeAddress=MyIpAddress:6363


class PreserveConfig:
  def __init__(self, filename="~/.preserve/.config"):
      self._parser=configparser.SafeConfigParser()
      self._parser.read(filename)

      _address=self._parser.get('MetadataNodes', 'Address')
      self._MetaDataNodes=_address.split(',')

      if self._parser.has_option('Local', 'StorageNodeAddress'):
         self._StorageNodeAddress=self._parser.get('Local', 'StorageNodeAddress')
      else:
         self._StorageNodeAddress=None

      if self._parser.has_option('Local', 'MetadataNodeAddress'):
         self._MetadataNodeAddress=self._parser.get('Local', 'MetadataNodeAddress')
      else:
         self._MetadataNodeAddress=None

  def get_MetaDataNodes(self):
      return self._MetaDataNodes

  def get_StorageNodeAddress(self):
      return self._StorageNodeAddress

  def get_MetadataNodeAddress(self):
      return self._MetaDataNodeAdress

