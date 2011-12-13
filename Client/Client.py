#!/usr/bin/env python3

import sys

sys.path.append("libs")
import ParseCommandLine

sys.path.append("../Common/")
import Config
import MetadataNodeHandler

_config=Config.Config()
_mdn=_config.get_MetadataNodes()

_mdn_handler=MetadataNodeHandler.MetadataNodeHandler(_mdn)

_pcl=ParseCommandLine.ParseCommandLine(_mdn_handler)
print(_pcl.process_command(sys.argv))
