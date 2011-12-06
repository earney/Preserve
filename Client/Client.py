#!/usr/bin/env python3

import sys

sys.path.append("libs")
import ParseCommandLine

sys.path.append("../Common/")
import Config

_config=Config.Config()
_mdn=_config.get_MetadataNodes()

_pcl=ParseCommandLine.ParseCommandLine(_mdn)
print(_pcl.process_command(sys.argv))
