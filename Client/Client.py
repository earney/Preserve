#!/usr/bin/env python3

import sys

sys.path.append("libs")
import ParseCommandLine

sys.path.append("../Common/")
import Config

_config=Config.Config()
_mdn=_config.get_MetaDataNodes()

#_url="127.0.0.1:9696"
_pcl=ParseCommandLine.ParseCommandLine(_mdn)
print(_pcl.process_command(sys.argv))
sys.exit()

import AssembleFile

_id="15e376e0695c9cb97aed2499e8452f288d2f24d4"
_as=AssembleFile.AssembleFile("127.0.0.1:9696")
_as.process(_id, "mytest.wmv")
