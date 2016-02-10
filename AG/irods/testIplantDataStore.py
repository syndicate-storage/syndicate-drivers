#!/usr/bin/env python

"""
   Copyright 2014 The Trustees of Princeton University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import os
import sys
import logging
import threading

from fs.fs_backend_loader import fs_backend_loader

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

DATASTORE_HOST = "irods-2.iplantcollaborative.org"
DATASTORE_PORT = 1247
DATASTORE_ZONE = "iplant"

BMS_HOST = "irods-2.iplantcollaborative.org"
BMS_PORT = 31333
BMS_VHOST = "/irods/useraccess"

def main():
    irods_config = {}
    irods_config["host"] = DATASTORE_HOST
    irods_config["port"] = DATASTORE_PORT
    irods_config["zone"] = DATASTORE_ZONE
    irods_config["user"] = "iychoi"
    irods_config["password"] = ""

    bms_config = {}
    bms_config["host"] = BMS_HOST
    bms_config["port"] = BMS_PORT
    bms_config["vhost"] = BMS_VHOST
    bms_config["user"] = "iychoi"
    bms_config["password"] = ""
    bms_config["dataset_root"] = "/iplant/home/iychoi"
    

    backend_config = {}
    backend_config["irods"] = irods_config
    backend_config["bms"] = bms_config

    def update_callback(updated_entries, added_entries, removed_entries):
        log.info("update-callback")
        for u in updated_entries:
            print "U", u
        for a in added_entries:
            print "A", a
        for r in removed_entries:
            print "R", r

    loader = fs_backend_loader("iplant_datastore", backend_config)
    fs_interface = loader.load()
    print fs_interface

    fs_interface.set_notification_cb(update_callback)
    fs_interface.connect()
    for e in fs_interface.list_dir("/iplant/home/iychoi"):
        print e
    fs_interface.close()
    print "done"

if __name__ == "__main__":
    main()


