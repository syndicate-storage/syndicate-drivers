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
import bms_client
import irods_client
import dataset_tracker

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger(__name__)

DATASTORE_HOST = "data.iplantcollaborative.org"
DATASTORE_PORT = 1247
DATASTORE_ZONE = "iplant"
DATASTORE_ROOT = "/iplant/home/iychoi"

def main():
    tracker = None
    client = irods_client.irods_client(host=DATASTORE_HOST, 
                                port=DATASTORE_PORT, 
                                user='iychoi', 
                                password='tprPfh112233', 
                                zone=DATASTORE_ZONE)
    client.connect()

    def onUpdate(updated_entries, added_entries, removed_entries):
        if updated_entries:
            updated_entries = list(updated_entries)
            print "updated:", len(updated_entries)
            for updated in updated_entries:
                print "-", updated    

        if added_entries:
            added_entries = list(added_entries)
            print "added:", len(added_entries)
            for added in added_entries:
                print "-", added

        if removed_entries:
            removed_entries = list(removed_entries)
            print "removed:", len(removed_entries)
            for removed in removed_entries:
                print "-", removed

    def onRequestUpdate(entry):
        print "requested:", entry
        entries = client.listStats(entry.path)
        l_entries = list(entries)
        tracker.updateDirectory(path=entry.path, entries=l_entries)

    tracker = dataset_tracker.dataset_tracker(root_path=DATASTORE_ROOT,
                              update_event_handler=onUpdate, 
                              request_for_update_handler=onRequestUpdate)

    entries = client.listStats(DATASTORE_ROOT)
    l_entries = list(entries)
    tracker.updateDirectory(path=DATASTORE_ROOT, entries=l_entries)

    for f in tracker.walk():
        print f

    client.close()
    print "complete!"

if __name__ == "__main__":
    main()


