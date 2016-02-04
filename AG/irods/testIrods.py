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
import hashlib
import logging
import time
import irods_client

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger(__name__)

DATASTORE_HOST = "data.iplantcollaborative.org"
DATASTORE_PORT = 1247
DATASTORE_ZONE = "iplant"


def hashfile(filepath):
    md5 = hashlib.md5()
    f = open(filepath, 'rb')
    try:
        md5.update(f.read())
    finally:
        f.close()
    return md5.hexdigest()

def filesize(filepath):
    return os.path.getsize(filepath)

def main():
    client = irods_client.irods_client(host=DATASTORE_HOST, 
                                port=DATASTORE_PORT, 
                                user='iychoi', 
                                password='tprPfh112233', 
                                zone=DATASTORE_ZONE)
    
    with client as c:
        entries = c.listStats("/iplant/home/iychoi/terasort_data")
        for entry in entries:
            print "download", entry.path
            print "checksum", entry.checksum, "size", entry.size

            newFile = None
            try:
                newFile = c.download(entry.path, entry.name)
                newChecksum = hashfile(newFile)
                size = filesize(newFile)

                print "checksum", newChecksum, "size", size
                assert(entry.checksum == newChecksum)
            except Exception as e:
                print "error raised", e
                break;
            finally:
                if newFile:
                    os.remove(newFile)

    print "complete!"

if __name__ == "__main__":
    main()


