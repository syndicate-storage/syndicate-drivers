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
import time

from retrying import retry
from timeout import timeout

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

BMS_HOST = "irods-2.iplantcollaborative.org"
BMS_PORT = 31333
BMS_VHOST = "/irods/useraccess"

def main():
    acceptor = bms_client.bms_message_acceptor("path", "*")
    client = bms_client.bms_client(host=BMS_HOST, 
                                port=BMS_PORT, 
                                user='iychoi', 
                                password='tprPfh112233', 
                                vhost=BMS_VHOST,
                                acceptors=[acceptor])

    def on_connect_callback():
        log.info("on_connect")

    def on_register_callback(message):
        log.info("on_register - %s", message)

    def on_message_callback(message):
        log.info("on_message - %s", message)

    client.set_callbacks(on_connect_callback=on_connect_callback,
                         on_register_callback=on_register_callback,
                         on_message_callback=on_message_callback)

    with client as c:
        time.sleep(15)

    print "complete!"

if __name__ == "__main__":
    main()


