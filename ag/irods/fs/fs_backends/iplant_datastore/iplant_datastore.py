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

"""
iPlant Data Store Backend
"""

import os
import time
import logging

from fs import abstract_fs
from fs import metadata
from fs.fs_backends.iplant_datastore import bms_client
from fs.fs_backends.iplant_datastore import irods_client

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

class backend_impl(abstract_fs.fs_base):
    def __init__(self, config):
        if not config:
            raise ValueError("fs configuration is not given correctly")

        dataset_root = config.get("dataset_root")
        if not dataset_root:
            raise ValueError("dataset_root configuration is not given correctly")
        dataset_root = dataset_root.rstrip("/")

        secrets = config.get("secrets")
        if not secrets:
            raise ValueError("secrets are not given correctly")

        user = secrets.get("user")
        if not user:
            raise ValueError("user is not given correctly")

        password = secrets.get("password")
        if not password:
            raise ValueError("password is not given correctly")

        irods_config = config.get("irods")
        if not irods_config:
            raise ValueError("irods configuration is not given correctly")

        bms_config = config.get("bms")
        if not bms_config:
            raise ValueError("bms configuration is not given correctly")

        self.irods_config = irods_config
        self.bms_config = bms_config

        # init irods client
        self.irods = irods_client.irods_client(host=self.irods_config["host"], 
                                               port=self.irods_config["port"], 
                                               user=user, 
                                               password=password, 
                                               zone=self.irods_config["zone"])
        # init bms client
        path_filter = dataset_root.rstrip("/") + "/*"

        acceptor = bms_client.bms_message_acceptor("path", 
                                                   path_filter)
        self.bms = bms_client.bms_client(host=self.bms_config["host"], 
                                         port=self.bms_config["port"], 
                                         user=user, 
                                         password=password, 
                                         vhost=self.bms_config["vhost"],
                                         acceptors=[acceptor])


        self.bms.setCallbacks(on_message_callback=self._on_bms_message_receive)

        # init dataset tracker
        self.dataset_tracker = metadata.dataset_tracker(root_path=dataset_root,
                                                        update_event_handler=self._on_dataset_update, 
                                                        request_for_update_handler=self._on_request_update)

        self.notification_cb = None

    def _on_bms_message_receive(self, message):
        log.info("_on_bms_message_received - %s", message)

        # TODO: parse message and update directory
        entries = self.irods.listStats(entry.path)
        stats = []
        for entry in entries:
            stat = abstract_fs.fs_stat(directory=entry.directory, 
                                       path=entry.path,
                                       name=entry.name, 
                                       size=entry.size,
                                       checksum=entry.checksum,
                                       create_time=entry.create_time,
                                       modify_time=entry.modify_time)
            stats.append(stat)

        self.dataset_tracker.updateDirectory(path=entry.path, entries=stats)

    def _on_dataset_update(self, updated_entries, added_entries, removed_entries):
        if self.notification_cb:
            self.notification_cb(updated_entries, added_entries, removed_entries)

    def _on_request_update(self, entry):
        entries = self.irods.listStats(entry.path)
        stats = []
        for entry in entries:
            stat = abstract_fs.fs_stat(directory=entry.directory, 
                                       path=entry.path,
                                       name=entry.name, 
                                       size=entry.size,
                                       checksum=entry.checksum,
                                       create_time=entry.create_time,
                                       modify_time=entry.modify_time)
            stats.append(stat)
        self.dataset_tracker.updateDirectory(path=entry.path, entries=stats)

    def connect(self):
        self.irods.connect()
        self.bms.connect()

        # add initial dataset
        dataset_root = self.dataset_tracker.getRootPath()
        entries = self.irods.listStats(dataset_root)
        for entry in entries:
            stat = abstract_fs.fs_stat(directory=entry.directory, 
                                       path=entry.path,
                                       name=entry.name, 
                                       size=entry.size,
                                       checksum=entry.checksum,
                                       create_time=entry.create_time,
                                       modify_time=entry.modify_time)
            stats.append(stat)
        self.dataset_tracker.updateDirectory(path=dataset_root, entries=stats)

    def close(self):
        self.bms.close()
        self.irods.close()

    def exists(self, path):
        return self.irods.exists(path)

    def list_dir(self, dirpath):
        return self.irods.list(dirpath)

    def is_dir(self, dirpath):
        return self.irods.isDir(dirpath)

    def read(self, filepath, offset, size):
        return self.irods.read(filepath, offset, size)

    def backend(self):
        return self.__class__

    def notification_supported(self):
        return True

    def set_notification_cb(self, notification_cb):
        self.notification_cb = notification_cb


