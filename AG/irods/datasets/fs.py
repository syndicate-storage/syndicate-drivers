#/usr/bin/python

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
import re
import sys
import time

import syndicate.ag.curation.specfile as AG_specfile
import syndicate.ag.curation.crawl as AG_crawl

DRIVER_NAME = "fs"

# list a directory 
def fs_listdir( fs, dirpath ):
   return fs.listdir( dirpath ) )

# is this a directory?
def fs_isdir( fs, dirpath ):
   return fs.isdir( dirpath )

# build a hierarchy, using sensible default callbacks
def build_hierarchy( fs_conf, root_dir, include_cb, fs_specfile_cbs, max_retries=1, num_threads=1, allow_partial_failure=False ):
   
   fs_crawler_cbs = AG_crawl.crawler_callbacks( include_cb=include_cb,
                                                  listdir_cb=fs_listdir,
                                                  isdir_cb=fs_isdir )
   
   hierarchy = AG_crawl.build_hierarchy( fs_conf, root_dir, DRIVER_NAME, fs_crawler_cbs, fs_specfile_cbs, allow_partial_failure=allow_partial_failure, max_retries=max_retries )
   
   return hierarchy
