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
import logging
import pika
import json
import string
import random
import threading

logging.basicConfig( format='[%(asctime)s] [%(levelname)s] [%(module)s:%(lineno)d] %(message)s' )

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

BMS_REGISTRATION_EXCHANGE = 'bms_registrations'
BMS_REGISTRATION_QUEUE = 'bms_registrations'

"""
Interface class to iPlant Border Message Server
"""
class bms_message_acceptor(object):
    def __init__(self, acceptor="path", 
                       pattern="*"):
        self.acceptor = acceptor
        self.pattern = pattern

    def as_dict(self):
        return self.__dict__

    def __repr__(self): 
        return "<bms_message_acceptor %s %s>" % (self.acceptor, self.pattern) 

class bms_client(object):
    def __init__(self, host=None,
                       port=31333,
                       vhost="/",
                       user=None,
                       password=None,
                       appid=None):
        self.host = host
        self.port = port
        self.vhost = vhost
        self.user = user
        self.password = password
        if appid:
            self.appid = appid
        else:
            self.appid = self._generate_appid()

        self.connection = None
        self.channel = None
        self.queue = None
        self.closing = False
        self.consumer_tag = None
        self.consumer_thread = None

        self.on_connect_callback = None
        self.on_register_callback = None
        self.on_message_callback = None

    def set_callbacks(self, on_connect_callback=None, on_register_callback=None, on_message_callback=None):
        if on_connect_callback:
            self.on_connect_callback = on_connect_callback
        if on_register_callback:
            self.on_register_callback = on_register_callback
        if on_message_callback:
            self.on_message_callback = on_message_callback

    def clear_callbacks(self):
        self.on_connect_callback = None
        self.on_register_callback = None
        self.on_message_callback = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _id_generator(self, size=8, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def _generate_appid(self):
        return self._id_generator()

    def _consumer_thread_task(self):
        log.info('start consumer thread task')
        self.connection.ioloop.start()
        log.info('stop consumer thread task')

    def connect(self):
        log.info('connect to %s', self.host)
        credentials = pika.PlainCredentials(self.user, 
                                            self.password)
        parameters = pika.ConnectionParameters(self.host, 
                                               self.port, 
                                               self.vhost, 
                                               credentials)
        self.connection = pika.SelectConnection(parameters,
                                                self.on_connection_open,
                                                stop_ioloop_on_close=False)
        log.info('start consuming messages')
        self.consumer_thread = threading.Thread(target=self._consumer_thread_task)
        self.consumer_thread.start()

    def on_connection_open(self, connection):
        log.info('connected')
        self.connection.add_on_close_callback(self.on_connection_closed)
        # open a channel
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_connection_closed(self, connection, reply_code, reply_text):
        log.info('connection closed')
        self.channel = None
        if self.closing:
            self.connection.ioloop.stop()
        else:
            log.info('reconnect')
            self.connection.add_timeout(5, self.reconnect)

    def on_channel_open(self, channel):
        log.info('open a channel')
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)

        log.info('declare a queue %s/%s', self.user, self.appid)
        # declare a queue
        self.queue = self.user + "/" + self.appid
        self.channel.queue_declare(self.on_queue_declareok, 
                                   queue=self.queue,
                                   durable=False, 
                                   exclusive=False, 
                                   auto_delete=True)

    def on_queue_declareok(self, mothod_frame):
        log.info('declared a queue %s/%s', self.user, self.appid)
        # set consumer
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self.consumer_tag = self.channel.basic_consume(self.on_message, 
                                                       queue=self.queue,
                                                       no_ack=False)
        # call callback
        if self.on_connect_callback:
            self.on_connect_callback()

    def on_channel_closed(self, channel, reply_code, reply_text):
        log.info('channel closed')
        self.connection.close()

    def on_consumer_cancelled(self, method_frame):
        log.info('consumer cancelled')
        if self.channel:
            self.channel.close()

    def on_message(self, channel, method, properties, body):
        log.info('Received message # %s from %s: %s',
                    method.delivery_tag, properties.app_id, body)

        # acknowledge
        self.channel.basic_ack(method.delivery_tag)

        # call callback
        if self.on_message_callback:
            self.on_message_callback(body)

    def reconnect(self):
        self.connection.ioloop.stop()

        if not self.closing:
            self.connection = self.connect()

    def close(self):
        log.info('stopping')
        self.closing = True

        if self.channel:
            self.channel.basic_cancel(self.on_cancelok, self.consumer_tag)

        self.connection.ioloop.start()
        self.connection.close()

        self.consumer_thread = None

    def on_cancelok(self, unused_frame):
        self.channel.close()

    def register(self, *acceptors):
        log.info('register')
        # make a registration message
        """
        reg_msg = {"request": "lease", 
                    "client": {"user_id": self.user,
                                "application_name": self.appid},
                    "acceptors": [{"acceptor": "path",
                                    "pattern": "/iplant/home/iychoi/*"}] }
        """
        acceptors_arr = []
        for acceptor in acceptors:
            acceptors_arr.append(acceptor.as_dict())

        reg_msg = {"request": "lease", 
                    "client": {"user_id": self.user,
                                "application_name": self.appid},
                    "acceptors": acceptors_arr}
        reg_msg_str = json.dumps(reg_msg)

        # set a message property
        prop = pika.BasicProperties(reply_to=self.queue)

        # request a registration
        self.channel.basic_publish(exchange=BMS_REGISTRATION_EXCHANGE,
                                   routing_key=BMS_REGISTRATION_QUEUE,
                                   properties=prop,
                                   body=reg_msg_str)


