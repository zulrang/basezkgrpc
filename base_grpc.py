import importlib
import atexit
import json
import time
import random
import logging

import grpc
from kazoo.client import KazooClient, KazooState
from kazoo.exceptions import NoNodeError, ConnectionLoss

LOGGER = logging.getLogger(__name__)


class BaseZKgRPC():

    ZK_ENDPOINT = '127.0.0.1:2181'
    CA_FILE = False
    CLIENT_CERT = False
    CLIENT_KEY = False

    def __init__(self):
        """Constructor"""

        # immediately connect to zookeeper
        self.zk = KazooClient(hosts=self.ZK_ENDPOINT, read_only=True)
        self.zk.start()
        self.zk_connected = True

        # add state change listener to monitor zk connection events
        self.zk.add_listener(self.kazoo_listener)

        # register deconstructor to run on exit
        atexit.register(self.__del__)

        self.channel = False
        self.stub = False

        # determine stub class and import
        stub_package, stub_class = self.STUB_CLASS.rsplit('.', 1)
        self.stub_class = getattr(importlib.import_module(stub_package), stub_class)

        # do the same for all methods and generate instance methods
        for method_name, request_class_package in self.METHODS:
            request_package, request_class_name = request_class_package.rsplit('.', 1)
            request_class = getattr(importlib.import_module(request_package), request_class_name)
            # create method to call lambda which passes info to generic method
            setattr(self, method_name, lambda method_name=method_name, request_class=request_class, **v: self.call_method(method_name, request_class, **v))

    def kazoo_listener(self, state):
        """Zookeeper state change handler monitors connection and flags availability"""

        if state == KazooState.LOST:
            LOGGER.info("Kazoo session lost")
            self.zk_connected = False
        elif state == KazooState.SUSPENDED:
            LOGGER.info("Kazoo disconnected")
            self.zk_connected = False
        else:
            LOGGER.info("Kazoo connected")
            self.zk_connected = True

    def connect(self):
        """Connect to gRPC endpoint based on TLS config"""

        endpoint = self.get_endpoint()
        LOGGER.info('Using endpoint: {}'.format(endpoint))

        # make TLS connection if given a root certificate
        if self.CA_FILE:
            self.channel = grpc.secure_channel(self.get_endpoint(), self.get_credentials())
        else:
            self.channel = grpc.insecure_channel(self.get_endpoint())
        self.stub = self.stub_class(self.channel)

    def get_endpoint(self):
        """Queries Zookeeper for a random available gRPC endpoint"""

        hosts = []
        # loop until at least one host is returned
        while len(hosts) < 1:
            try:
                # wait until zookeeper is flagged as available
                while not self.zk_connected:
                    LOGGER.info("Waiting for Zookeeper connection")
                    time.sleep(1)

                # iterate keys within the root
                brokers = [json.loads(self.zk.get('{}/{}'.format(self.ZK_KEY, node))[0])
                           for node in self.zk.get_children(self.ZK_KEY)]

                # build endpoints from returned json
                hosts = ['%s:%d' % (b['host'], b['port']) for b in brokers]

                # wait until at least one host is returned
                if len(hosts) == 0:
                    LOGGER.info("Waiting for hosts to be available in {}".format(self.ZK_KEY))
                    time.sleep(1)
            # handle connection issues and try again
            except NoNodeError:
                time.sleep(1)
            except ConnectionLoss:
                time.sleep(1)

        # choose a random host
        return random.choice(hosts)

    def get_credentials(self):
        """Creates TLS credentials"""

        with open(self.CA_FILE, 'rb') as f:
            ca_trust = f.read()
        return grpc.ssl_channel_credentials(root_certificates=ca_trust)

    def call_method(self, method_name, request_class, *args, **kwargs):
        """Generic method to build then send gRPC request"""

        # ensure we're connected
        if not self.channel or not self.stub:
            self.connect()

        # create a valid request message
        request = request_class(**kwargs)

        # reconnect automatically
        while True:
            try:
                # make the call
                response = getattr(self.stub, method_name)(request)
                return response
            except grpc._channel._Rendezvous as ex:
                # handle events that can be reconnected
                if ex.code() == grpc.StatusCode.UNAVAILABLE or ex.code() == grpc.StatusCode.INTERNAL:
                    # reconnect
                    LOGGER.info('Reconnecting...')
                    self.connect()
                else:
                    raise ex

    def __del__(self):
        """Ends zookeeper session and closes connection"""

        self.zk.stop()
        self.zk.close()
