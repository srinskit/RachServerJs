import json
import websocket
import time
import threading
from typing import Callable


class Rach:
    """
    A class used to represent a publisher to a topic

    Attributes:
        sock (websocket.WebSocketApp): The websocket client
        sock_manager_thread (thread
        cred (dict): The credentials dictionary
        server_path (str): The path to Rach Server
        debug (bool): The flag denoting whether to print debug messages
        killed (bool): The flag denoting state of Rach Client
        ns (str): The namespace of Rach Client
        matcher (int): The counter helping the make_req_matcher method
        callbacks (dict): The map mapping topic to it's (callback, arg) tuple
        request_map (dict): The map mapping request matcher to the success and failure callbacks
        pub_topics (dict): pub_topics[ <topic> ] is True if Rach Client is publicising at <topic>
        sub_topics (dic): sub_topics[ <topic> ] is True if Rach Client is subscribed to <topic>
    """

    class Publisher:
        """
        A class used to represent a publisher to a topic

        Attributes:
            rach (Rach): The Rach client
            topic (str): The topic the publisher publishes at
        """

        def __init__(self, topic, rach):
            """
            Parameters:
                topic (str): The topic the publisher publishes at
                rach (Rach): The Rach client to publish through
            """
            self.rach = rach
            self.topic = topic

        def pub(self, data):
            """Publish data to Rach Server

            Parameters:
                data (dict): The data to publish
            """
            self.rach.pub(self.topic, data)

        def close(self):
            """Destroy the publisher
            """
            self.rach.rm_pub(self.topic)

    def __init__(self, path, cred):
        """
        Parameters:
            path (str): The URL of Rach Server
            cred (dict): The credentials used for authentication
        """
        self.sock: websocket.WebSocketApp = None
        self.sock_manager_thread: threading.Thread = None
        self.cred = cred
        self.server_path = path
        self.debug = False
        self.killed = False
        self.connected = False
        self.ns = '/'
        self.matcher = 0
        self.callbacks = {}
        self.request_map = {}
        self.pub_topics = {}
        self.sub_topics = {}
        # Todo: Why non-lambda not work how-does-python-distinguish-callback-function-which-is-a-member-of-a-class
        path = Rach.prep_path(self.server_path, self.cred)
        # Note: the closure context of lambda allows us to use <self> inside lambda
        self.sock = websocket.WebSocketApp(path,
                                           on_open=lambda ws: self.ws_on_open(ws),
                                           on_message=lambda ws, msg: self.ws_on_message(ws, msg),
                                           on_error=lambda ws, error: self.ws_on_error(ws, error),
                                           on_close=lambda ws: self.ws_on_close(ws))

    @staticmethod
    def prep_path(path, cred):
        """Add parameters to the Rach Server path

        Parameters:
            path (str): The URL of Rach Server
            cred (dict): The credentials used for authentication

        Returns:
            str: The parameterized path to Rach Server
        """
        return path + '?type=terminal&username=%s&password=%s' % (cred.get('username', ''), cred.get('password', ''))

    def start(self, on_start=None):
        """Connect to the Rach Server

        Parameters:
              on_start (function): The callback to call when connection established
        """
        self.killed = False
        self.connected = False
        self.sock_manager_thread = threading.Thread(target=self.sock_manager)
        self.sock_manager_thread.start()
        # Todo: block till connect
        time.sleep(1)
        # Todo: implement on_start call. currently dummy for consistency with RachJs
        if on_start is not None:
            on_start()

    def stop(self):
        """Disconnect from the Rach Server
        """
        self.killed = True
        self.rm_all_sub()
        self.rm_all_pub()
        if self.sock is not None:
            self.sock.close()
        if self.sock_manager_thread is not None:
            self.sock_manager_thread.join()

    def send(self, msg, callback=None):
        """Send msg to Rach Server

        Parameters:
              msg (dict): The message to send
              callback (function, optional): The callback to pass null to on success and not null on error
        """
        try:
            str_msg = json.dumps(msg)
            self.log('ws send msg: ' + str_msg)
            if self.connected:
                self.sock.send(str_msg)
            else:
                callback(True) if callable(callback) else None
                return
        except json.decoder.JSONDecodeError:
            self.log('ws msg encode error: ' + str(msg))
            callback(True) if callable(callback) else None
            return
        callback(None) if callable(callback) else None

    def ws_on_open(self, _):
        """Callback to handle websocket connection open

        Parameters:
              _ (websocket.WebSocketApp): The websocket client
        """
        self.log('ws opened')
        self.connected = True

    def ws_on_message(self, _, str_msg):
        """Callback to handle message from websocket

        Parameters:
              _ (websocket.WebSocketApp): The websocket client
              str_msg (str): The message
        """
        try:
            msg = json.loads(str_msg)
            self.log('ws msg: ' + str(msg))
            self.process_msg(msg)
        except json.decoder.JSONDecodeError:
            self.log('ws msg decode error: ' + str_msg)

    def ws_on_error(self, _, err):
        """Callback to handle websocket error

        Parameters:
              _ (websocket.WebSocketApp): The websocket client
              err (object): The error
        """
        self.log('ws error: %s' % str(err))

    def ws_on_close(self, _):
        """Callback to handle websocket close

        Parameters:
              _ (websocket.WebSocketApp): The websocket client
        """
        self.log('ws closed')
        self.connected = False

    def log(self, msg):
        """Print debug messages

        Parameters:
              msg (str): The debug message
        """
        if self.debug:
            print(msg)

    def enable_debug(self):
        """Enable printing of debug messages

        """
        self.debug = True
        websocket.enableTrace(False)

    def disable_debug(self):
        """Disable printing of debug messages

        """
        self.debug = False
        websocket.enableTrace(False)

    def process_msg(self, msg):
        """Process received message

        Parameters:
              msg (dict): Process received message
        """
        typ = msg.get('type')
        matcher = msg.get('matcher', None)

        if not typ:
            self.log('process_msg: msg lacks type')
            return
        if typ == 'auth':
            if not msg.get('data', {}).get('success', False):
                self.stop()
        elif typ == 'err':
            if matcher is not None and matcher in self.request_map:
                _, _, cb, args = self.request_map.pop(matcher)
                err_msg = msg.get('verbose')
                if args is not None:
                    cb(*([err_msg] + args))
                else:
                    cb(err_msg)
        elif typ == 'ack':
            if matcher is not None and matcher in self.request_map:
                cb, args, _, _ = self.request_map.pop(matcher)
                cb(*args)
        elif typ == 'pub':
            self.process_pub(msg.get('data', None))
        elif typ == 'service':
            if matcher is not None and matcher in self.request_map:
                data = msg.get('data')
                cb, args, _, _ = self.request_map.pop(matcher)
                if data is not None:
                    cb(*([data] + args))
                else:
                    cb(*args)
        elif typ == "cs_ping":
            if matcher is not None and matcher in self.request_map:
                on_success, _, _, _ = self.request_map.pop(matcher)
                if callable(on_success):
                    on_success()

    def sock_manager(self):
        """Maintain socket connection

        """

        while not self.killed:
            # Attempt connect
            self.sock.run_forever()
            # Reaches here when on disconnect
            if not self.killed:
                time.sleep(2)

    def make_req_matcher(self):
        """Generate unique string every call

        Returns:
            str: The unique string
        """

        self.matcher += 1
        return str(self.matcher)

    def set_namespace(self, ns):
        """Set namespace of Rach Client

        Parameters:
            ns (str): The namespace
        """

        if len(ns) > 1 and ns[-1] == '/':
            ns = ns[:-1]
        if ns[0] != '/':
            ns = '/' + ns
        self.ns = ns

    def get_fully_qualified_topic(self, topic):
        """Get fully qualified topic name

        Parameters:
            topic (str): The topic
        """
        if len(topic) > 1 and topic[-1] == '/':
            topic = topic[:-1]
        if topic[0] == '/':
            return topic
        return self.ns + ('' if self.ns == '' else '/') + topic

    def add_callback(self, topic, callback, args):
        """Callback for addition of subscription callback and it's args

        Parameters:
            topic (str): The subscription topic
            callback (function): The subscription callback
            args (list): The additional arguments to be passed to the subscription callback
        """

        self.callbacks[topic] = (callback, args)
        self.sub_topics[topic] = True

    def rm_callback(self, topic):
        """Callback for removal of subscription callback and it's args

        Parameters:
            topic (str): The subscription topic
        """
        self.callbacks.pop(topic)
        self.sub_topics.pop(topic)

    def add_pub_callback(self, topic):
        """Callback for approval of publicise

            Parameters:
            topic (str): The publish topic
        """
        self.pub_topics[topic] = True

    def rm_pub_callback(self, topic):
        """Callback for removal of publicise

            Parameters:
            topic (str): The publish topic
        """
        self.pub_topics.pop(topic)

    @staticmethod
    def empty_callback(*args):
        """Callback that does nothing

            Parameters:
                args (tuple): Dummy arguments
        """
        pass

    def add_sub(self, topic, callback, args):
        """Request to subscribe to a topic

            Parameters:
                topic (str): The topic
                callback (function): The callback to call when event occurs at <topic>
                args (list): The arguments to pass to the callback in addition to data
        """

        # Todo: Support for multiple sub requests
        topic = self.get_fully_qualified_topic(topic)
        if topic in self.sub_topics:
            self.log('Already subscribed to %s' % topic)
            return
        matcher = self.make_req_matcher()
        msg = {'type': 'addSub', 'matcher': matcher, 'data': {'topic': topic}}
        self.request_map[matcher] = (
            Rach.add_callback, (self, topic, callback, args), Rach.empty_callback, (self,)
        )
        self.send(msg)

    def rm_sub(self, topic):
        """Request to remove a subscription to a topic

            Parameters:
                topic (str): The topic
        """

        topic = self.get_fully_qualified_topic(topic)
        if topic not in self.sub_topics:
            self.log('Not subscribed to %s' % topic)
            return
        matcher = self.make_req_matcher()
        msg = {'type': 'rmSub', 'matcher': matcher, 'data': {'topic': topic}}
        self.request_map[matcher] = (
            Rach.rm_callback, (self, topic), Rach.empty_callback, (self,)
        )
        self.send(msg)

    def add_pub(self, topic):
        """Request to publish to a topic

            Parameters:
                topic (str): The topic
        """

        topic = self.get_fully_qualified_topic(topic)
        if topic in self.pub_topics:
            self.log('Already registered to publish to %s' % topic)
            return
        matcher = self.make_req_matcher()
        msg = {'type': 'addPub', 'matcher': matcher, 'data': {'topic': topic}}
        self.request_map[matcher] = (
            Rach.add_pub_callback, (self, topic), Rach.empty_callback, (self,)
        )
        self.send(msg)
        return Rach.Publisher(topic, self)

    def rm_pub(self, topic):
        """Request to stop publishing to a topic

            Parameters:
                topic (str): The topic
        """

        topic = self.get_fully_qualified_topic(topic)
        if topic not in self.pub_topics:
            self.log('Not publishing to %s' % topic)
            return
        matcher = self.make_req_matcher()
        msg = {'type': 'rmPub', 'matcher': matcher, 'data': {'topic': topic}}
        self.request_map[matcher] = (
            Rach.rm_pub_callback, (self, topic), Rach.empty_callback, (self,)
        )
        self.send(msg)

    def pub(self, topic, data):
        """Request to publish to a topic

            Parameters:
                topic (str): The topic
                data (dict): The data to publish
        """

        topic = self.get_fully_qualified_topic(topic)
        if topic not in self.pub_topics:
            self.log('Not publishing to %s' % topic)
            return
        matcher = self.make_req_matcher()
        msg = {'type': 'pub', 'matcher': matcher, 'data': {'topic': topic, 'data': data}}
        self.send(msg)

    def process_pub(self, data):
        """Process data received from subscription

            Parameters:
                data (dict): The data received
        """

        if data is None:
            return
        try:
            tmp = self.callbacks.get(data.get('topic'), (None, None))
            cb: Callable = tmp[0]
            args: list = tmp[1]
            cb(data, *args)
        except TypeError:
            self.log('Could not call callback')

    def rm_all_sub(self):
        """Request removal of all subscription

        """

        for topic in self.sub_topics:
            self.rm_sub(topic)

    def rm_all_pub(self):
        """Request withdrawal of all publicise requests

        """
        for topic in self.pub_topics:
            self.rm_pub(topic)

    def service_call(self, topic, args, on_result, on_result_args, on_err, on_err_args):
        """Call a service

            Parameters:
                topic (str): The service topic
                args (list): The service arguments
                on_result (function): The callback to call when service call returns
                on_result_args (list): The parameters to pass to on_result
                on_err (function): The callback to call when service call throws exception
                on_err_args (list): The parameters to pass to on_err
        """
        matcher = self.make_req_matcher()
        msg = {
            'type': 'service',
            'matcher': matcher,
            'data': {
                'topic': topic,
                'args': args,
            }
        }
        self.request_map[matcher] = [
            on_result, on_result_args, on_err, on_err_args
        ]
        self.send(msg)

    def ping(self, on_success=None, on_fail=None):
        """Ping Rach Server

            Parameters:
                on_success (function, optional): The callback to call on success
                on_fail (function, optional): The callback to call on failure
        """
        matcher = self.make_req_matcher()
        msg = {
            'type': 'cs_ping',
            'matcher': matcher,
            'data': None,
        }
        self.request_map[matcher] = [
            on_success, None, on_fail, None
        ]
        self.send(msg, lambda e: on_fail(e) if e is not None else None)
