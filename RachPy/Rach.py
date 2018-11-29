import json
import websocket
import time
import threading


class Rach:
    class Publisher:
        def __init__(self, topic, rach):
            self.rach = rach
            self.topic = topic

        def pub(self, data):
            self.rach: Rach
            self.rach.pub(self.topic, data)

        def close(self):
            self.rach: Rach
            self.rach.rm_pub(self.topic)

    def __init__(self, path, cred):
        self.sock = None
        self.cred = cred
        self.server_path = path
        self.debug = False
        self.killed = False
        self.sock_manager_thread = None
        self.ns = '/'
        self.matcher = 0
        self.callbacks = {}
        self.request_map = {}
        self.pub_topics = {}
        self.sub_topics = {}

    @staticmethod
    def prep_path(path, cred):
        return path + '?type=terminal&username=%s&password=%s' % (cred.get('username', ''), cred.get('password', ''))

    def start(self):
        self.killed = False
        path = Rach.prep_path(self.server_path, self.cred)
        # Todo: Why non-lambda not work how-does-python-distinguish-callback-function-which-is-a-member-of-a-class
        self.sock = websocket.WebSocketApp(path,
                                           on_message=lambda ws, msg: self.ws_on_message(ws, msg),
                                           on_error=lambda ws, error: self.ws_on_error(ws, error),
                                           on_close=lambda ws: self.ws_on_close(ws))
        self.sock.on_open = lambda ws: self.ws_on_open(ws)
        self.sock_manager_thread = threading.Thread(target=self.sock_manager)
        self.sock_manager_thread.start()
        # Todo: block till connect

    def stop(self):
        self.killed = True
        self.rm_all_sub()
        self.rm_all_pub()
        if self.sock is not None:
            self.sock.close()
        if self.sock_manager_thread is not None:
            self.sock_manager_thread.join()

    def send(self, msg):
        try:
            str_msg = json.dumps(msg)
            self.log('ws send msg: ' + str_msg)
            self.sock.send(str_msg)
        except json.decoder.JSONDecodeError:
            self.log('ws msg encode error: ' + str(msg))

    def ws_on_open(self, _):
        self.log('ws opened')

    def ws_on_message(self, _, str_msg):
        try:
            msg = json.loads(str_msg)
            self.log('ws msg: ' + str(msg))
            self.process_msg(msg)
        except json.decoder.JSONDecodeError:
            self.log('ws msg decode error: ' + str_msg)

    def ws_on_error(self, _, error):
        self.log('ws error: ' + str(error))

    def ws_on_close(self, _):
        self.log('ws closed')

    def log(self, msg):
        if self.debug:
            print(msg)

    def enable_debug(self):
        self.debug = True
        websocket.enableTrace(False)

    def disable_debug(self):
        self.debug = False
        websocket.enableTrace(False)

    def process_msg(self, msg):
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
                cb(*args)
        elif typ == 'ack':
            if matcher is not None and matcher in self.request_map:
                cb, args, _, _ = self.request_map.pop(matcher)
                cb(*args)
        elif typ == 'pub':
            self.process_pub(msg.get('data', None))

    def sock_manager(self):
        while not self.killed:
            self.sock.run_forever()
            if not self.killed:
                time.sleep(1)

    def make_req_matcher(self):
        self.matcher += 1
        return str(self.matcher)

    def set_namespace(self, ns):
        if ns[-1] != '/':
            self.ns = ns + '/'
        else:
            self.ns = ns

    def get_fully_qualified_topic(self, topic):
        if topic[0] == '/':
            return topic
        return self.ns + topic

    def add_callback(self, topic, callback, args):
        self.callbacks[topic] = (callback, args)
        self.sub_topics[topic] = True

    def rm_callback(self, topic):
        self.callbacks.pop(topic)
        self.sub_topics.pop(topic)

    def add_pub_callback(self, topic):
        self.pub_topics[topic] = True

    def rm_pub_callback(self, topic):
        self.pub_topics.pop(topic)

    def empty_callback(self):
        pass

    def add_sub(self, topic, callback, args):
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
        topic = self.get_fully_qualified_topic(topic)
        if topic not in self.pub_topics:
            self.log('Not publishing to %s' % topic)
            return
        matcher = self.make_req_matcher()
        msg = {'type': 'pub', 'matcher': matcher, 'data': {'topic': topic, 'data': data}}
        self.send(msg)

    def process_pub(self, data):
        if data is None:
            return
        cb, args = self.callbacks.get(data.get('topic'), (None, None))
        args: tuple
        if not callable(cb):
            return
        if args is None:
            cb(data)
        else:
            cb(data, *args)

    def rm_all_sub(self):
        for topic in self.sub_topics:
            self.rm_sub(topic)

    def rm_all_pub(self):
        for topic in self.pub_topics:
            self.rm_pub(topic)
